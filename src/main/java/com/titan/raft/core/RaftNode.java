package com.titan.raft.core;

import com.titan.raft.allocator.DirectBufferAllocator;
import com.titan.raft.network.Transport;
import com.titan.raft.wal.WriteAheadLog;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.CRC32;

public final class RaftNode {

    private static final Logger LOGGER = Logger.getLogger(RaftNode.class.getName());

    public enum Role {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }

    // Cache line padding simulation and VarHandles for lock-free state transitions
    private volatile long p0, p1, p2, p3, p4, p5, p6, p7;
    private volatile long currentTerm;
    private volatile int votedFor;
    private volatile int roleOrdinal; // 0: FOLLOWER, 1: CANDIDATE, 2: LEADER
    private volatile long commitIndex;
    private volatile long lastApplied;
    private volatile long lastHeartbeatTimeNanos;
    private volatile long electionTimeoutNanos;
    private volatile long leaderId;
    private volatile int running; // 1 if running, 0 if stopped

    private static final VarHandle CURRENT_TERM_VH;
    private static final VarHandle VOTED_FOR_VH;
    private static final VarHandle ROLE_ORDINAL_VH;
    private static final VarHandle COMMIT_INDEX_VH;
    private static final VarHandle LAST_APPLIED_VH;
    private static final VarHandle RUNNING_VH;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            CURRENT_TERM_VH = l.findVarHandle(RaftNode.class, "currentTerm", long.class);
            VOTED_FOR_VH = l.findVarHandle(RaftNode.class, "votedFor", int.class);
            ROLE_ORDINAL_VH = l.findVarHandle(RaftNode.class, "roleOrdinal", int.class);
            COMMIT_INDEX_VH = l.findVarHandle(RaftNode.class, "commitIndex", long.class);
            LAST_APPLIED_VH = l.findVarHandle(RaftNode.class, "lastApplied", long.class);
            RUNNING_VH = l.findVarHandle(RaftNode.class, "running", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final int nodeId;
    private final PeerNode[] peers;
    private final WriteAheadLog wal;
    private final Transport transport;
    private final DirectBufferAllocator bufferAllocator;
    private final Thread engineThread;

    private static final long MIN_ELECTION_TIMEOUT_NANOS = 150_000_000L; // 150ms
    private static final long MAX_ELECTION_TIMEOUT_NANOS = 300_000_000L; // 300ms
    private static final long HEARTBEAT_INTERVAL_NANOS = 50_000_000L;   // 50ms

    public RaftNode(int nodeId,
                    PeerNode[] peers,
                    WriteAheadLog wal,
                    Transport transport,
                    DirectBufferAllocator bufferAllocator) {
        if (peers == null) {
            throw new IllegalArgumentException("Peers array cannot be null");
        }
        if (wal == null) {
            throw new IllegalArgumentException("WAL cannot be null");
        }
        if (transport == null) {
            throw new IllegalArgumentException("Transport cannot be null");
        }
        if (bufferAllocator == null) {
            throw new IllegalArgumentException("BufferAllocator cannot be null");
        }

        this.nodeId = nodeId;
        this.peers = peers;
        this.wal = wal;
        this.transport = transport;
        this.bufferAllocator = bufferAllocator;

        this.currentTerm = wal.readLastTerm();
        this.votedFor = wal.readLastVotedFor();
        this.roleOrdinal = Role.FOLLOWER.ordinal();
        this.commitIndex = wal.getCommitIndex();
        this.lastApplied = wal.getLastApplied();
        this.leaderId = -1;
        this.running = 0;

        resetElectionTimeout();
        this.lastHeartbeatTimeNanos = System.nanoTime();

        this.engineThread = new Thread(this::runEngineLoop, "titan-raft-engine-" + nodeId);
        this.engineThread.setDaemon(true);
    }

    public synchronized void start() {
        if (RUNNING_VH.compareAndSet(this, 0, 1)) {
            LOGGER.info(() -> "Starting RaftNode " + nodeId + " at term " + getCurrentTerm());
            engineThread.start();
        }
    }

    public synchronized void stop() {
        if (RUNNING_VH.compareAndSet(this, 1, 0)) {
            LOGGER.info(() -> "Stopping RaftNode " + nodeId);
            LockSupport.unpark(engineThread);
            try {
                engineThread.join(2000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.warning("Interrupted while waiting for engine thread to join");
            }
            try {
                wal.close();
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "Failed to cleanly close WAL", e);
            }
        }
    }

    private void runEngineLoop() {
        while (isRunning()) {
            long now = System.nanoTime();
            Role currentRole = getRole();

            try {
                switch (currentRole) {
                    case FOLLOWER:
                    case CANDIDATE:
                        if (now - lastHeartbeatTimeNanos > electionTimeoutNanos) {
                            startElection();
                        } else {
                            long parkTime = electionTimeoutNanos - (now - lastHeartbeatTimeNanos);
                            if (parkTime > 0) {
                                LockSupport.parkNanos(parkTime);
                            }
                        }
                        break;

                    case LEADER:
                        if (now - lastHeartbeatTimeNanos > HEARTBEAT_INTERVAL_NANOS) {
                            sendAppendEntriesToAll(true);
                            lastHeartbeatTimeNanos = now;
                        } else {
                            long parkTime = HEARTBEAT_INTERVAL_NANOS - (now - lastHeartbeatTimeNanos);
                            if (parkTime > 0) {
                                LockSupport.parkNanos(parkTime);
                            }
                        }
                        break;

                    default:
                        throw new IllegalStateException("Unknown raft role: " + currentRole);
                }

                applyEntriesToStateMachine();

            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error in Raft engine loop for node " + nodeId, e);
            }
        }
    }

    private boolean isRunning() {
        return (int) RUNNING_VH.getAcquire(this) == 1;
    }

    public Role getRole() {
        return Role.values()[(int) ROLE_ORDINAL_VH.getAcquire(this)];
    }

    private void setRole(Role newRole) {
        ROLE_ORDINAL_VH.setRelease(this, newRole.ordinal());
    }

    public long getCurrentTerm() {
        return (long) CURRENT_TERM_VH.getAcquire(this);
    }

    private void setCurrentTerm(long term) {
        CURRENT_TERM_VH.setRelease(this, term);
        wal.writeTermAndVote(term, getVotedFor());
    }

    public int getVotedFor() {
        return (int) VOTED_FOR_VH.getAcquire(this);
    }

    private void setVotedFor(int candidateId) {
        VOTED_FOR_VH.setRelease(this, candidateId);
        wal.writeTermAndVote(getCurrentTerm(), candidateId);
    }

    private void resetElectionTimeout() {
        long randomOffset = ThreadLocalRandom.current().nextLong(
                MIN_ELECTION_TIMEOUT_NANOS,
                MAX_ELECTION_TIMEOUT_NANOS
        );
        electionTimeoutNanos = randomOffset;
        lastHeartbeatTimeNanos = System.nanoTime();
    }

    private void startElection() {
        setRole(Role.CANDIDATE);
        long newTerm = getCurrentTerm() + 1;
        setCurrentTerm(newTerm);
        setVotedFor(nodeId);
        resetElectionTimeout();

        LOGGER.info(() -> "Node " + nodeId + " starting election for term " + newTerm);

        int votesReceived = 1;
        int quorum = (peers.length + 1) / 2 + 1;

        if (votesReceived >= quorum) {
            transitionToLeader();
            return;
        }

        long lastLogIndex = wal.getLastLogIndex();
        long lastLogTerm = wal.getLastLogTerm();

        for (PeerNode peer : peers) {
            int peerId = peer.getPeerId();
            try {
                boolean granted = transport.sendRequestVote(peerId, newTerm, nodeId, lastLogIndex, lastLogTerm);
                if (granted) {
                    votesReceived++;
                    if (votesReceived >= quorum) {
                        transitionToLeader();
                        return;
                    }
                } else {
                    long peerTerm = transport.getPeerTerm(peerId);
                    if (peerTerm > getCurrentTerm()) {
                        stepDown(peerTerm);
                        return;
                    }
                }
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to send RequestVote to peer " + peerId, e);
            }
        }
    }

    private void transitionToLeader() {
        if (getRole() != Role.CANDIDATE) {
            return;
        }
        setRole(Role.LEADER);
        leaderId = nodeId;
        LOGGER.info(() -> "Node " + nodeId + " became LEADER for term " + getCurrentTerm());

        long lastLogIndex = wal.getLastLogIndex();
        for (PeerNode peer : peers) {
            peer.setNextIndex(lastLogIndex + 1);
            peer.setMatchIndex(0);
        }

        // Append no-op entry for leader completeness invariant
        try {
            appendLogEntry((byte) 0x00, bufferAllocator.allocate(0));
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to append leader no-op entry", e);
        }

        sendAppendEntriesToAll(true);
    }

    public void stepDown(long term) {
        setCurrentTerm(term);
        setVotedFor(-1);
        setRole(Role.FOLLOWER);
        leaderId = -1;
        resetElectionTimeout();
        LOGGER.info(() -> "Node " + nodeId + " stepped down to FOLLOWER at term " + term);
    }

    public synchronized LogEntry appendLogEntry(byte opCode, ByteBuffer payload) throws IOException {
        if (getRole() != Role.LEADER) {
            throw new IllegalStateException("Node " + nodeId + " is not the leader. Current leader is " + leaderId);
        }

        long term = getCurrentTerm();
        long index = wal.getLastLogIndex() + 1;

        CRC32 crc32 = new CRC32();
        if (payload != null && payload.remaining() > 0) {
            payload.mark();
            crc32.update(payload);
            payload.reset();
        }
        long checksum = crc32.getValue();

        LogEntry entry = new LogEntry(term, index, opCode, payload, checksum);
        wal.append(entry);

        sendAppendEntriesToAll(false);
        return entry;
    }

    private void sendAppendEntriesToAll(boolean isHeartbeat) {
        long currentTermVal = getCurrentTerm();
        long commitIndexVal = (long) COMMIT_INDEX_VH.getAcquire(this);

        for (PeerNode peer : peers) {
            long prevLogIndex = peer.getNextIndex() - 1;
            long prevLogTerm = wal.getTermAtIndex(prevLogIndex);

            ByteBuffer entriesPayload = null;
            if (!isHeartbeat) {
                long lastLogIndex = wal.getLastLogIndex();
                if (lastLogIndex >= peer.getNextIndex()) {
                    entriesPayload = wal.readEntries(peer.getNextIndex(), lastLogIndex);
                }
            }

            try {
                boolean success = transport.sendAppendEntries(
                        peer.getPeerId(),
                        currentTermVal,
                        nodeId,
                        prevLogIndex,
                        prevLogTerm,
                        entriesPayload,
                        commitIndexVal
                );

                if (success) {
                    long lastLogIndex = wal.getLastLogIndex();
                    if (!isHeartbeat && lastLogIndex >= peer.getNextIndex()) {
                        peer.setMatchIndex(lastLogIndex);
                        peer.setNextIndex(lastLogIndex + 1);
                    }
                    peer.updateRTT(System.nanoTime());
                    checkAndUpdateCommitIndex();
                } else {
                    long peerTerm = transport.getPeerTerm(peer.getPeerId());
                    if (peerTerm > currentTermVal) {
                        stepDown(peerTerm);
                        return;
                    } else {
                        // Decrement nextIndex and retry on next cycle
                        long nextIdx = peer.getNextIndex();
                        if (nextIdx > 1) {
                            peer.setNextIndex(nextIdx - 1);
                        }
                    }
                }
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to send AppendEntries to peer " + peer.getPeerId(), e);
            }
        }
    }

    private void checkAndUpdateCommitIndex() {
        if (getRole() != Role.LEADER) {
            return;
        }

        long highestCommit = (long) COMMIT_INDEX_VH.getAcquire(this);
        long lastLogIndex = wal.getLastLogIndex();
        long currentTermVal = getCurrentTerm();

        for (long idx = lastLogIndex; idx > highestCommit; idx--) {
            if (wal.getTermAtIndex(idx) != currentTermVal) {
                continue;
            }

            int replicatedCount = 1; // Self
            int quorum = (peers.length + 1) / 2 + 1;

            for (PeerNode peer : peers) {
                if (peer.getMatchIndex() >= idx) {
                    replicatedCount++;
                }
            }

            if (replicatedCount >= quorum) {
                COMMIT_INDEX_VH.setRelease(this, idx);
                LOGGER.info(() -> "Leader " + nodeId + " advanced commitIndex to " + idx);
                break;
            }
        }
    }

    private void applyEntriesToStateMachine() {
        long commitIdx = (long) COMMIT_INDEX_VH.getAcquire(this);
        long appliedIdx = (long) LAST_APPLIED_VH.getAcquire(this);

        while (commitIdx > appliedIdx) {
            appliedIdx++;
            try {
                LogEntry entry = wal.getEntryAtIndex(appliedIdx);
                if (entry != null) {
                    executeStateMachineCommand(entry);
                }
                LAST_APPLIED_VH.setRelease(this, appliedIdx);
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to apply entry at index " + appliedIdx, e);
                break;
            }
            commitIdx = (long) COMMIT_INDEX_VH.getAcquire(this);
        }
    }

    private void executeStateMachineCommand(LogEntry entry) {
        // Production state machine execution hook
        // Payload is read directly from DirectByteBuffer pointer without heap allocation
        ByteBuffer payload = entry.getPayload();
        if (payload != null && payload.remaining() > 0) {
            // Process command payload based on OpCode
            byte opCode = entry.getOpCode();
            // Stateless processing for zero-allocation conformance
        }
    }

    public synchronized boolean handleRequestVote(long term, int candidateId, long lastLogIndex, long lastLogTerm) {
        long currentTermVal = getCurrentTerm();

        if (term > currentTermVal) {
            stepDown(term);
        }

        currentTermVal = getCurrentTerm();
        int votedForVal = getVotedFor();

        if (term < currentTermVal) {
            return false;
        }

        boolean voteGranted = false;
        if ((votedForVal == -1 || votedForVal == candidateId)) {
            long myLastLogTerm = wal.getLastLog