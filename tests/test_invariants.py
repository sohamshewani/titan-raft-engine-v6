package com.titan.raft.engine;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

/* =====================================================================
 * MOCK / STUB ENGINE ARCHITECTURE (Titan Raft Engine Implementation)
 * ===================================================================== */

enum NodeState {
    FOLLOWER, CANDIDATE, LEADER, SHUTDOWN
}

record LogEntry(long term, long index, byte[] data) {
    public int sizeBytes() {
        return 16 + (data != null ? data.length : 0);
    }
}

class RaftConfiguration {
    private final int nodeId;
    private final Set<Integer> clusterPeers;
    private final Duration electionTimeoutMin;
    private final Duration electionTimeoutMax;
    private final Duration heartbeatInterval;
    private final Path storagePath;

    public RaftConfiguration(int nodeId, Set<Integer> clusterPeers, Path storagePath) {
        this.nodeId = nodeId;
        this.clusterPeers = Set.copyOf(clusterPeers);
        this.electionTimeoutMin = Duration.ofMillis(150);
        this.electionTimeoutMax = Duration.ofMillis(300);
        this.heartbeatInterval = Duration.ofMillis(50);
        this.storagePath = storagePath;
    }

    public int getNodeId() { return nodeId; }
    public Set<Integer> getClusterPeers() { return clusterPeers; }
    public Path getStoragePath() { return storagePath; }
}

interface StateMachine {
    byte[] apply(byte[] command);
    void snapshot(FileChannel channel) throws IOException;
    void restore(FileChannel channel) throws IOException;
}

class DurabilityLog implements AutoCloseable {
    private final Path logFile;
    private FileChannel channel;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private long lastIndex = 0;
    private long lastTerm = 0;

    public DurabilityLog(Path directory) throws IOException {
        Files.createDirectories(directory);
        this.logFile = directory.resolve("raft.wal");
        this.channel = FileChannel.open(logFile, 
            StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        recover();
    }

    private void recover() throws IOException {
        lock.writeLock().lock();
        try {
            channel.position(0);
            ByteBuffer lengthBuf = ByteBuffer.allocate(4);
            while (channel.read(lengthBuf) == 4) {
                lengthBuf.flip();
                int size = lengthBuf.getInt();
                lengthBuf.clear();
                
                ByteBuffer dataBuf = ByteBuffer.allocate(size);
                channel.read(dataBuf);
                dataBuf.flip();
                
                long term = dataBuf.getLong();
                long index = dataBuf.getLong();
                int dataLen = dataBuf.getInt();
                byte[] data = new byte[dataLen];
                dataBuf.get(data);
                
                this.lastIndex = index;
                this.lastTerm = term;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void append(LogEntry entry) throws IOException {
        lock.writeLock().lock();
        try {
            ByteBuffer buf = ByteBuffer.allocate(entry.sizeBytes() + 4);
            buf.putInt(entry.sizeBytes());
            buf.putLong(entry.term());
            buf.putLong(entry.index());
            buf.putInt(entry.data() != null ? entry.data().length : 0);
            if (entry.data() != null) {
                buf.put(entry.data());
            }
            buf.flip();
            
            channel.position(channel.size());
            while (buf.hasRemaining()) {
                channel.write(buf);
            }
            channel.force(false);
            
            this.lastIndex = entry.index();
            this.lastTerm = entry.term();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<LogEntry> getEntriesFrom(long startIndex) throws IOException {
        lock.readLock().lock();
        try {
            List<LogEntry> entries = new ArrayList<>();
            channel.position(0);
            ByteBuffer lengthBuf = ByteBuffer.allocate(4);
            while (channel.read(lengthBuf) == 4) {
                lengthBuf.flip();
                int size = lengthBuf.getInt();
                lengthBuf.clear();
                
                ByteBuffer dataBuf = ByteBuffer.allocate(size);
                channel.read(dataBuf);
                dataBuf.flip();
                
                long term = dataBuf.getLong();
                long index = dataBuf.getLong();
                int dataLen = dataBuf.getInt();
                byte[] data = new byte[dataLen];
                dataBuf.get(data);
                
                if (index >= startIndex) {
                    entries.add(new LogEntry(term, index, data));
                }
            }
            return entries;
        } finally {
            lock.readLock().unlock();
        }
    }

    public long getLastIndex() {
        lock.readLock().lock();
        try { return lastIndex; }
        finally { lock.readLock().unlock(); }
    }

    public long getLastTerm() {
        lock.readLock().lock();
        try { return lastTerm; }
        finally { lock.readLock().unlock(); }
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}

class TitanRaftEngine implements AutoCloseable {
    private final RaftConfiguration config;
    private final StateMachine stateMachine;
    private final DurabilityLog durabilityLog;
    private final ReentrantReadWriteLock engineLock = new ReentrantReadWriteLock();

    private NodeState state = NodeState.FOLLOWER;
    private long currentTerm = 0;
    private Integer votedFor = null;
    private long commitIndex = 0;
    private long lastApplied = 0;

    private final Map<Integer, Long> nextIndex = new ConcurrentHashMap<>();
    private final Map<Integer, Long> matchIndex = new ConcurrentHashMap<>();
    
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> heartbeatTask;
    private ScheduledFuture<?> electionTask;
    private final Random random = new Random();
    
    private final List<Consumer<NodeState>> stateChangeListeners = new CopyOnWriteArrayList<>();

    public TitanRaftEngine(RaftConfiguration config, StateMachine stateMachine) throws IOException {
        this.config = config;
        this.stateMachine = stateMachine;
        this.durabilityLog = new DurabilityLog(config.getStoragePath());
        
        // Recover commit/apply tracking
        this.commitIndex = durabilityLog.getLastIndex();
        this.lastApplied = this.commitIndex;
        
        resetElectionTimeout();
    }

    public void addStateChangeListener(Consumer<NodeState> listener) {
        stateChangeListeners.add(listener);
    }

    private void changeState(NodeState newState) {
        engineLock.writeLock().lock();
        try {
            if (this.state == newState) return;
            this.state = newState;
            if (newState == NodeState.LEADER) {
                cancelElectionTimer();
                initializeLeaderState();
                startHeartbeatTimer();
            } else if (newState == NodeState.FOLLOWER || newState == NodeState.CANDIDATE) {
                cancelHeartbeatTimer();
                resetElectionTimeout();
            }
        } finally {
            engineLock.writeLock().unlock();
        }
        for (var listener : stateChangeListeners) {
            try {
                listener.accept(newState);
            } catch (Exception ignored) {}
        }
    }

    private void initializeLeaderState() {
        long lastIdx = durabilityLog.getLastIndex();
        for (int peer : config.getClusterPeers()) {
            if (peer != config.getNodeId()) {
                nextIndex.put(peer, lastIdx + 1);
                matchIndex.put(peer, 0L);
            }
        }
    }

    private void cancelElectionTimer() {
        if (electionTask != null) {
            electionTask.cancel(true);
        }
    }

    private void cancelHeartbeatTimer() {
        if (heartbeatTask != null) {
            heartbeatTask.cancel(true);
        }
    }

    private void resetElectionTimeout() {
        cancelElectionTimer();
        long timeout = 150 + random.nextInt(150);
        electionTask = scheduler.schedule(this::handleElectionTimeout, timeout, TimeUnit.MILLISECONDS);
    }

    private void startHeartbeatTimer() {
        cancelHeartbeatTimer();
        heartbeatTask = scheduler.scheduleAtFixedRate(this::sendHeartbeats, 
            0, 50, TimeUnit.MILLISECONDS);
    }

    private void handleElectionTimeout() {
        engineLock.writeLock().lock();
        try {
            if (state == NodeState.LEADER || state == NodeState.SHUTDOWN) return;
            
            state = NodeState.CANDIDATE;
            currentTerm++;
            votedFor = config.getNodeId();
            resetElectionTimeout();
            
            // Trigger auto-win if single node or isolated lab test
            if (config.getClusterPeers().size() <= 1) {
                changeState(NodeState.LEADER);
            }
        } finally {
            engineLock.writeLock().unlock();
        }
    }

    private void sendHeartbeats() {
        engineLock.readLock().lock();
        try {
            if (state != NodeState.LEADER) return;
            // Simulated heartbeat broadcast emission
        } finally {
            engineLock.readLock().unlock();
        }
    }

    public CompletableFuture<byte[]> propose(byte[] command) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        engineLock.writeLock().lock();
        try {
            if (state != NodeState.LEADER) {
                future.completeExceptionally(new IllegalStateException("Not the leader. Current state: " + state));
                return future;
            }
            
            long index = durabilityLog.getLastIndex() + 1;
            long term = currentTerm;
            LogEntry entry = new LogEntry(term, index, command);
            
            durabilityLog.append(entry);
            
            // Simulate single node direct commit
            if (config.getClusterPeers().size() <= 1) {
                commitIndex = index;
                byte[] result = stateMachine.apply(command);
                lastApplied = index;
                future.complete(result);
            } else {
                // Multi-node asynchronous replication stub
                future.complete(null);
            }
        } catch (Exception e) {
            future.completeExceptionally(e);
        } finally {
            engineLock.writeLock().unlock();
        }
        return future;
    }

    public void forceBecomeLeader() {
        engineLock.writeLock().lock();
        try {
            currentTerm++;
            votedFor = config.getNodeId();
        } finally {
            engineLock.writeLock().unlock();
        }
        changeState(NodeState.LEADER);
    }

    public NodeState getState() {
        engineLock.readLock().lock();
        try { return state; }
        finally { engineLock.readLock().unlock(); }
    }

    public long getCurrentTerm() {
        engineLock.readLock().lock();
        try { return currentTerm; }
        finally { engineLock.readLock().unlock(); }
    }

    public long getCommitIndex() {
        engineLock.readLock().lock();
        try