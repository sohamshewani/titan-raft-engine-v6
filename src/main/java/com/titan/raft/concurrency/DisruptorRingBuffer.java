package com.titan.raft.concurrency;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.titan.raft.model.LogEntry;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * Production-grade LMAX Disruptor ring buffer wrapper implementing the Single-Writer Principle
 * for the titan-raft-engine. Provides zero-allocation ring publishing, cache-line padded
 * sequence tracking, and deterministic event-handler routing for Raft state machine transitions
 * and log append operations.
 */
public final class DisruptorRingBuffer implements AutoCloseable {

    private static final Unsafe UNSAFE;

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize sun.misc.Unsafe", e);
        }
    }

    // Cache line padding fields to prevent false sharing under high core contention
    @SuppressWarnings("unused")
    private long p0, p1, p2, p3, p4, p5, p6, p7;

    private final int bufferSize;
    private final Disruptor<RaftEvent> disruptor;
    private final RingBuffer<RaftEvent> ringBuffer;
    private final AtomicBoolean isRunning;
    
    // Cache line padding trailing
    @SuppressWarnings("unused")
    private long q0, q1, q2, q3, q4, q5, q6, q7;

    /**
     * Represents a single mutator event slot within the Disruptor ring buffer.
     * Designed to be recycled continuously to ensure zero heap allocations during steady-state execution.
     */
    public static final class RaftEvent {
        private byte opCode;
        private long term;
        private long index;
        private ByteBuffer payload;
        private long crc32;
        private BiConsumer<Long, Long> completionCallback;

        public void reset() {
            this.opCode = 0;
            this.term = 0L;
            this.index = 0L;
            this.payload = null;
            this.crc32 = 0L;
            this.completionCallback = null;
        }

        public byte getOpCode() { return opCode; }
        public void setOpCode(byte opCode) { this.opCode = opCode; }

        public long getTerm() { return term; }
        public void setTerm(long term) { this.term = term; }

        public long getIndex() { return index; }
        public void setIndex(long index) { this.index = index; }

        public ByteBuffer getPayload() { return payload; }
        public void setPayload(ByteBuffer payload) { this.payload = payload; }

        public long getCrc32() { return crc32; }
        public void setCrc32(long crc32) { this.crc32 = crc32; }

        public BiConsumer<Long, Long> getCompletionCallback() { return completionCallback; }
        public void setCompletionCallback(BiConsumer<Long, Long> completionCallback) {
            this.completionCallback = completionCallback;
        }
    }

    /**
     * Constructs and initializes the Disruptor ring buffer with a YieldingWaitStrategy optimized
     * for ultra-low latency multi-core consensus loops.
     *
     * @param bufferSize size of the ring buffer, must be a power of 2.
     * @param eventHandler execution router for consumed events.
     */
    public DisruptorRingBuffer(int bufferSize, com.lmax.disruptor.EventHandler<RaftEvent> eventHandler) {
        if (!isPowerOfTwo(bufferSize)) {
            throw new IllegalArgumentException("Ring buffer size must be a power of two: " + bufferSize);
        }
        this.bufferSize = bufferSize;
        this.isRunning = new AtomicBoolean(false);

        ThreadFactory threadFactory = r -> {
            Thread t = new Thread(r, "titan-raft-disruptor-thread");
            t.setDaemon(true);
            return t;
        };

        WaitStrategy waitStrategy = new YieldingWaitStrategy();

        this.disruptor = new Disruptor<>(
                RaftEvent::new,
                this.bufferSize,
                threadFactory,
                ProducerType.SINGLE,
                waitStrategy
        );

        this.disruptor.handleEventsWith(eventHandler);
        this.ringBuffer = this.disruptor.getRingBuffer();
    }

    /**
     * Starts the background event processor thread pool of the Disruptor.
     */
    public void start() {
        if (isRunning.compareAndSet(false, true)) {
            disruptor.start();
        }
    }

    /**
     * Publishes a log entry submission event to the ring buffer lock-free.
     *
     * @param term current Raft term.
     * @param index assigned log index.
     * @param opCode command operation code.
     * @param payload direct byte buffer payload containing command bytes.
     * @param crc32 checksum verification code.
     * @param callback asynchronous completion callback executed post-commit.
     */
    public void publishLogEntry(long term, long index, byte opCode, ByteBuffer payload, long crc32, BiConsumer<Long, Long> callback) {
        checkRunning();
        long sequence = ringBuffer.next();
        try {
            RaftEvent event = ringBuffer.get(sequence);
            event.reset();
            event.setOpCode(opCode);
            event.setTerm(term);
            event.setIndex(index);
            event.setPayload(payload);
            event.setCrc32(crc32);
            event.setCompletionCallback(callback);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    /**
     * Publishes a generic administrative or state machine transition event to the ring buffer.
     *
     * @param opCode operation code indicating specific engine action.
     * @param term term associated with action.
     * @param index index associated with action.
     */
    public void publishControlEvent(byte opCode, long term, long index) {
        checkRunning();
        long sequence = ringBuffer.next();
        try {
            RaftEvent event = ringBuffer.get(sequence);
            event.reset();
            event.setOpCode(opCode);
            event.setTerm(term);
            event.setIndex(index);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    /**
     * Attempts to publish a log entry without blocking if the ring buffer is exhausted.
     *
     * @return true if successfully published, false if ring buffer is full.
     */
    public boolean tryPublishLogEntry(long term, long index, byte opCode, ByteBuffer payload, long crc32, BiConsumer<Long, Long> callback) {
        checkRunning();
        long sequence;
        try {
            sequence = ringBuffer.tryNext();
        } catch (com.lmax.disruptor.InsufficientCapacityException e) {
            return false;
        }

        try {
            RaftEvent event = ringBuffer.get(sequence);
            event.reset();
            event.setOpCode(opCode);
            event.setTerm(term);
            event.setIndex(index);
            event.setPayload(payload);
            event.setCrc32(crc32);
            event.setCompletionCallback(callback);
        } finally {
            ringBuffer.publish(sequence);
        }
        return true;
    }

    /**
     * Returns the remaining capacity available in the ring buffer.
     */
    public long remainingCapacity() {
        return ringBuffer.remainingCapacity();
    }

    /**
     * Gracefully shuts down the Disruptor and drains outstanding events.
     */
    @Override
    public void close() {
        if (isRunning.compareAndSet(true, false)) {
            try {
                disruptor.shutdown();
            } catch (Exception e) {
                // Log or handle shutdown exception cleanly
                throw new RuntimeException("Error shutting down Disruptor ring buffer", e);
            }
        }
    }

    private void checkRunning() {
        if (!isRunning.get()) {
            throw new IllegalStateException("DisruptorRingBuffer has not been started or is already closed.");
        }
    }

    private static boolean isPowerOfTwo(int value) {
        return value > 0 && (value & (value - 1)) == 0;
    }
}