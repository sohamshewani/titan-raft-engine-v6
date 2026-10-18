package com.titan.raft.memory;

import sun.misc.Unsafe;

import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;

public final class DirectBufferAllocator implements AutoCloseable {

    private static final Unsafe UNSAFE;
    
    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            UNSAFE = (Unsafe) field.get(null);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize Unsafe for DirectBufferAllocator", e);
        }
    }

    private static final int CACHE_LINE_SIZE = 64;
    private static final int PAGE_SIZE = UNSAFE.pageSize();

    private final long arenaCapacity;
    private final long baseAddress;
    private final AtomicLong allocatedBytes;
    private final StampedLock lock;
    
    private long currentOffset;
    private volatile boolean isClosed;

    @SuppressWarnings("unused")
    private long p0, p1, p2, p3, p4, p5, p6, p7; // Cache-line padding for state variables

    public DirectBufferAllocator(long arenaCapacity) {
        if (arenaCapacity <= 0) {
            throw new IllegalArgumentException("Arena capacity must be greater than zero.");
        }
        
        // Align capacity to page size
        long remainder = arenaCapacity % PAGE_SIZE;
        if (remainder != 0) {
            this.arenaCapacity = arenaCapacity + (PAGE_SIZE - remainder);
        } else {
            this.arenaCapacity = arenaCapacity;
        }

        this.baseAddress = UNSAFE.allocateMemory(this.arenaCapacity);
        if (this.baseAddress == 0L) {
            throw new OutOfMemoryError("Failed to allocate direct memory arena of size: " + this.arenaCapacity);
        }

        // Zero out the allocated arena memory for safety
        UNSAFE.setMemory(this.baseAddress, this.arenaCapacity, (byte) 0);

        this.allocatedBytes = new AtomicLong(0L);
        this.lock = new StampedLock();
        this.currentOffset = 0L;
        this.isClosed = false;
    }

    public long allocate(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("Allocation size must be greater than zero.");
        }
        if (isClosed) {
            throw new IllegalStateException("Allocator arena is closed.");
        }

        // 8-byte alignment for performance
        int alignedSize = (size + 7) & ~7;

        long stamp = lock.writeLock();
        try {
            if (isClosed) {
                throw new IllegalStateException("Allocator arena is closed.");
            }

            if (currentOffset + alignedSize > arenaCapacity) {
                throw new OutOfMemoryError("Arena exhausted. Requested: " + alignedSize + 
                        ", Available: " + (arenaCapacity - currentOffset));
            }

            long allocatedAddress = baseAddress + currentOffset;
            currentOffset += alignedSize;
            allocatedBytes.addAndGet(alignedSize);

            return allocatedAddress;
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public ByteBuffer wrap(long address, int capacity) {
        if (isClosed) {
            throw new IllegalStateException("Allocator arena is closed.");
        }
        if (address < baseAddress || address + capacity > baseAddress + arenaCapacity) {
            throw new IndexOutOfBoundsException("Address range out of arena bounds.");
        }

        try {
            // Using standard reflection to instantiate direct ByteBuffer mapped to address
            ByteBuffer buffer = ByteBuffer.allocateDirect(0);
            
            // Set address field
            Field addressField = ByteBuffer.class.getDeclaredField("address");
            addressField.setAccessible(true);
            addressField.setLong(buffer, address);

            // Set capacity and limit fields
            Field capacityField = ByteBuffer.class.getDeclaredField("capacity");
            capacityField.setAccessible(true);
            capacityField.setInt(buffer, capacity);

            Field limitField = ByteBuffer.class.getDeclaredField("limit");
            limitField.setAccessible(true);
            limitField.setInt(buffer, capacity);

            buffer.order(ByteOrder.BIG_ENDIAN);
            buffer.position(0);
            return buffer;
        } catch (Exception e) {
            throw new RuntimeException("Failed to wrap native memory address into ByteBuffer", e);
        }
    }

    public MemorySegment wrapAsSegment(long address, long bytesSize) {
        if (isClosed) {
            throw new IllegalStateException("Allocator arena is closed.");
        }
        if (address < baseAddress || address + bytesSize > baseAddress + arenaCapacity) {
            throw new IndexOutOfBoundsException("Address range out of arena bounds.");
        }
        return MemorySegment.ofAddress(address);
    }

    public void reset() {
        long stamp = lock.writeLock();
        try {
            if (isClosed) {
                throw new IllegalStateException("Allocator arena is closed.");
            }
            UNSAFE.setMemory(baseAddress, arenaCapacity, (byte) 0);
            currentOffset = 0L;
            allocatedBytes.set(0L);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public long getAllocatedBytes() {
        return allocatedBytes.get();
    }

    public long getCapacity() {
        return arenaCapacity;
    }

    public long getRemainingBytes() {
        long stamp = lock.readLock();
        try {
            return arenaCapacity - currentOffset;
        } finally {
            lock.unlockRead(stamp);
        }
    }

    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void close() {
        if (isClosed) {
            return;
        }

        long stamp = lock.writeLock();
        try {
            if (isClosed) {
                return;
            }
            isClosed = true;
            if (baseAddress != 0L) {
                UNSAFE.freeMemory(baseAddress);
            }
        } finally {
            lock.unlockWrite(stamp);
        }
    }
}