package com.titan.raft.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

public final class WriteAheadLog implements AutoCloseable {

    public static final int RECORD_HEADER_SIZE = 8 + 8 + 1 + 4 + 4; // term(8) + index(8) + opCode(1) + payloadLength(4) + crc32(4) = 25 bytes
    private static final long DEFAULT_FILE_CAPACITY = 64L * 1024L * 1024L; // 64 MB per segment

    private final File directory;
    private final long segmentCapacity;
    private RandomAccessFile currentFile;
    private FileChannel currentChannel;
    private MappedByteBuffer currentMappedBuffer;
    
    private long currentSegmentBaseIndex = 1L;
    private long lastAppendedIndex = 0L;
    private long lastAppendedTerm = 0L;
    private boolean isOpen;

    public WriteAheadLog(File directory) throws IOException {
        this(directory, DEFAULT_FILE_CAPACITY);
    }

    public WriteAheadLog(File directory, long segmentCapacity) throws IOException {
        this.directory = directory;
        this.segmentCapacity = segmentCapacity;
        if (!directory.exists() && !directory.mkdirs()) {
            throw new IOException("Failed to create WAL directory: " + directory.getAbsolutePath());
        }
        this.isOpen = true;
        initializeAndRecover();
    }

    private void initializeAndRecover() throws IOException {
        File[] segmentFiles = directory.listFiles((dir, name) -> name.endsWith(".wal"));
        if (segmentFiles == null || segmentFiles.length == 0) {
            createNewSegment(1L);
        } else {
            File latestSegment = null;
            long maxBaseIndex = -1L;
            for (File file : segmentFiles) {
                String name = file.getName();
                try {
                    long baseIndex = Long.parseLong(name.substring(0, name.indexOf('.')));
                    if (baseIndex > maxBaseIndex) {
                        maxBaseIndex = baseIndex;
                        latestSegment = file;
                    }
                } catch (NumberFormatException e) {
                    // Ignore malformed file names
                }
            }

            if (latestSegment == null) {
                createNewSegment(1L);
            } else {
                currentSegmentBaseIndex = maxBaseIndex;
                currentFile = new RandomAccessFile(latestSegment, "rw");
                currentChannel = currentFile.getChannel();
                currentMappedBuffer = currentChannel.map(FileChannel.MapMode.READ_WRITE, 0, segmentCapacity);
                currentMappedBuffer.order(ByteOrder.LITTLE_ENDIAN);
                
                // Scan and recover valid records
                recoverSegment();
            }
        }
    }

    private void createNewSegment(long baseIndex) throws IOException {
        if (currentChannel != null) {
            currentChannel.force(false);
            currentChannel.close();
        }
        if (currentFile != null) {
            currentFile.close();
        }

        currentSegmentBaseIndex = baseIndex;
        File segmentFile = new File(directory, String.format("%020d.wal", baseIndex));
        currentFile = new RandomAccessFile(segmentFile, "rw");
        currentFile.setLength(segmentCapacity);
        currentChannel = currentFile.getChannel();
        currentMappedBuffer = currentChannel.map(FileChannel.MapMode.READ_WRITE, 0, segmentCapacity);
        currentMappedBuffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    private void recoverSegment() throws IOException {
        currentMappedBuffer.position(0);
        long validLimit = 0;
        
        while (currentMappedBuffer.remaining() >= RECORD_HEADER_SIZE) {
            int headerPos = currentMappedBuffer.position();
            
            long term = currentMappedBuffer.getLong();
            long index = currentMappedBuffer.getLong();
            byte opCode = currentMappedBuffer.get();
            int payloadLength = currentMappedBuffer.getInt();
            int storedCrc = currentMappedBuffer.getInt();

            // Check for unwritten zero-padding
            if (term == 0L && index == 0L && opCode == 0 && payloadLength == 0 && storedCrc == 0) {
                currentMappedBuffer.position(headerPos);
                break;
            }

            if (payloadLength < 0 || currentMappedBuffer.remaining() < payloadLength) {
                // Corrupted or partial record at tail
                break;
            }

            int payloadPos = currentMappedBuffer.position();
            CRC32 crc32 = new CRC32();
            // Compute CRC over term, index, opCode, payloadLength, and payload
            currentMappedBuffer.position(headerPos);
            byte[] headerBytes = new byte[RECORD_HEADER_SIZE - 4];
            currentMappedBuffer.get(headerBytes);
            crc32.update(headerBytes);

            byte[] payloadBytes = new byte[payloadLength];
            currentMappedBuffer.get(payloadBytes);
            crc32.update(payloadBytes);

            int computedCrc = (int) crc32.getValue();
            if (computedCrc != storedCrc) {
                // Checksum mismatch, truncate here
                currentMappedBuffer.position(headerPos);
                break;
            }

            lastAppendedIndex = index;
            lastAppendedTerm = term;
            validLimit = currentMappedBuffer.position();
        }

        currentMappedBuffer.position((int) validLimit);
    }

    public synchronized void append(long term, long index, byte opCode, java.nio.ByteBuffer payload) throws IOException {
        ensureOpen();
        
        int payloadLength = payload != null ? payload.remaining() : 0;
        int totalRecordSize = RECORD_HEADER_SIZE + payloadLength;

        if (currentMappedBuffer.remaining() < totalRecordSize) {
            currentChannel.force(false);
            createNewSegment(index);
        }

        int headerPos = currentMappedBuffer.position();
        
        currentMappedBuffer.putLong(term);
        currentMappedBuffer.putLong(index);
        currentMappedBuffer.put(opCode);
        currentMappedBuffer.putInt(payloadLength);
        
        int crcPlaceholderPos = currentMappedBuffer.position();
        currentMappedBuffer.putInt(0); // placeholder for CRC

        int payloadStartPos = currentMappedBuffer.position();
        if (payloadLength > 0) {
            int originalLimit = payload.limit();
            int originalPosition = payload.position();
            currentMappedBuffer.put(payload);
            payload.position(originalPosition);
            payload.limit(originalLimit);
        }

        // Compute CRC32
        CRC32 crc32 = new CRC32();
        currentMappedBuffer.position(headerPos);
        byte[] headerBytes = new byte[RECORD_HEADER_SIZE - 4];
        currentMappedBuffer.get(headerBytes);
        crc32.update(headerBytes);

        if (payloadLength > 0) {
            int originalLimit = payload.limit();
            int originalPosition = payload.position();
            byte[] payloadBytes = new byte[payloadLength];
            payload.get(payloadBytes);
            crc32.update(payloadBytes);
            payload.position(originalPosition);
            payload.limit(originalLimit);
        }

        int computedCrc = (int) crc32.getValue();

        // Write CRC back
        currentMappedBuffer.putInt(crcPlaceholderPos, computedCrc);
        // Restore position to end of record
        currentMappedBuffer.position(payloadStartPos + payloadLength);

        lastAppendedIndex = index;
        lastAppendedTerm = term;
    }

    public synchronized void flush() throws IOException {
        ensureOpen();
        if (currentChannel != null) {
            currentChannel.force(false);
        }
    }

    public synchronized void truncate(long index) throws IOException {
        ensureOpen();
        // If truncation falls back to a previous segment, we must delete newer segments
        File[] segmentFiles = directory.listFiles((dir, name) -> name.endsWith(".wal"));
        if (segmentFiles != null) {
            for (File file : segmentFiles) {
                String name = file.getName();
                try {
                    long baseIndex = Long.parseLong(name.substring(0, name.indexOf('.')));
                    if (baseIndex > index) {
                        if (currentChannel != null && currentSegmentBaseIndex == baseIndex) {
                            currentChannel.close();
                            currentFile.close();
                            currentChannel = null;
                            currentFile = null;
                        }
                        if (!file.delete()) {
                            throw new IOException("Failed to delete truncated WAL segment: " + file.getAbsolutePath());
                        }
                    }
                } catch (NumberFormatException e) {
                    // Ignore
                }
            }
        }

        // Re-locate current active segment containing 'index' or lower
        initializeAndRecover();
        
        // Scan current segment to find exact byte offset for 'index' and zero out remainder
        currentMappedBuffer.position(0);
        boolean found = false;
        int targetPosition = 0;

        while (currentMappedBuffer.remaining() >= RECORD_HEADER_SIZE) {
            int headerPos = currentMappedBuffer.position();
            long term = currentMappedBuffer.getLong();
            long entryIndex = currentMappedBuffer.getLong();
            byte opCode = currentMappedBuffer.get();
            int payloadLength = currentMappedBuffer.getInt();
            int storedCrc = currentMappedBuffer.getInt();

            if (term == 0L && entryIndex == 0L) {
                break;
            }

            if (entryIndex == index) {
                targetPosition = headerPos;
                found = true;
                break;
            }

            if (entryIndex > index) {
                targetPosition = headerPos;
                found = true;
                break;
            }

            currentMappedBuffer.position(currentMappedBuffer.position() + payloadLength);
        }

        if (found) {
            currentMappedBuffer.position(targetPosition);
            int remainingBytes = currentMappedBuffer.remaining();
            byte[] zeros = new byte[Math.min(remainingBytes, 4096)];
            while (currentMappedBuffer.hasRemaining()) {
                int toWrite = Math.min(currentMappedBuffer.remaining(), zeros.length);
                currentMappedBuffer.put(zeros, 0, toWrite);
            }
            currentMappedBuffer.position(targetPosition);
        }

        // Recompute lastAppendedIndex
        scanLastAppended();
    }

    private void scanLastAppended() throws IOException {
        lastAppendedIndex = 0L;
        lastAppendedTerm = 0L;
        currentMappedBuffer.position(0);

        while (currentMappedBuffer.remaining() >= RECORD_HEADER_SIZE) {
            int headerPos = currentMappedBuffer.position();
            long term = currentMappedBuffer.getLong();
            long index = currentMappedBuffer.getLong();
            byte opCode = currentMappedBuffer.get();
            int payloadLength = currentMappedBuffer.getInt();
            int storedCrc = currentMappedBuffer.getInt();

            if (term == 0L && index == 0L) {
                currentMappedBuffer.position(headerPos);
                break;
            }

            if (currentMappedBuffer.remaining() < payloadLength) {
                currentMappedBuffer.position(headerPos);
                break;
            }

            currentMappedBuffer.position(currentMappedBuffer.position() + payloadLength);
            lastAppendedIndex = index;
            lastAppendedTerm = term;
        }
    }

    public synchronized LogEntry read(long index) throws IOException {
        ensureOpen();
        // Search across all segments from oldest to newest
        File[] segmentFiles = directory.listFiles((dir, name) -> name.endsWith(".wal"));
        if (segmentFiles == null) {
            return null;
        }

        java.util.Arrays.sort(segmentFiles);
        for (File file : segmentFiles) {
            try (RandomAccessFile raf = new RandomAccessFile(file, "r");
                 FileChannel channel = raf.getChannel()) {
                MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
                buffer.order(ByteOrder.LITTLE_ENDIAN);

                while (buffer.remaining() >= RECORD_HEADER_SIZE) {
                    int headerPos = buffer.position();
                    long term = buffer.getLong();
                    long entryIndex = buffer.getLong();
                    byte opCode = buffer.get();
                    int payloadLength = buffer.getInt();
                    int storedCrc = buffer.getInt();

                    if (term == 0L && entryIndex == 0L) {
                        break;
                    }

                    ByteBuffer payload = ByteBuffer.allocateDirect(payloadLength);
                    buffer.get(payload.array(), 0, payloadLength);
                    payload.position(0);

                    if (entryIndex == index) {
                        return new LogEntry(term, index, opCode, payload, storedCrc);
                    }
                }
            }
        }
        return null;
    }

    public long getLastAppendedIndex() {
        return lastAppendedIndex;
    }

    public long getLastAppendedTerm() {
        return lastAppendedTerm;
    }

    private void ensureOpen() throws IOException {
        if (!isOpen) {
            throw new IOException("WriteAheadLog is closed");
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (!isOpen) {
            return;
        }
        isOpen = false;
        if (currentChannel != null) {
            currentChannel.force(false);
            currentChannel.close();
        }
        if (currentFile != null) {
            currentFile.close();
        }
    }

    public static final class LogEntry {
        private final long term;
        private final long index;
        private final byte opCode;
        private final ByteBuffer payload;
        private final int crc32;

        public LogEntry(long term, long index, byte opCode, ByteBuffer payload, int crc32) {
            this.term = term;
            this.index = index;
            this.opCode = opCode;
            this.payload = payload;
            this.crc32 = crc32;
        }

        public long getTerm() {
            return term;
        }

        public long getIndex() {
            return index;
        }

        public byte getOpCode() {
            return opCode;
        }

        public ByteBuffer getPayload() {
            return payload;
        }

        public int getCrc32() {
            return crc32;
        }
    }
}