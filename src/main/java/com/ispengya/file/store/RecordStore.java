package com.ispengya.file.store;

import com.ispengya.file.codec.Codec;
import com.ispengya.file.core.SequentialLog;
import com.ispengya.file.index.SimpleConsumeQueue;
import com.ispengya.file.index.SimpleIndexEntry;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


public class RecordStore<T> implements AutoCloseable {
    private final SequentialLog log;
    private final Codec<T> codec;
    private final SimpleConsumeQueue consumeQueue;
    private final StoreCheckpoint checkpoint;

    public RecordStore(SequentialLog log, Codec<T> codec) {
        this(log, codec, null, null);
    }

    public RecordStore(SequentialLog log, Codec<T> codec, SimpleConsumeQueue consumeQueue) {
        this(log, codec, consumeQueue, null);
    }

    public RecordStore(SequentialLog log, Codec<T> codec, SimpleConsumeQueue consumeQueue, StoreCheckpoint checkpoint) {
        this.log = log;
        this.codec = codec;
        this.consumeQueue = consumeQueue;
        this.checkpoint = checkpoint;
    }

    public synchronized long append(T value) {
        return append(value, 0L);
    }

    public synchronized long append(T value, long tagCode) {
        byte[] payload = codec.encode(value);
        int length = payload.length;
        ByteBuffer buffer = ByteBuffer.allocate(4 + length);
        buffer.putInt(length);
        buffer.put(payload);
        byte[] record = buffer.array();
        long maxOffset = log.append(record);
        long recordOffset = maxOffset - record.length;
        if (consumeQueue != null) {
            consumeQueue.append(recordOffset, record.length, tagCode);
        }
        return recordOffset;
    }

    public synchronized T read(long offset) {
        byte[] header = log.read(offset, 4);
        int length = ByteBuffer.wrap(header).getInt();
        byte[] payload = log.read(offset + 4, length);
        return codec.decode(payload);
    }

    public synchronized T readByLogicalOffset(long logicalOffset) {
        if (consumeQueue == null) {
            throw new IllegalStateException("consume queue not configured");
        }
        SimpleIndexEntry entry = consumeQueue.get(logicalOffset);
        if (entry == null) {
            return null;
        }
        return read(entry.getPhysicalOffset());
    }

    public synchronized List<T> queryByTimeRange(long beginTimestamp, long endTimestamp) {
        if (consumeQueue == null) {
            throw new IllegalStateException("consume queue not configured");
        }
        List<T> result = new ArrayList<>();
        long logicalOffset = 0;
        while (true) {
            SimpleIndexEntry entry = consumeQueue.get(logicalOffset);
            if (entry == null) {
                break;
            }
            long ts = entry.getTagCode();
            if (ts < beginTimestamp) {
                logicalOffset++;
                continue;
            }
            if (ts > endTimestamp) {
                break;
            }
            result.add(read(entry.getPhysicalOffset()));
            logicalOffset++;
        }
        return result;
    }

    public synchronized long flush() {
        long offset = log.flush();
        long cqOffset = -1;
        if (consumeQueue != null) {
            cqOffset = consumeQueue.flush();
        }
        if (checkpoint != null) {
            checkpoint.save(offset, cqOffset);
        }
        return offset;
    }

    @Override
    public void close() {
        flush();
        log.close();
        if (consumeQueue != null) {
            consumeQueue.close();
        }
    }

    public static class StoreCheckpoint {
        private final String filePath;

        public StoreCheckpoint(String filePath) {
            this.filePath = filePath;
        }

        public void save(long commitLogMaxOffset, long consumeQueueMaxOffset) {
            File file = new File(filePath);
            File parent = file.getParentFile();
            if (parent != null && !parent.exists()) {
                parent.mkdirs();
            }
            ByteBuffer buffer = ByteBuffer.allocate(16);
            buffer.putLong(commitLogMaxOffset);
            buffer.putLong(consumeQueueMaxOffset);
            buffer.flip();
            try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
                raf.setLength(16);
                raf.getChannel().write(buffer);
                raf.getChannel().force(true);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public long[] load() {
            File file = new File(filePath);
            if (!file.exists() || file.length() < 16) {
                return null;
            }
            ByteBuffer buffer = ByteBuffer.allocate(16);
            try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
                int read = raf.getChannel().read(buffer);
                if (read < 16) {
                    return null;
                }
                buffer.flip();
                long commitLogMaxOffset = buffer.getLong();
                long consumeQueueMaxOffset = buffer.getLong();
                return new long[]{commitLogMaxOffset, consumeQueueMaxOffset};
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}