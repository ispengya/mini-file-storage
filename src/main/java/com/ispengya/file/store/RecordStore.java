package com.ispengya.file.store;

import com.ispengya.file.api.StoreCorruptedException;
import com.ispengya.file.codec.Codec;
import com.ispengya.file.core.RecordFrame;
import com.ispengya.file.core.SequentialLog;
import com.ispengya.file.index.SimpleConsumeQueue;
import com.ispengya.file.index.SimpleIndexEntry;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * 通用记录存储引擎(格式 v2)
 *
 * <p>磁盘单元为 {@link RecordFrame} 自校验帧:组帧、校验、扫描推进全部委托 RecordFrame,
 * 本类只负责"对象 → 帧 → CommitLog + ConsumeQueue"的编排。帧头携带
 * storeTimestamp/tagCode/keyHash,保证未来可由 CommitLog 单独重建索引(唯一真相源)。</p>
 */
public class RecordStore<T> implements AutoCloseable {

    /**
     * 默认单条记录 body 上限
     */
    public static final int DEFAULT_MAX_BODY_SIZE = 1024 * 1024;

    private final SequentialLog log;
    private final Codec<T> codec;
    private final SimpleConsumeQueue consumeQueue;
    private final StoreCheckpoint checkpoint;
    private final int maxBodySize;

    public RecordStore(SequentialLog log, Codec<T> codec) {
        this(log, codec, null, null);
    }

    public RecordStore(SequentialLog log, Codec<T> codec, SimpleConsumeQueue consumeQueue) {
        this(log, codec, consumeQueue, null);
    }

    public RecordStore(SequentialLog log, Codec<T> codec, SimpleConsumeQueue consumeQueue, StoreCheckpoint checkpoint) {
        this(log, codec, consumeQueue, checkpoint, DEFAULT_MAX_BODY_SIZE);
    }

    public RecordStore(SequentialLog log, Codec<T> codec, SimpleConsumeQueue consumeQueue,
                       StoreCheckpoint checkpoint, int maxBodySize) {
        if (maxBodySize <= 0) {
            throw new IllegalArgumentException("maxBodySize must be positive");
        }
        this.log = log;
        this.codec = codec;
        this.consumeQueue = consumeQueue;
        this.checkpoint = checkpoint;
        this.maxBodySize = maxBodySize;
    }

    /**
     * 写入一条记录,tagCode 与引擎写入时间一致(默认时间语义查询可用)
     */
    public synchronized long append(T value) {
        return append(value, System.currentTimeMillis(), 0L);
    }

    /**
     * 写入一条记录并指定 tagCode(同步落入帧头与 ConsumeQueue)
     */
    public synchronized long append(T value, long tagCode) {
        return append(value, tagCode, 0L);
    }

    /**
     * 写入一条记录,tagCode 与 keyHash 均落入帧头
     *
     * <p>不变式:帧内 tagCode == ConsumeQueue 内 tagCode;
     * ConsumeQueue 的 size == 帧总长(与恢复扫描的推进量同源)。</p>
     *
     * @return 帧在 CommitLog 中的起始物理 offset
     */
    public synchronized long append(T value, long tagCode, long keyHash) {
        byte[] body = codec.encode(value);
        if (body.length > maxBodySize) {
            throw new IllegalArgumentException("body size " + body.length + " exceeds maxBodySize " + maxBodySize);
        }
        byte[] frame = RecordFrame.encode(System.currentTimeMillis(), tagCode, keyHash, body);
        long maxOffset = log.append(frame);
        long recordOffset = maxOffset - frame.length;
        if (consumeQueue != null) {
            consumeQueue.append(recordOffset, frame.length, tagCode);
        }
        return recordOffset;
    }

    /**
     * 按物理 offset 读取一条记录;魔数/CRC 校验失败抛 {@link StoreCorruptedException}
     */
    public synchronized T read(long offset) {
        byte[] header = log.read(offset, RecordFrame.HEADER_SIZE);
        int bodyLen = ByteBuffer.wrap(header).getInt(4);
        if (bodyLen < 0 || bodyLen > maxBodySize) {
            throw new StoreCorruptedException(offset, "invalid bodyLen: " + bodyLen);
        }
        byte[] frame = log.read(offset, RecordFrame.HEADER_SIZE + bodyLen);
        RecordFrame.Frame parsed = RecordFrame.parse(ByteBuffer.wrap(frame), 0, frame.length, maxBodySize);
        if (parsed.getStatus() != RecordFrame.Status.OK) {
            throw new StoreCorruptedException(offset, "frame status: " + parsed.getStatus());
        }
        byte[] body = Arrays.copyOfRange(frame, RecordFrame.HEADER_SIZE, frame.length);
        return codec.decode(body);
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