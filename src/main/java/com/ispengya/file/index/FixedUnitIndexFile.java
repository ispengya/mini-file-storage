package com.ispengya.file.index;

import com.ispengya.file.core.SimpleMappedFile;
import com.ispengya.file.core.SimpleMappedFileQueue;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * 定长 entry 的分段索引文件,复用内核的 mmap 分段/滚动/恢复/刷盘能力。
 *
 * <p>自定义索引(例如带 topic/queueId 的 ConsumeQueue)只需提供:</p>
 * <ul>
 *   <li>一个 {@link IndexEntryCodec},定义 entry 布局与合法性;</li>
 *   <li>一个目录和单文件大小;</li>
 * </ul>
 * 即可获得 append / get / count / truncate / flush / 崩溃后按有效前缀恢复,
 * 不再重复实现文件滚动与映射管理。</p>
 *
 * <p>线程模型:实例方法互斥;同一个目录不应被多个实例同时写入。</p>
 *
 * @param <E> entry 类型
 */
public class FixedUnitIndexFile<E> implements AutoCloseable {

    private final SimpleMappedFileQueue mappedFileQueue;
    private final IndexEntryCodec<E> codec;
    private final int unitSize;

    private FixedUnitIndexFile(SimpleMappedFileQueue mappedFileQueue, IndexEntryCodec<E> codec) {
        this.mappedFileQueue = mappedFileQueue;
        this.codec = codec;
        this.unitSize = codec.unitSize();
    }

    /**
     * 打开并恢复一个定长索引文件目录
     *
     * @param storePath 索引文件目录
     * @param fileSize  单文件大小(内部向下对齐到 unitSize 的整数倍)
     * @param codec     entry 编解码
     */
    public static <E> FixedUnitIndexFile<E> open(String storePath, int fileSize,
                                                 IndexEntryCodec<E> codec) throws IOException {
        if (codec == null) {
            throw new IllegalArgumentException("codec must not be null");
        }
        int unitSize = codec.unitSize();
        if (unitSize <= 0) {
            throw new IllegalArgumentException("unitSize must be positive");
        }
        int alignedFileSize = (fileSize / unitSize) * unitSize;
        if (alignedFileSize < unitSize) {
            throw new IllegalArgumentException("fileSize must be at least one entry: " + fileSize);
        }
        SimpleMappedFileQueue queue = SimpleMappedFileQueue.recoverWith(storePath, alignedFileSize,
                (file, size, mapped) -> scanValidPrefix(mapped, size, unitSize, codec));
        return new FixedUnitIndexFile<>(queue, codec);
    }

    /**
     * 追加一条 entry,空间不足时自动滚动到下一个文件
     */
    public synchronized void append(E entry) {
        if (entry == null || !codec.isValid(entry)) {
            throw new IllegalArgumentException("invalid index entry: " + entry);
        }
        ByteBuffer unit = ByteBuffer.allocate(unitSize);
        codec.encode(entry, unit);
        if (unit.position() != unitSize) {
            throw new IllegalStateException("codec must fill the whole entry: wrote "
                    + unit.position() + " of " + unitSize);
        }
        byte[] bytes = unit.array();
        try {
            SimpleMappedFile mappedFile = mappedFileQueue.getLastMappedFile(true);
            if (mappedFile == null) {
                throw new IllegalStateException("no mapped file for index");
            }
            if (!mappedFile.append(bytes, 0, unitSize)) {
                mappedFile = mappedFileQueue.getLastMappedFile(true);
                if (mappedFile == null || !mappedFile.append(bytes, 0, unitSize)) {
                    throw new IllegalStateException("append index entry failed");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("append index entry failed: " + storePath(), e);
        }
    }

    /**
     * 按逻辑位点读取(从 0 开始);越界或 entry 无效时返回 null
     */
    public synchronized E get(long logicalIndex) {
        if (logicalIndex < 0) {
            throw new IllegalArgumentException("logicalIndex must be non-negative");
        }
        long offsetInIndexFile = logicalIndex * unitSize;
        SimpleMappedFile mappedFile = mappedFileQueue.findMappedFileByOffset(offsetInIndexFile);
        if (mappedFile == null) {
            return null;
        }
        int position = (int) (offsetInIndexFile - mappedFile.getFileFromOffset());
        ByteBuffer unit = mappedFile.read(position, unitSize);
        E entry = codec.decode(unit);
        return entry != null && codec.isValid(entry) ? entry : null;
    }

    /**
     * 当前有效 entry 数
     */
    public synchronized long count() {
        return mappedFileQueue.getMaxOffset() / unitSize;
    }

    /**
     * 逻辑截断到指定 entry 数(之后的追加原位覆盖)
     */
    public synchronized void truncateTo(long entryCount) {
        if (entryCount < 0) {
            throw new IllegalArgumentException("entryCount must be non-negative");
        }
        long current = count();
        if (entryCount > current) {
            throw new IllegalArgumentException("cannot extend index by truncateTo: current="
                    + current + ", requested=" + entryCount);
        }
        mappedFileQueue.truncateTo(entryCount * unitSize);
    }

    public synchronized void flush() {
        mappedFileQueue.flush();
    }

    @Override
    public synchronized void close() {
        mappedFileQueue.close();
    }

    public int unitSize() {
        return unitSize;
    }

    private String storePath() {
        return mappedFileQueue.getStorePath();
    }

    /**
     * 按 codec 的有效性定义恢复每个文件的有效前缀
     */
    private static <E> int scanValidPrefix(ByteBuffer mapped, int fileSize, int unitSize,
                                           IndexEntryCodec<E> codec) {
        int position = 0;
        while (position + unitSize <= fileSize) {
            ByteBuffer unit = mapped.duplicate();
            unit.position(position);
            unit.limit(position + unitSize);
            E entry;
            try {
                entry = codec.decode(unit);
            } catch (RuntimeException e) {
                break;
            }
            if (entry == null || !codec.isValid(entry)) {
                break;
            }
            position += unitSize;
        }
        return position;
    }
}
