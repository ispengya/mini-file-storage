package com.ispengya.file.index;

import com.ispengya.file.core.SimpleMappedFile;
import com.ispengya.file.core.SimpleMappedFileQueue;

import java.io.IOException;
import java.nio.ByteBuffer;


public class SimpleConsumeQueue implements AutoCloseable {
    public static final int CQ_STORE_UNIT_SIZE = 20;

    /**
     * 负责管理索引文件的映射文件队列
     * 这里的 offset 空间是“索引文件自己的字节偏移”
     */
    private final SimpleMappedFileQueue mappedFileQueue;

    public SimpleConsumeQueue(String storePath, int mappedFileSize) {
        this.mappedFileQueue = new SimpleMappedFileQueue(storePath, alignFileSize(mappedFileSize));
    }

    public SimpleConsumeQueue(String storePath, int mappedFileSize, boolean recover) throws IOException {
        if (recover) {
            this.mappedFileQueue = SimpleMappedFileQueue.recoverForConsumeQueue(storePath, alignFileSize(mappedFileSize));
        } else {
            this.mappedFileQueue = new SimpleMappedFileQueue(storePath, alignFileSize(mappedFileSize));
        }
    }

    /**
     * 追加一条 20 字节索引单元
     *
     * @param size CommitLog 中该记录的帧总长(RecordFrame.HEADER_SIZE + bodyLen),
     *             与恢复扫描的推进量同源;v1 时代的"4 字节头 + payload"语义已随格式 v2 废弃
     */
    public synchronized void append(long physicalOffset, int size, long tagCode) {
        byte[] unit = new byte[CQ_STORE_UNIT_SIZE];
        ByteBuffer buffer = ByteBuffer.wrap(unit);
        buffer.putLong(physicalOffset);
        buffer.putInt(size);
        buffer.putLong(tagCode);
        try {
            SimpleMappedFile mappedFile = mappedFileQueue.getLastMappedFile(true);
            if (mappedFile == null) {
                throw new IllegalStateException("no mapped file for consume queue");
            }
            if (!mappedFile.append(unit, 0, CQ_STORE_UNIT_SIZE)) {
                mappedFile = mappedFileQueue.getLastMappedFile(true);
                if (mappedFile == null) {
                    throw new IllegalStateException("no mapped file for consume queue");
                }
                if (!mappedFile.append(unit, 0, CQ_STORE_UNIT_SIZE)) {
                    throw new IllegalStateException("append consume queue failed");
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized SimpleIndexEntry get(long logicalOffset) {
        if (logicalOffset < 0) {
            throw new IllegalArgumentException("logicalOffset must be non-negative");
        }
        long offsetInIndexFile = logicalOffset * CQ_STORE_UNIT_SIZE;
        SimpleMappedFile mappedFile = mappedFileQueue.findMappedFileByOffset(offsetInIndexFile);
        if (mappedFile == null) {
            return null;
        }
        long fileStart = mappedFile.getFileFromOffset();
        int position = (int) (offsetInIndexFile - fileStart);
        ByteBuffer buffer = mappedFile.read(position, CQ_STORE_UNIT_SIZE);
        long physicalOffset = buffer.getLong();
        int size = buffer.getInt();
        long tagCode = buffer.getLong();
        if (physicalOffset < 0 || size <= 0) {
            return null;
        }
        return new SimpleIndexEntry(physicalOffset, size, tagCode);
    }

    public synchronized long flush() {
        return mappedFileQueue.flush();
    }

    /**
     * 当前合法索引条目数(由恢复扫描/追加维护的文件写位置推导,不信任文件尾部残留)
     */
    public synchronized long count() {
        return mappedFileQueue.getMaxOffset() / CQ_STORE_UNIT_SIZE;
    }

    /**
     * ConsumeQueue 的"自时钟":CommitLog 中已被索引覆盖到的字节位点。
     * 由最后一条合法索引的 physicalOffset + size 得出——Reput 重启续传的精确起点。
     *
     * @return 已索引位点;空队列为 0
     */
    public synchronized long cqCursor() {
        long n = count();
        if (n == 0) {
            return 0L;
        }
        SimpleIndexEntry last = get(n - 1);
        if (last == null) {
            // 恢复扫描保证 wrotePosition 内全部单元合法,走到这里说明实现被破坏
            throw new IllegalStateException("consume queue tail entry invalid at count=" + n);
        }
        return last.getPhysicalOffset() + last.getSize();
    }

    /**
     * 交叉校验式尾部截断:丢弃所有"指向 maxLogOffset 之后数据"的索引条目。
     * 场景:断电后 CQ 的页落了盘、对应数据帧的页丢了——CQ 时钟超前于数据真相,
     * 这些条目是撒谎指针,必须从尾部弹出,让 Reput 起点回退到与数据一致的位置。
     *
     * @param maxLogOffset CommitLog 恢复后的合法写位置(数据真相)
     * @return 被截断的条目数
     */
    public synchronized long truncateBeyond(long maxLogOffset) {
        long truncated = 0;
        while (true) {
            long n = count();
            if (n == 0) {
                break;
            }
            SimpleIndexEntry last = get(n - 1);
            if (last != null && last.getPhysicalOffset() + last.getSize() <= maxLogOffset) {
                break; // 尾部条目落在数据真相之内,截断完成
            }
            // 条目越界(或本身是脏结构):弹掉最后一条索引单元
            mappedFileQueue.truncateTo((n - 1) * CQ_STORE_UNIT_SIZE);
            truncated++;
        }
        return truncated;
    }

    @Override
    public void close() {
        flush();
        mappedFileQueue.close();
    }

    private int alignFileSize(int mappedFileSize) {
        int units = mappedFileSize / CQ_STORE_UNIT_SIZE;
        if (units <= 0) {
            throw new IllegalArgumentException("mappedFileSize must be at least " + CQ_STORE_UNIT_SIZE);
        }
        return units * CQ_STORE_UNIT_SIZE;
    }
}
