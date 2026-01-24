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
