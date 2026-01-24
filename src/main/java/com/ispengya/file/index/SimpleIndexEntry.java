package com.ispengya.file.index;

/**
 * 简化版索引条目
 * 描述一条记录在顺序日志中的位置和大小
 */
public class SimpleIndexEntry {
    private final long physicalOffset;
    private final int size;
    private final long tagCode;

    public SimpleIndexEntry(long physicalOffset, int size, long tagCode) {
        this.physicalOffset = physicalOffset;
        this.size = size;
        this.tagCode = tagCode;
    }

    public long getPhysicalOffset() {
        return physicalOffset;
    }

    public int getSize() {
        return size;
    }

    public long getTagCode() {
        return tagCode;
    }
}

