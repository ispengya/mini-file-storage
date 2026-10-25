package com.ispengya.file.api;

import com.ispengya.file.index.SimpleConsumeQueue;
import com.ispengya.file.index.SimpleIndexEntry;
import com.ispengya.file.store.RecordStore;

/**
 * 门面内部的 CQ 查询视图:逻辑位点 → ConsumeQueue entry → CommitLog 回表
 */
final class CqLogicalIndex<T> implements LogicalIndex<T> {

    private final SimpleConsumeQueue consumeQueue;
    private final RecordStore<T> recordStore;

    CqLogicalIndex(SimpleConsumeQueue consumeQueue, RecordStore<T> recordStore) {
        this.consumeQueue = consumeQueue;
        this.recordStore = recordStore;
    }

    @Override
    public T getByLogicalIndex(long logicalIndex) {
        SimpleIndexEntry entry = consumeQueue.get(logicalIndex);
        if (entry == null) {
            return null;
        }
        return recordStore.read(entry.getPhysicalOffset());
    }

    @Override
    public long logicalCount() {
        return consumeQueue.count();
    }
}
