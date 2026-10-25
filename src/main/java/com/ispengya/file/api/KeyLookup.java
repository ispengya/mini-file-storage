package com.ispengya.file.api;

import com.ispengya.file.index.KeyHash;
import com.ispengya.file.index.KeyIndexPlugin;
import com.ispengya.file.store.RecordStore;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * 门面内部的 KeyIndex 查询视图:hash 候选 → 过滤越界 offset → 去重 → CommitLog 回表
 */
final class KeyLookup<T> implements KeyIndexView<T> {

    private final KeyIndexPlugin keyIndex;
    private final RecordStore<T> recordStore;
    private final LongSupplier logMaxOffset;

    KeyLookup(KeyIndexPlugin keyIndex, RecordStore<T> recordStore, LongSupplier logMaxOffset) {
        this.keyIndex = keyIndex;
        this.recordStore = recordStore;
        this.logMaxOffset = logMaxOffset;
    }

    @Override
    public List<T> getByKey(String key) {
        Objects.requireNonNull(key, "key must not be null");
        long maxOffset = logMaxOffset.getAsLong();
        LinkedHashSet<Long> uniqueOffsets = new LinkedHashSet<>(keyIndex.lookup(KeyHash.of(key)));
        List<T> result = new ArrayList<>(uniqueOffsets.size());
        for (Long offset : uniqueOffsets) {
            // KeyIndex 可能残留全局截断前的陈旧条目(offset >= 数据真相):跳过而不是抛越界
            if (offset >= maxOffset) {
                continue;
            }
            T value = recordStore.read(offset);
            if (value != null) {
                result.add(value);
            }
        }
        return result;
    }
}
