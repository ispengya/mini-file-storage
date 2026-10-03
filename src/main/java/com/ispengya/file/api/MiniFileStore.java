package com.ispengya.file.api;

import com.ispengya.file.codec.Codec;
import com.ispengya.file.core.MmapSequentialLog;
import com.ispengya.file.core.SequentialLogConfig;
import com.ispengya.file.index.KeyIndexFile;
import com.ispengya.file.index.SimpleConsumeQueue;
import com.ispengya.file.store.RecordStore;
import com.ispengya.file.store.RecordStore.StoreCheckpoint;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link FileStore} 的默认实现:对内部组件的一次性装配与封装。
 *
 * <p>构造顺序与生命周期(启动恢复 → 运行 → 关闭)对齐 RecordStore 的设计:</p>
 *
 * <ol>
 *   <li>CommitLog:{@link MmapSequentialLog} 构造时按 [length][payload] 扫描恢复写位置</li>
 *   <li>ConsumeQueue:按 20 字节单元扫描恢复索引尾部,新索引在有效区后追加</li>
 *   <li>KeyIndex:按 16 字节单元扫描恢复追加位置(读到全零空洞即停止)</li>
 *   <li>Checkpoint:记录 CommitLog / ConsumeQueue 最近一次刷盘偏移</li>
 * </ol>
 *
 * @param <T> 业务记录类型
 */
public class MiniFileStore<T> implements FileStore<T> {

    private final StoreConfig config;
    private final RecordStore<T> recordStore;
    private final SimpleConsumeQueue consumeQueue;
    private final KeyIndexFile keyIndexFile;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public MiniFileStore(StoreConfig config, Codec<T> codec) {
        this.config = config;
        mkdirs(config.commitLogPath());
        mkdirs(config.consumeQueuePath());
        mkdirs(config.keyIndexPath());

        MmapSequentialLog log = new MmapSequentialLog(
                new SequentialLogConfig(config.commitLogPath(), config.getCommitLogFileSize(), config.isSyncFlush()));

        SimpleConsumeQueue cq;
        try {
            cq = new SimpleConsumeQueue(config.consumeQueuePath(), config.getConsumeQueueFileSize(), true);
        } catch (IOException e) {
            throw new IllegalStateException("recover consume queue failed: " + config.consumeQueuePath(), e);
        }

        KeyIndexFile keyIndex;
        try {
            keyIndex = new KeyIndexFile(config.keyIndexPath(), config.getKeyIndexFileSize(), true);
        } catch (IOException e) {
            throw new IllegalStateException("recover key index failed: " + config.keyIndexPath(), e);
        }
        StoreCheckpoint checkpoint = new StoreCheckpoint(config.checkpointPath());

        this.consumeQueue = cq;
        this.keyIndexFile = keyIndex;
        this.recordStore = new RecordStore<>(log, codec, cq, checkpoint);
    }

    @Override
    public synchronized long put(T value) {
        return recordStore.append(value, System.currentTimeMillis());
    }

    @Override
    public synchronized long put(T value, long tagCode) {
        return recordStore.append(value, tagCode);
    }

    @Override
    public synchronized long put(T value, String key) {
        checkKey(key);
        long physicalOffset = recordStore.append(value, System.currentTimeMillis());
        keyIndexFile.put(hashKey(key), physicalOffset);
        return physicalOffset;
    }

    @Override
    public synchronized T get(long physicalOffset) {
        return recordStore.read(physicalOffset);
    }

    @Override
    public synchronized T getByLogicalIndex(long logicalIndex) {
        return recordStore.readByLogicalOffset(logicalIndex);
    }

    @Override
    public synchronized List<T> getByKey(String key) {
        checkKey(key);
        List<Long> offsets = keyIndexFile.get(hashKey(key));
        List<T> result = new ArrayList<>(offsets.size());
        for (Long offset : offsets) {
            T value = recordStore.read(offset);
            if (value != null) {
                result.add(value);
            }
        }
        return result;
    }

    @Override
    public synchronized List<T> queryByTagRange(long beginTag, long endTag) {
        return recordStore.queryByTimeRange(beginTag, endTag);
    }

    @Override
    public synchronized void flush() {
        if (closed.get()) {
            throw new IllegalStateException("store already closed");
        }
        recordStore.flush();
        keyIndexFile.flush();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            keyIndexFile.close();
            recordStore.close();
        }
    }

    /**
     * 业务配置,便于诊断
     */
    public StoreConfig getConfig() {
        return config;
    }

    /**
     * 与 FileDemoTest 保持同一约定:key 的 hash 取 32 位无符号扩展,
     * 与 KeyIndex 中 8 字节 keyHash 字段的存储方式一致。
     */
    static long hashKey(String key) {
        return key.hashCode() & 0xffffffffL;
    }

    private static void checkKey(String key) {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
    }

    private static void mkdirs(String path) {
        File dir = new File(path);
        if (!dir.exists() && !dir.mkdirs() && !dir.exists()) {
            throw new IllegalStateException("cannot create directory: " + path);
        }
    }
}
