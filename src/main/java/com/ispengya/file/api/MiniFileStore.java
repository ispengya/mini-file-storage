package com.ispengya.file.api;

import com.ispengya.file.codec.Codec;
import com.ispengya.file.core.MmapSequentialLog;
import com.ispengya.file.core.SequentialLog;
import com.ispengya.file.core.SequentialLogConfig;
import com.ispengya.file.index.ConsumeQueueIndex;
import com.ispengya.file.index.KeyHash;
import com.ispengya.file.index.KeyIndexFile;
import com.ispengya.file.index.KeyIndexPlugin;
import com.ispengya.file.index.RecordIndex;
import com.ispengya.file.index.SimpleConsumeQueue;
import com.ispengya.file.index.SimpleIndexEntry;
import com.ispengya.file.store.IndexHost;
import com.ispengya.file.store.RecordStore;
import com.ispengya.file.store.StoreCheckpoint;
import com.ispengya.file.store.StoreMetadata;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link FileStore} 的默认实现:CommitLog 同步落盘 + 可选索引插件异步构建。
 *
 * <p>内核只对 CommitLog 负责;ConsumeQueue / KeyIndex 是内置的 {@link RecordIndex} 插件,
 * 由使用方按需装配:</p>
 * <ul>
 *   <li>{@code new MiniFileStore(config, codec)}:兼容默认,启用 CQ + KeyIndex;</li>
 *   <li>{@code MiniFileStore.builder(config, codec).build()}:纯 WAL,不建任何索引;</li>
 *   <li>{@code .withConsumeQueue()} / {@code .withKeyIndex()}:只启用需要的内置索引;</li>
 *   <li>{@code .withIndex(custom)}:接入自定义派生索引(如内存 KV、Topic 索引)。</li>
 * </ul>
 *
 * <p>写路径:</p>
 * <ol>
 *   <li>put 只写 CommitLog(唯一真相源),组帧盖 storeTimestamp/meta0/meta1;</li>
 *   <li>{@link IndexHost} 驱动所有索引插件按帧回放;</li>
 *   <li>SYNC 模式 put 等待所有启用索引覆盖本帧;无索引时立即返回;
 *       ASYNC 模式立即返回,读侧用 {@link FileStore#awaitIndexed} 显式升级一致性。</li>
 * </ol>
 *
 * <p>启动生命周期:</p>
 * <ol>
 *   <li>抢占 baseDir/.store.lock(进程独占,双开直接拒绝);</li>
 *   <li>校验 store.properties 与当前配置一致,不一致直接拒绝(防止 setLength 静默截断);</li>
 *   <li>CommitLog 按<b>全局前缀协议</b>恢复:文件必须从 0 连续、封存文件残尾必须全零,
 *       损坏/缺号即整体截断,后续文件登记 {@code data/.quarantined};</li>
 *   <li>只构造启用的索引;每个索引从自己的游标开始回放(新索引返回 0 全量重建);</li>
 *   <li>启动后台索引线程(无索引则不启动)。</li>
 * </ol>
 *
 * <p>关闭顺序:停索引线程并收尾回放 → 数据/索引/水位依次落盘 → 关索引 → 关日志 → 释放锁。</p>
 *
 * <p>锁层级:门面 monitor(put/get 等) → IndexHost monitor(await/signal) → 日志 monitor(append/read)。
 * 单向嵌套,索引线程从不反向获取门面锁,无死锁路径。</p>
 *
 * @param <T> 业务记录类型
 */
public class MiniFileStore<T> implements FileStore<T> {

    /**
     * SYNC 模式 put 等待索引的超时
     */
    private static final long SYNC_AWAIT_TIMEOUT_MILLIS = 5_000L;

    /**
     * close 时等待索引线程退出的超时
     */
    private static final long INDEX_JOIN_TIMEOUT_MILLIS = 5_000L;

    private final StoreConfig config;
    private final FileChannel lockChannel;
    private final FileLock storeLock;
    private final SequentialLog log;
    private final RecordStore<T> recordStore;
    private final StoreCheckpoint checkpoint;
    private final IndexHost indexHost;
    private final List<RecordIndex> indexes;
    private final ConsumeQueueIndex consumeQueueIndex;
    private final KeyIndexPlugin keyIndexPlugin;
    private final LogicalIndex<T> logicalIndex;
    private final KeyIndexView<T> keyIndexView;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * 兼容默认构造:CQ + KeyIndex 全部启用(与重构前行为一致)
     */
    public MiniFileStore(StoreConfig config, Codec<T> codec) {
        this(builder(config, codec).withDefaultIndexes());
    }

    private MiniFileStore(Builder<T> builder) {
        this.config = builder.config;
        Codec<T> codec = builder.codec;

        // 进程级单实例互斥:先抢 .store.lock,失败立即拒绝,绝不带着双写风险继续装配
        String lockPath = config.getBaseDir() + "/.store.lock";
        mkdirs(config.getBaseDir());
        FileChannel channel = null;
        try {
            channel = FileChannel.open(Paths.get(lockPath),
                    StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            FileLock lock = channel.tryLock();
            if (lock == null) {
                closeQuietly(channel);
                throw new StoreLockHeldException(lockPath);
            }
            this.lockChannel = channel;
            this.storeLock = lock;
        } catch (OverlappingFileLockException e) {
            closeQuietly(channel);
            throw new StoreLockHeldException(lockPath);
        } catch (IOException e) {
            closeQuietly(channel);
            throw new IllegalStateException("open store lock failed: " + lockPath, e);
        }

        try {
            // 索引名必须在打开任何资源前校验,避免重名配置泄漏文件句柄
            checkIndexNames(builder.consumeQueue, builder.keyIndex, builder.customIndexes);

            // 只校验本次真正启用的索引尺寸;未启用的索引目录不参与布局校验
            StoreMetadata.verifyOrCreate(config, builder.consumeQueue, builder.keyIndex);

            mkdirs(config.commitLogPath());

            this.log = new MmapSequentialLog(new SequentialLogConfig(
                    config.commitLogPath(), config.getCommitLogFileSize(),
                    config.isSyncFlush(), config.getMaxBodySize()));
            this.recordStore = new RecordStore<>(log, codec, null, null, config.getMaxBodySize());
            this.checkpoint = new StoreCheckpoint(config.checkpointPath());

            List<RecordIndex> active = new ArrayList<>();
            ConsumeQueueIndex cqIndex = null;
            KeyIndexPlugin keyPlugin = null;
            LogicalIndex<T> logicalView = null;
            KeyIndexView<T> keyView = null;

            // 顺序保持"先 KeyIndex、后 ConsumeQueue":KeyIndex 容忍重复
            if (builder.keyIndex) {
                mkdirs(config.keyIndexPath());
                KeyIndexFile keyIndexFile;
                try {
                    keyIndexFile = new KeyIndexFile(config.keyIndexPath(), config.getKeyIndexFileSize(), true);
                } catch (IOException e) {
                    throw new IllegalStateException("recover key index failed: " + config.keyIndexPath(), e);
                }
                keyPlugin = new KeyIndexPlugin(keyIndexFile,
                        new File(config.keyIndexPath(), "cursor"));
                keyView = new KeyLookup<>(keyPlugin, recordStore, log::getMaxOffset);
                active.add(keyPlugin);
            }
            if (builder.consumeQueue) {
                mkdirs(config.consumeQueuePath());
                SimpleConsumeQueue consumeQueue;
                try {
                    consumeQueue = new SimpleConsumeQueue(
                            config.consumeQueuePath(), config.getConsumeQueueFileSize(), true);
                } catch (IOException e) {
                    throw new IllegalStateException("recover consume queue failed: " + config.consumeQueuePath(), e);
                }
                cqIndex = new ConsumeQueueIndex(consumeQueue);
                logicalView = new CqLogicalIndex<>(consumeQueue, recordStore);
                active.add(cqIndex);
            }
            active.addAll(builder.customIndexes);

            this.indexes = active;
            this.consumeQueueIndex = cqIndex;
            this.keyIndexPlugin = keyPlugin;
            this.logicalIndex = logicalView;
            this.keyIndexView = keyView;
            this.indexHost = new IndexHost(log, active, config.getMaxBodySize());

            initializeIndexes();
            if (!active.isEmpty()) {
                this.indexHost.startWorker();
            }
        } catch (RuntimeException | Error e) {
            releaseLock();
            throw e;
        }
    }

    /**
     * 可配置门面构造器。默认不启用任何索引(纯 WAL)。
     *
     * @param <T> 业务记录类型
     */
    public static <T> Builder<T> builder(StoreConfig config, Codec<T> codec) {
        return new Builder<>(config, codec);
    }

    /**
     * 启动自愈:CommitLog 全局前缀恢复后,让每个索引从自己的游标续传/全量重建。
     */
    private void initializeIndexes() {
        long dataTruth = log.getMaxOffset();
        indexHost.init(dataTruth);
        indexHost.replayTo(dataTruth);
    }

    private static void checkIndexNames(boolean consumeQueue, boolean keyIndex,
                                        List<RecordIndex> customIndexes) {
        Set<String> names = new HashSet<>();
        if (consumeQueue) {
            names.add("consumequeue");
        }
        if (keyIndex) {
            names.add("keyindex");
        }
        for (RecordIndex index : customIndexes) {
            String name = index.name();
            if (name == null || name.trim().isEmpty()) {
                throw new IllegalArgumentException("record index name must not be blank");
            }
            if (!names.add(name)) {
                throw new IllegalArgumentException("duplicate record index name: " + name);
            }
        }
    }

    @Override
    public synchronized long put(T value) {
        return putInternal(value, System.currentTimeMillis(), 0L);
    }

    @Override
    public synchronized long put(T value, long tagCode) {
        return putInternal(value, tagCode, 0L);
    }

    @Override
    public synchronized long put(T value, String key) {
        checkKey(key);
        return putInternal(value, System.currentTimeMillis(), KeyHash.of(key));
    }

    private long putInternal(T value, long tagCode, long keyHash) {
        if (closed.get()) {
            throw new IllegalStateException("store already closed");
        }
        long start = recordStore.append(value, tagCode, keyHash);
        indexHost.signal();
        if (config.getIndexMode() == StoreConfig.IndexMode.SYNC
                && !indexHost.awaitCovered(start, SYNC_AWAIT_TIMEOUT_MILLIS)) {
            String error = indexHost.getLastError();
            throw new IllegalStateException(error != null
                    ? "index host stopped on error: " + error
                    : "index did not catch up within " + SYNC_AWAIT_TIMEOUT_MILLIS + "ms");
        }
        return start;
    }

    @Override
    public synchronized T get(long physicalOffset) {
        return recordStore.read(physicalOffset);
    }

    @Override
    public synchronized T getByLogicalIndex(long logicalIndex) {
        if (this.logicalIndex == null) {
            throw new IllegalStateException("consume queue index is not enabled; "
                    + "open with MiniFileStore.builder(...).withConsumeQueue()");
        }
        return this.logicalIndex.getByLogicalIndex(logicalIndex);
    }

    @Override
    public synchronized List<T> getByKey(String key) {
        checkKey(key);
        if (keyIndexView == null) {
            throw new IllegalStateException("key index is not enabled; "
                    + "open with MiniFileStore.builder(...).withKeyIndex()");
        }
        return keyIndexView.getByKey(key);
    }

    @Override
    public synchronized List<T> queryByTagRange(long beginTag, long endTag) {
        SimpleConsumeQueue consumeQueue = requireConsumeQueue();
        List<T> result = new ArrayList<>();
        long logicalOffset = 0;
        while (true) {
            SimpleIndexEntry entry = consumeQueue.get(logicalOffset);
            if (entry == null) {
                break;
            }
            long tag = entry.getTagCode();
            if (tag < beginTag) {
                logicalOffset++;
                continue;
            }
            if (tag > endTag) {
                break;
            }
            result.add(recordStore.read(entry.getPhysicalOffset()));
            logicalOffset++;
        }
        return result;
    }

    /**
     * 逻辑位点查询能力;未启用 ConsumeQueue 时返回空
     */
    public Optional<LogicalIndex<T>> logical() {
        return Optional.ofNullable(logicalIndex);
    }

    /**
     * 业务 key 查询能力;未启用 KeyIndex 时返回空
     */
    public Optional<KeyIndexView<T>> keys() {
        return Optional.ofNullable(keyIndexView);
    }

    /**
     * 按能力类型查找已启用的索引/视图(内置查询视图或自定义插件)
     */
    public <C> Optional<C> index(Class<C> type) {
        if (type.isInstance(logicalIndex)) {
            return Optional.of(type.cast(logicalIndex));
        }
        if (type.isInstance(keyIndexView)) {
            return Optional.of(type.cast(keyIndexView));
        }
        for (RecordIndex index : indexes) {
            if (type.isInstance(index)) {
                return Optional.of(type.cast(index));
            }
        }
        return Optional.empty();
    }

    @Override
    public boolean awaitIndexed(long offset, long timeoutMillis) {
        return indexHost.awaitCovered(offset, timeoutMillis);
    }

    @Override
    public long maxIndexedOffset() {
        return indexHost.minCursor();
    }

    @Override
    public synchronized void flush() {
        if (closed.get()) {
            throw new IllegalStateException("store already closed");
        }
        flushAll();
    }

    /**
     * 有序落盘协议:数据 → 索引 → 水位。任何一步之间崩溃,checkpoint 只会滞后不会超前。
     */
    private void flushAll() {
        long logMax = recordStore.flush();
        for (RecordIndex index : indexes) {
            index.flush();
        }
        long cqMax = consumeQueueIndex == null ? -1L : consumeQueueIndex.getConsumeQueue().flush();
        long cqCursor = indexHost.getCursor(consumeQueueIndex);
        long keyCursor = indexHost.getCursor(keyIndexPlugin);
        checkpoint.save(logMax, cqMax, cqCursor, keyCursor);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            indexHost.stopAndDrain(INDEX_JOIN_TIMEOUT_MILLIS);
        } catch (RuntimeException | Error ignored) {
            // 收尾失败不阻断资源关闭
        }
        try {
            flushAll();
        } catch (RuntimeException | Error ignored) {
            // 同上;已关闭状态下的落盘失败不应导致锁泄漏
        }
        for (RecordIndex index : indexes) {
            try {
                index.close();
            } catch (RuntimeException | Error ignored) {
                // 单个索引关闭失败不阻断其它组件
            }
        }
        try {
            recordStore.close();
        } finally {
            // 组件全部落盘关闭后再释放进程锁,避免半关闭状态下被新实例接管
            releaseLock();
        }
    }

    private SimpleConsumeQueue requireConsumeQueue() {
        if (consumeQueueIndex == null) {
            throw new IllegalStateException("consume queue index is not enabled; "
                    + "open with MiniFileStore.builder(...).withConsumeQueue()");
        }
        return consumeQueueIndex.getConsumeQueue();
    }

    /**
     * 业务配置,便于诊断
     */
    public StoreConfig getConfig() {
        return config;
    }

    /**
     * 本次打开时 CommitLog 全局前缀截断的终点物理位点。
     * 非 -1 表示启动时发现损坏/缺号,断点之后的数据文件已登记进 {@code data/.quarantined},
     * 不再参与恢复。
     *
     * @return 截断终点;-1 表示日志完整连续
     */
    public long getRecoveryTruncatedOffset() {
        return log.getRecoveryTruncatedOffset();
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

    /**
     * 释放文件锁与锁通道;锁文件本身保留(删除存在竞态,与 RocketMQ 同款处理)
     */
    private void releaseLock() {
        try {
            if (storeLock != null && storeLock.isValid()) {
                storeLock.release();
            }
        } catch (IOException ignored) {
        }
        closeQuietly(lockChannel);
    }

    private static void closeQuietly(FileChannel channel) {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException ignored) {
            }
        }
    }

    /**
     * 可选索引装配器。默认不启用任何索引;自定义索引实例由调用方持有,
     * 生命周期(flush/close)交给存储统一管理。
     *
     * @param <T> 业务记录类型
     */
    public static final class Builder<T> {

        private final StoreConfig config;
        private final Codec<T> codec;
        private final List<RecordIndex> customIndexes = new ArrayList<>();
        private boolean consumeQueue;
        private boolean keyIndex;

        private Builder(StoreConfig config, Codec<T> codec) {
            if (config == null) {
                throw new IllegalArgumentException("config must not be null");
            }
            if (codec == null) {
                throw new IllegalArgumentException("codec must not be null");
            }
            this.config = config;
            this.codec = codec;
        }

        /**
         * 启用内置 ConsumeQueue(逻辑位点 / tag 范围查询)
         */
        public Builder<T> withConsumeQueue() {
            this.consumeQueue = true;
            return this;
        }

        /**
         * 启用内置 KeyIndex(业务 key 查询)
         */
        public Builder<T> withKeyIndex() {
            this.keyIndex = true;
            return this;
        }

        /**
         * 启用全部内置索引(CQ + KeyIndex)
         */
        public Builder<T> withDefaultIndexes() {
            this.consumeQueue = true;
            this.keyIndex = true;
            return this;
        }

        /**
         * 接入自定义派生索引;启动时按 replayFrom 全量/增量回放
         */
        public Builder<T> withIndex(RecordIndex index) {
            if (index == null) {
                throw new IllegalArgumentException("index must not be null");
            }
            this.customIndexes.add(index);
            return this;
        }

        /**
         * 打开存储
         */
        public MiniFileStore<T> build() {
            return new MiniFileStore<>(this);
        }

        /**
         * {@link #build()} 的别名
         */
        public MiniFileStore<T> open() {
            return build();
        }
    }
}
