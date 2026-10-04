package com.ispengya.file.api;

import com.ispengya.file.codec.Codec;
import com.ispengya.file.core.MmapSequentialLog;
import com.ispengya.file.core.SequentialLog;
import com.ispengya.file.core.SequentialLogConfig;
import com.ispengya.file.index.KeyIndexFile;
import com.ispengya.file.index.SimpleConsumeQueue;
import com.ispengya.file.store.RecordStore;
import com.ispengya.file.store.ReputService;
import com.ispengya.file.store.StoreCheckpoint;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link FileStore} 的默认实现:CommitLog 同步落盘 + Reput 异步建索引的门面装配。
 *
 * <p>写路径(与旧同步双写模型的本质区别):</p>
 * <ol>
 *   <li>put 只写 CommitLog(唯一真相源),组帧盖 storeTimestamp/tagCode/keyHash;</li>
 *   <li>Reput 后台线程按帧头回放 ConsumeQueue 与 KeyIndex(派生视图);</li>
 *   <li>SYNC 模式 put 等待本帧索引完成(默认,行为等同旧语义);
 *       ASYNC 模式立即返回,读侧用 {@link FileStore#awaitIndexed} 显式升级一致性。</li>
 * </ol>
 *
 * <p>启动生命周期:</p>
 * <ol>
 *   <li>抢占 baseDir/.store.lock(进程独占,双开直接拒绝);</li>
 *   <li>CommitLog 按帧协议扫描恢复,CQ/KeyIndex 扫描恢复追加位置;</li>
 *   <li>交叉校验:CQ 中"指向未落盘数据"的撒谎条目从尾部截断;</li>
 *   <li>游标初始化(CQ 自时钟 + checkpoint 的 key 提示,取保守值);</li>
 *   <li>同步 catch-up 回放——冷启动补索引、崩溃自愈、删目录重建是同一段代码;</li>
 *   <li>启动后台 Reput 线程。</li>
 * </ol>
 *
 * <p>关闭顺序:停 Reput 并收尾回放 → 数据/索引/水位依次落盘 → 关组件 → 释放锁。
 * 不变式 checkpoint ≤ 索引 ≤ 数据由该顺序保证,水位永远"滞后但真实"。</p>
 *
 * <p>锁层级:门面 monitor(put/get 等) → Reput monitor(await/signal) → 日志 monitor(append/read)。
 * 单向嵌套,Reput 线程从不反向获取门面锁,无死锁路径。</p>
 *
 * @param <T> 业务记录类型
 */
public class MiniFileStore<T> implements FileStore<T> {

    /**
     * SYNC 模式 put 等待索引的超时
     */
    private static final long SYNC_AWAIT_TIMEOUT_MILLIS = 5_000L;

    /**
     * close 时等待 Reput 线程退出的超时
     */
    private static final long REPUT_JOIN_TIMEOUT_MILLIS = 5_000L;

    private final StoreConfig config;
    private final FileChannel lockChannel;
    private final FileLock storeLock;
    private final SequentialLog log;
    private final RecordStore<T> recordStore;
    private final SimpleConsumeQueue consumeQueue;
    private final KeyIndexFile keyIndexFile;
    private final StoreCheckpoint checkpoint;
    private final ReputService reput;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public MiniFileStore(StoreConfig config, Codec<T> codec) {
        this.config = config;

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
            mkdirs(config.commitLogPath());
            mkdirs(config.consumeQueuePath());
            mkdirs(config.keyIndexPath());

            this.log = new MmapSequentialLog(
                    new SequentialLogConfig(config.commitLogPath(), config.getCommitLogFileSize(), config.isSyncFlush()));

            try {
                this.consumeQueue = new SimpleConsumeQueue(
                        config.consumeQueuePath(), config.getConsumeQueueFileSize(), true);
            } catch (IOException e) {
                throw new IllegalStateException("recover consume queue failed: " + config.consumeQueuePath(), e);
            }

            try {
                this.keyIndexFile = new KeyIndexFile(
                        config.keyIndexPath(), config.getKeyIndexFileSize(), true);
            } catch (IOException e) {
                throw new IllegalStateException("recover key index failed: " + config.keyIndexPath(), e);
            }

            this.checkpoint = new StoreCheckpoint(config.checkpointPath());

            // 纯日志模式:索引写路径全部交给 Reput
            this.recordStore = new RecordStore<>(log, codec, null, null, config.getMaxBodySize());
            this.reput = new ReputService(log, consumeQueue, keyIndexFile, config.getMaxBodySize());

            initializeReput();
            this.reput.startWorker();
        } catch (RuntimeException | Error e) {
            releaseLock();
            throw e;
        }
    }

    /**
     * 启动自愈:交叉校验撒谎的 CQ 尾条目 → 保守推导双游标 → 同步追平回放。
     * "冷启动补索引 / 崩溃自愈 / 索引目录被删后全量重建"共用这一段——日志是唯一真相源的直接推论。
     */
    private void initializeReput() {
        long dataTruth = log.getMaxOffset();
        consumeQueue.truncateBeyond(dataTruth);
        long cqCursor = consumeQueue.cqCursor();

        long keyHint = -1L;
        long[] cp = checkpoint.load();
        if (cp != null && cp.length >= 4 && cp[3] >= 0) {
            // 跨设备写序无法保证 cp 页与 cq 页的相对持久顺序,取保守下界,重叠区靠读去重消化
            keyHint = Math.min(cp[3], cqCursor);
        }
        reput.initCursors(cqCursor, keyHint);
        reput.replayTo(dataTruth);
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
        return putInternal(value, System.currentTimeMillis(), hashKey(key));
    }

    private long putInternal(T value, long tagCode, long keyHash) {
        if (closed.get()) {
            throw new IllegalStateException("store already closed");
        }
        long start = recordStore.append(value, tagCode, keyHash);
        reput.signal();
        if (config.getIndexMode() == StoreConfig.IndexMode.SYNC
                && !reput.awaitCovered(start, SYNC_AWAIT_TIMEOUT_MILLIS)) {
            String error = reput.getLastError();
            throw new IllegalStateException(error != null
                    ? "reput stopped on corrupt frame: " + error
                    : "reput did not catch up within " + SYNC_AWAIT_TIMEOUT_MILLIS + "ms");
        }
        return start;
    }

    @Override
    public synchronized T get(long physicalOffset) {
        return recordStore.read(physicalOffset);
    }

    @Override
    public synchronized T getByLogicalIndex(long logicalIndex) {
        return readByLogicalIndex(logicalIndex);
    }

    @Override
    public synchronized List<T> getByKey(String key) {
        checkKey(key);
        // keyindex 语义是"候选集"且允许回放重叠产生重复条目:按 offset 去重后再回表
        LinkedHashSet<Long> uniqueOffsets = new LinkedHashSet<>(keyIndexFile.get(hashKey(key)));
        List<T> result = new ArrayList<>(uniqueOffsets.size());
        for (Long offset : uniqueOffsets) {
            T value = recordStore.read(offset);
            if (value != null) {
                result.add(value);
            }
        }
        return result;
    }

    @Override
    public synchronized List<T> queryByTagRange(long beginTag, long endTag) {
        return queryByTagRangeLocked(beginTag, endTag);
    }

    @Override
    public boolean awaitIndexed(long offset, long timeoutMillis) {
        return reput.awaitCovered(offset, timeoutMillis);
    }

    @Override
    public long maxIndexedOffset() {
        return reput.getCqCursor();
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
        long cqMax = consumeQueue.flush();
        keyIndexFile.flush();
        checkpoint.save(logMax, cqMax, reput.getCqCursor(), reput.getKeyCursor());
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            reput.stopAndDrain(REPUT_JOIN_TIMEOUT_MILLIS);
            flushAll();
            consumeQueue.close();
            keyIndexFile.close();
            recordStore.close();
            // 组件全部落盘关闭后再释放进程锁,避免半关闭状态下被新实例接管
            releaseLock();
        }
    }

    /**
     * 门面锁内执行:读门面持有的组件
     */
    private T readByLogicalIndex(long logicalIndex) {
        SimpleConsumeQueue cq = this.consumeQueue;
        com.ispengya.file.index.SimpleIndexEntry entry = cq.get(logicalIndex);
        if (entry == null) {
            return null;
        }
        return recordStore.read(entry.getPhysicalOffset());
    }

    private List<T> queryByTagRangeLocked(long beginTag, long endTag) {
        List<T> result = new ArrayList<>();
        long logicalOffset = 0;
        while (true) {
            com.ispengya.file.index.SimpleIndexEntry entry = consumeQueue.get(logicalOffset);
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
     * 业务配置,便于诊断
     */
    public StoreConfig getConfig() {
        return config;
    }

    /**
     * 与 FileDemoTest 保持同一约定:key 的 hash 取 32 位无符号扩展,与帧内/KeyIndex 的存储方式一致
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
}
