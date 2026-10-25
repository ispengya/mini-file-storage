package com.ispengya.file.store;

import com.ispengya.file.core.SequentialLog;
import com.ispengya.file.index.ConsumeQueueIndex;
import com.ispengya.file.index.KeyIndexFile;
import com.ispengya.file.index.KeyIndexPlugin;
import com.ispengya.file.index.SimpleConsumeQueue;

import java.util.Arrays;

/**
 * 兼容门面:把内建 ConsumeQueue + KeyIndex 装配成 {@link IndexHost} 的两个索引插件。
 *
 * <p>真正的回放逻辑已下沉到 {@link IndexHost}:按 CommitLog 顺序驱动一组
 * {@link com.ispengya.file.index.RecordIndex}。本类保留原有 API,供手工装配
 * (单元测试/旧调用)使用;新代码应优先通过 {@code MiniFileStore.builder(...)}
 * 选择索引。</p>
 *
 * <p>插件顺序保持"先 KeyIndex、后 ConsumeQueue":KeyIndex 容忍重复,
 * 崩溃最多多一条重复;反向顺序会留下 CQ 时钟已越过、key 永久缺失的缺口。</p>
 */
public class ReputService {

    private final IndexHost host;
    private final ConsumeQueueIndex consumeQueueIndex;
    private final KeyIndexPlugin keyIndexPlugin;

    public ReputService(SequentialLog log, SimpleConsumeQueue consumeQueue,
                        KeyIndexFile keyIndexFile, int maxBodySize) {
        this.consumeQueueIndex = new ConsumeQueueIndex(consumeQueue);
        // 兼容路径不持久化 key 游标,由调用方通过 initCursors 显式指定
        this.keyIndexPlugin = new KeyIndexPlugin(keyIndexFile, null);
        this.host = new IndexHost(log, Arrays.asList(keyIndexPlugin, consumeQueueIndex), maxBodySize);
    }

    /**
     * 显式初始化两个内建索引的游标(兼容旧行为)
     *
     * @param cqCursorFromQueue ConsumeQueue 自时钟
     * @param keyCursorHint      key 游标提示;负数表示与 CQ 对齐
     */
    public void initCursors(long cqCursorFromQueue, long keyCursorHint) {
        long keyCursor = keyCursorHint >= 0 ? keyCursorHint : cqCursorFromQueue;
        host.setCursors(keyCursor, cqCursorFromQueue);
    }

    public long replayTo(long target) {
        return host.replayTo(target);
    }

    public void signal() {
        host.signal();
    }

    public boolean awaitCovered(long recordStartOffset, long timeoutMillis) {
        return host.awaitCovered(recordStartOffset, timeoutMillis);
    }

    public void startWorker() {
        host.startWorker();
    }

    public void stopAndDrain(long joinTimeoutMillis) {
        host.stopAndDrain(joinTimeoutMillis);
    }

    public long getCqCursor() {
        return host.getCursor(consumeQueueIndex);
    }

    public long getKeyCursor() {
        return host.getCursor(keyIndexPlugin);
    }

    public String getLastError() {
        return host.getLastError();
    }

    /**
     * 暴露宿主,便于诊断/扩展
     */
    public IndexHost getHost() {
        return host;
    }
}
