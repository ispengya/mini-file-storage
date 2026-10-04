package com.ispengya.file.store;

import com.ispengya.file.core.RecordFrame;
import com.ispengya.file.core.SequentialLog;
import com.ispengya.file.index.KeyIndexFile;
import com.ispengya.file.index.SimpleConsumeQueue;

import java.nio.ByteBuffer;

/**
 * 索引回放服务(Reput):后台单线程追尾 CommitLog 帧,按帧头信息构建 ConsumeQueue 与 KeyIndex。
 *
 * <p>架构定位:CommitLog 是唯一真相源,两份索引都是"可弃的派生视图"。
 * 本服务把索引构建从 put 写路径摘除,写路径只落日志;错位不再可能——
 * 索引"落后于数据"从故障变回设计允许的状态。</p>
 *
 * <p><b>每帧写入顺序(正确性设计,勿改):</b></p>
 * <pre>
 * ① keyHash != 0 → keyIndex.put     先写"容忍重复"的
 * ② cq.append                       后写"精确自时钟"的
 * ③ 游标推进 + notifyAll             cqCursor 因此蕴含 key 已完成——await 只需等它
 * </pre>
 * 崩溃只能切在相邻两步之间:若 CQ 先写而 key 后写,断点会留下"CQ 时钟已越过、
 * KeyIndex 永久缺失"的缺口(重放从 CQ 时钟续,不会再补);本顺序下任何断点
 * 最坏只是 keyindex 多一条重复,读侧去重兜底。
 *
 * <p>锁纪律:本类只持自身 monitor,{@code await} 的调用方(门面)持门面锁——
 * 两把锁从不嵌套获取,不存在死锁路径。游标推进后日志侧的可见性由
 * {@code SequentialLog} 内部同步保证,回放读帧与追加同在日志锁上串行。</p>
 */
public class ReputService {

    private final SequentialLog log;
    private final SimpleConsumeQueue consumeQueue;
    private final KeyIndexFile keyIndexFile;
    private final int maxBodySize;

    /**
     * 自身 monitor:保护游标与等待队列
     */
    private final Object lock = new Object();

    /**
     * 已构建索引覆盖到的 CommitLog 字节位点(CQ 自时钟,await 谓词)
     */
    private volatile long cqCursor;

    /**
     * KeyIndex 已构建覆盖位点(推进与 cqCursor 同步,保留供 checkpoint 观测)
     */
    private volatile long keyCursor;

    /**
     * 帧读取错误停点信息;非 null 表示回放因损坏停止
     */
    private volatile String lastError;

    private volatile boolean stopRequested;

    private Thread worker;

    public ReputService(SequentialLog log, SimpleConsumeQueue consumeQueue,
                        KeyIndexFile keyIndexFile, int maxBodySize) {
        this.log = log;
        this.consumeQueue = consumeQueue;
        this.keyIndexFile = keyIndexFile;
        this.maxBodySize = maxBodySize;
    }

    /**
     * 初始化游标(启动时由门面在交叉校验截断之后调用):
     * cqCursor 以 CQ 文件自时钟为准;keyCursor 用 checkpoint 提示(无提示则对齐 cqCursor,
     * 表示"从日志现状视为已同步",新写入按顺序重建;全量重建 keyindex 由门面显式回放覆盖)
     */
    public void initCursors(long cqCursorFromQueue, long keyCursorHint) {
        synchronized (lock) {
            this.cqCursor = cqCursorFromQueue;
            this.keyCursor = keyCursorHint >= 0 ? keyCursorHint : cqCursorFromQueue;
        }
    }

    /**
     * 同步回放到 target(启动 catch-up / 测试驱动 / stopAndDrain 收尾)
     *
     * @return 实际到达的位点(遇错停在中途)
     */
    public long replayTo(long target) {
        synchronized (lock) {
            while (cqCursor < target && lastError == null) {
                long before = cqCursor;
                replayNextFrameLocked();
                if (cqCursor == before) {
                    break; // 无可读完整帧或出错,不再空转
                }
            }
            return cqCursor;
        }
    }

    /**
     * 追加了数据后调用:唤醒后台线程
     */
    public void signal() {
        synchronized (lock) {
            lock.notifyAll();
        }
    }

    /**
     * 等待索引完整覆盖"起始位置为 recordStartOffset 的记录"。
     * 游标按整帧推进,故 cqCursor > recordStartOffset 恰好等价于该帧完成——
     * 且因 key→cq→推进 的顺序,蕴含同帧的 key 单元也已先写入。await 只需盯 cqCursor 一个谓词。
     *
     * @return true 已覆盖;false 超时或回放因帧损坏停止
     */
    public boolean awaitCovered(long recordStartOffset, long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        synchronized (lock) {
            while (cqCursor <= recordStartOffset && lastError == null) {
                long remain = deadline - System.currentTimeMillis();
                if (remain <= 0) {
                    return false;
                }
                try {
                    lock.wait(Math.min(remain, 100L));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return cqCursor > recordStartOffset;
        }
    }

    /**
     * 启动后台追尾线程(daemon)
     */
    public void startWorker() {
        if (worker != null) {
            throw new IllegalStateException("reput worker already started");
        }
        worker = new Thread(this::workerLoop, "mini-reput");
        worker.setDaemon(true);
        worker.start();
    }

    /**
     * 停线程并在调用线程收尾回放(保证返回后无在途索引写)
     */
    public void stopAndDrain(long joinTimeoutMillis) {
        stopRequested = true;
        signal();
        if (worker != null) {
            try {
                worker.join(joinTimeoutMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        replayTo(log.getMaxOffset());
    }

    public long getCqCursor() {
        return cqCursor;
    }

    public long getKeyCursor() {
        return keyCursor;
    }

    public String getLastError() {
        return lastError;
    }

    /**
     * 后台主循环:有尾追尾,无尾短等。异常不外溢——错误已记入 lastError
     */
    private void workerLoop() {
        while (!stopRequested) {
            try {
                long replayed;
                do {
                    replayed = replayTo(log.getMaxOffset());
                } while (replayed < log.getMaxOffset() && lastError == null && !stopRequested);
            } catch (RuntimeException e) {
                lastError = e.getMessage();
            }
            synchronized (lock) {
                if (stopRequested) {
                    return;
                }
                try {
                    lock.wait(200L); // 超时兜底,防 signal 丢失
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    /**
     * 回放 cqCursor 处的一帧。必须在持有 lock 时调用。
     */
    private void replayNextFrameLocked() {
        byte[] header;
        try {
            header = log.read(cqCursor, RecordFrame.HEADER_SIZE);
        } catch (IllegalArgumentException e) {
            return; // 越过合法写位置:数据还没到,等下一次
        }
        int bodyLen = ByteBuffer.wrap(header).getInt(4);
        int total = RecordFrame.HEADER_SIZE + bodyLen;
        if (bodyLen < 0 || total > maxBodySize + RecordFrame.HEADER_SIZE || cqCursor + total > log.getMaxOffset()) {
            lastError = "invalid frame length " + bodyLen + " at reput cursor " + cqCursor;
            return;
        }
        byte[] frame = log.read(cqCursor, total);
        RecordFrame.Frame parsed = RecordFrame.parse(ByteBuffer.wrap(frame), 0, total, maxBodySize);
        if (parsed.getStatus() != RecordFrame.Status.OK) {
            // 合法区内坏帧 = 外部损坏,停在这里绝不越过(与恢复协议同一信条)
            lastError = "frame not replayable at offset " + cqCursor + ": " + parsed.getStatus();
            return;
        }

        // ① 先 key(容忍重复) ② 后 cq(精确自时钟) —— 顺序是正确性的一部分
        if (parsed.getKeyHash() != 0L && cqCursor >= keyCursor) {
            keyIndexFile.put(parsed.getKeyHash(), cqCursor);
        }
        consumeQueue.append(cqCursor, total, parsed.getTagCode());
        cqCursor += total;
        keyCursor = cqCursor;
        lock.notifyAll();
    }
}
