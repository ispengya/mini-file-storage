package com.ispengya.file.store;

import com.ispengya.file.core.RecordFrame;
import com.ispengya.file.core.SequentialLog;
import com.ispengya.file.index.RecordIndex;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 索引宿主:按 CommitLog 顺序驱动一组可插拔索引({@link RecordIndex})。
 *
 * <p>本类是 Reput 模型的泛化:把"固定构建 ConsumeQueue + KeyIndex"改为
 * "遍历一组索引插件,各自维护独立游标"。</p>
 *
 * <p><b>回放算法:</b></p>
 * <ol>
 *   <li>启动时对每个索引调用 {@link RecordIndex#replayFrom(long)} 得到各自起点;</li>
 *   <li>从所有起点的最小值开始扫描 CommitLog,逐帧调用"尚未覆盖该帧"的索引;</li>
 *   <li>每帧完成后推进对应索引的游标;所以"后加索引"只需返回 0,已有索引自然跳过重叠帧。</li>
 * </ol>
 *
 * <p>等待谓词使用所有索引游标的最小值:该值越过某条记录的起点,
 * 才表示"所有启用的索引都已覆盖这条记录"。</p>
 *
 * <p>锁纪律:本类只持自身 monitor;门面在持有门面锁时调用 {@code awaitCovered},
 * 后台线程从不反向获取门面锁,不存在死锁路径。</p>
 */
public class IndexHost {

    private final SequentialLog log;
    private final List<RecordIndex> indexes;
    private final int maxBodySize;

    /**
     * 自身 monitor:保护游标与等待队列
     */
    private final Object lock = new Object();

    /**
     * 各索引已覆盖到的 CommitLog 位点,与 {@link #indexes} 下标一一对应
     */
    private final long[] cursors;

    /**
     * 回放错误停点信息;非 null 表示某个索引或帧导致回放停止
     */
    private volatile String lastError;

    private volatile boolean stopRequested;

    private Thread worker;

    public IndexHost(SequentialLog log, List<RecordIndex> indexes, int maxBodySize) {
        this.log = log;
        this.indexes = new ArrayList<>(indexes);
        this.maxBodySize = maxBodySize;
        this.cursors = new long[this.indexes.size()];
    }

    /**
     * 初始化各索引游标(启动时、CommitLog 全局前缀恢复之后调用)
     *
     * @param logMaxOffset 恢复后的合法日志尾位点
     */
    public void init(long logMaxOffset) {
        synchronized (lock) {
            for (int i = 0; i < indexes.size(); i++) {
                long from = indexes.get(i).replayFrom(logMaxOffset);
                // 游标非法时退化为全量回放,绝不用 clamp 把损坏状态伪装成"已追平"
                cursors[i] = from >= 0L && from <= logMaxOffset ? from : 0L;
            }
        }
    }

    /**
     * 显式覆盖游标(兼容旧 ReputService 的 initCursors,亦可用于测试)
     *
     * @param values 与构造时索引顺序一致
     */
    public void setCursors(long... values) {
        synchronized (lock) {
            if (values.length != cursors.length) {
                throw new IllegalArgumentException("expected " + cursors.length + " cursors, got " + values.length);
            }
            System.arraycopy(values, 0, cursors, 0, values.length);
        }
    }

    public boolean hasIndexes() {
        return !indexes.isEmpty();
    }

    public List<RecordIndex> getIndexes() {
        return new ArrayList<>(indexes);
    }

    /**
     * 同步回放到 target(启动 catch-up / 测试驱动 / stopAndDrain 收尾)
     *
     * @return 所有索引游标的最小值(遇错停在中途)
     */
    public long replayTo(long target) {
        synchronized (lock) {
            if (indexes.isEmpty()) {
                return target;
            }
            while (minCursorLocked() < target && lastError == null) {
                long before = minCursorLocked();
                replayNextFrameLocked(before);
                if (minCursorLocked() == before) {
                    break; // 无可读完整帧或出错,不再空转
                }
            }
            return minCursorLocked();
        }
    }

    /**
     * 等待所有启用索引覆盖"起始位置为 recordStartOffset 的记录"。
     * 游标按整帧推进,故 minCursor > recordStartOffset 等价于所有索引都完成了该帧。
     *
     * @return true 已覆盖;false 超时或回放因错误停止
     */
    public boolean awaitCovered(long recordStartOffset, long timeoutMillis) {
        if (indexes.isEmpty()) {
            return true;
        }
        long deadline = System.currentTimeMillis() + timeoutMillis;
        synchronized (lock) {
            while (minCursorLocked() <= recordStartOffset && lastError == null) {
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
            return minCursorLocked() > recordStartOffset;
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
     * 启动后台追尾线程(daemon)
     */
    public void startWorker() {
        if (worker != null) {
            throw new IllegalStateException("index worker already started");
        }
        worker = new Thread(this::workerLoop, "mini-index");
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

    /**
     * 所有索引游标的最小值;没有索引时返回日志尾位点
     */
    public long minCursor() {
        synchronized (lock) {
            return minCursorLocked();
        }
    }

    public long getCursor(RecordIndex index) {
        synchronized (lock) {
            int position = indexes.indexOf(index);
            return position < 0 ? -1L : cursors[position];
        }
    }

    public long getCursor(int index) {
        synchronized (lock) {
            return index >= 0 && index < cursors.length ? cursors[index] : -1L;
        }
    }

    public String getLastError() {
        return lastError;
    }

    private long minCursorLocked() {
        if (indexes.isEmpty()) {
            return log.getMaxOffset();
        }
        long min = cursors[0];
        for (int i = 1; i < cursors.length; i++) {
            min = Math.min(min, cursors[i]);
        }
        return min;
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
     * 回放 scanCursor 处的一帧。必须在持有 lock 时调用。
     */
    private void replayNextFrameLocked(long scanCursor) {
        byte[] header;
        try {
            header = log.read(scanCursor, RecordFrame.HEADER_SIZE);
        } catch (IllegalArgumentException e) {
            // 帧不跨文件:封存文件尾部可能存在"装不下下一帧"的浪费区。
            // 落后到该区域的所有索引一起跳到下一个文件起点。
            long nextFileStart = log.nextFileStartOffset(scanCursor);
            if (nextFileStart > scanCursor) {
                for (int i = 0; i < cursors.length; i++) {
                    if (cursors[i] < nextFileStart) {
                        cursors[i] = nextFileStart;
                    }
                }
                lock.notifyAll();
            }
            return; // 没有下一个文件:数据还没到,等下一次
        }

        int bodyLen = ByteBuffer.wrap(header).getInt(4);
        int total = RecordFrame.HEADER_SIZE + bodyLen;
        if (bodyLen < 0 || total > maxBodySize + RecordFrame.HEADER_SIZE
                || scanCursor + total > log.getMaxOffset()) {
            lastError = "invalid frame length " + bodyLen + " at index cursor " + scanCursor;
            return;
        }

        byte[] frameBytes = log.read(scanCursor, total);
        RecordFrame.Frame parsed = RecordFrame.parse(ByteBuffer.wrap(frameBytes), 0, total, maxBodySize);
        if (parsed.getStatus() != RecordFrame.Status.OK) {
            lastError = "frame not replayable at offset " + scanCursor + ": " + parsed.getStatus();
            return;
        }

        byte[] body = Arrays.copyOfRange(frameBytes, RecordFrame.HEADER_SIZE, total);
        long end = scanCursor + total;
        for (int i = 0; i < indexes.size(); i++) {
            if (cursors[i] >= end) {
                continue;
            }
            try {
                indexes.get(i).onFrame(scanCursor, parsed, body);
            } catch (RuntimeException e) {
                lastError = "index '" + indexes.get(i).name() + "' failed at offset "
                        + scanCursor + ": " + e.getMessage();
                return;
            }
            cursors[i] = end;
        }
        lock.notifyAll();
    }
}
