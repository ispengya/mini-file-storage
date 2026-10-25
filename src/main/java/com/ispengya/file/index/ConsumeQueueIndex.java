package com.ispengya.file.index;

import com.ispengya.file.core.RecordFrame;

/**
 * 内置 ConsumeQueue 索引插件:按帧顺序追加 20 字节逻辑位点条目。
 *
 * <p>自身的 entry 文件就是游标来源(最后一条的 offset+size),
 * 删除目录后 {@link #replayFrom(long)} 返回 0,由内核全量回放重建。</p>
 */
public class ConsumeQueueIndex implements RecordIndex {

    private final SimpleConsumeQueue consumeQueue;

    private volatile long cursor;

    public ConsumeQueueIndex(SimpleConsumeQueue consumeQueue) {
        this.consumeQueue = consumeQueue;
    }

    @Override
    public String name() {
        return "consumequeue";
    }

    @Override
    public long replayFrom(long logMaxOffset) {
        // 交叉校验:CQ 的页可能先于数据落盘,尾部撒谎条目必须先截断
        consumeQueue.truncateBeyond(logMaxOffset);
        this.cursor = consumeQueue.cqCursor();
        return cursor;
    }

    @Override
    public void onFrame(long physicalOffset, RecordFrame.Frame frame, byte[] body) {
        consumeQueue.append(physicalOffset, frame.getTotalLen(), frame.getTagCode());
        this.cursor = physicalOffset + frame.getTotalLen();
    }

    @Override
    public long progress() {
        return cursor;
    }

    @Override
    public void flush() {
        consumeQueue.flush();
    }

    @Override
    public void close() {
        consumeQueue.close();
    }

    public SimpleConsumeQueue getConsumeQueue() {
        return consumeQueue;
    }
}
