package com.ispengya.file.api;

import com.ispengya.file.core.RecordFrame;
import com.ispengya.file.core.SequentialLog;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * {@link RecordScanner} 的 CommitLog 实现:按帧协议顺序读取,并跳过封存文件的浪费区。
 *
 * <p>内部不持有文件句柄,读取复用 {@link SequentialLog};创建时固定 endOffset,
 * 保证同一次扫描看到稳定的逻辑范围。</p>
 */
final class CommitLogScanner implements RecordScanner {

    private final SequentialLog log;
    private final int maxBodySize;
    private final long endOffset;

    private long cursor;
    private long currentOffset = -1L;
    private RecordFrame.Frame currentFrame;
    private byte[] currentBody;
    private boolean closed;

    CommitLogScanner(SequentialLog log, int maxBodySize, long startOffset) {
        if (startOffset < 0) {
            throw new IllegalArgumentException("startOffset must be non-negative");
        }
        this.log = log;
        this.maxBodySize = maxBodySize;
        this.endOffset = log.getMaxOffset();
        this.cursor = startOffset;
    }

    @Override
    public boolean next() {
        if (closed) {
            return false;
        }
        while (cursor < endOffset) {
            byte[] header;
            try {
                header = log.read(cursor, RecordFrame.HEADER_SIZE);
            } catch (IllegalArgumentException e) {
                // 帧不跨文件:封存文件尾部的浪费区不可读,跳到下一个文件起点
                long nextFileStart = log.nextFileStartOffset(cursor);
                if (nextFileStart > cursor) {
                    cursor = nextFileStart;
                    continue;
                }
                return false; // 没有后续文件,快照内已无可读帧
            }

            int bodyLen = ByteBuffer.wrap(header).getInt(4);
            int total = RecordFrame.HEADER_SIZE + bodyLen;
            if (bodyLen < 0 || total > maxBodySize + RecordFrame.HEADER_SIZE
                    || cursor + total > endOffset) {
                throw new StoreCorruptedException(cursor, "invalid frame length " + bodyLen);
            }

            byte[] frameBytes = log.read(cursor, total);
            RecordFrame.Frame parsed = RecordFrame.parse(
                    ByteBuffer.wrap(frameBytes), 0, total, maxBodySize);
            if (parsed.getStatus() != RecordFrame.Status.OK) {
                throw new StoreCorruptedException(cursor, "frame status: " + parsed.getStatus());
            }

            this.currentOffset = cursor;
            this.currentFrame = parsed;
            this.currentBody = Arrays.copyOfRange(frameBytes, RecordFrame.HEADER_SIZE, total);
            this.cursor = cursor + total;
            return true;
        }
        return false;
    }

    @Override
    public long offset() {
        requireCurrent();
        return currentOffset;
    }

    @Override
    public long endOffset() {
        return endOffset;
    }

    @Override
    public RecordFrame.Frame frame() {
        requireCurrent();
        return currentFrame;
    }

    @Override
    public byte[] body() {
        requireCurrent();
        return currentBody;
    }

    @Override
    public void close() {
        closed = true;
        currentFrame = null;
        currentBody = null;
    }

    private void requireCurrent() {
        if (currentFrame == null) {
            throw new IllegalStateException("call next() and check its result first");
        }
    }
}
