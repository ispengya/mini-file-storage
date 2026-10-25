package com.ispengya.file.index;

import com.ispengya.file.core.RecordFrame;

import java.io.File;
import java.util.List;

/**
 * 内置 KeyIndex 索引插件:把帧头的 keyHash 与物理 offset 追加成 16 字节单元。
 *
 * <p>KeyIndex 条目本身无法推出日志进度(只有 offset,没有帧长),
 * 因此额外维护一个 {@code cursor} 文件。启动时:</p>
 * <ul>
 *   <li>游标缺失/越界/索引为空 → 从 0 全量回放;</li>
 *   <li>游标有效 → 从游标续传,重放重叠区最多产生重复条目,查询侧按 offset 去重。</li>
 * </ul>
 */
public class KeyIndexPlugin implements RecordIndex {

    private final KeyIndexFile keyIndexFile;
    private final IndexCursorFile cursorFile;

    private volatile long cursor;

    /**
     * @param cursorFile 游标文件;传 null 表示不持久化游标(仅供兼容/测试装配)
     */
    public KeyIndexPlugin(KeyIndexFile keyIndexFile, File cursorFile) {
        this.keyIndexFile = keyIndexFile;
        this.cursorFile = cursorFile == null ? null : new IndexCursorFile(cursorFile);
    }

    @Override
    public String name() {
        return "keyindex";
    }

    @Override
    public long replayFrom(long logMaxOffset) {
        long stored = cursorFile == null ? -1L : cursorFile.load();
        long from = stored >= 0 && stored <= logMaxOffset && keyIndexFile.count() > 0
                ? stored : 0L;
        this.cursor = from;
        return from;
    }

    @Override
    public void onFrame(long physicalOffset, RecordFrame.Frame frame, byte[] body) {
        if (frame.getKeyHash() != 0L) {
            keyIndexFile.put(frame.getKeyHash(), physicalOffset);
        }
        this.cursor = physicalOffset + frame.getTotalLen();
    }

    @Override
    public long progress() {
        return cursor;
    }

    @Override
    public void flush() {
        keyIndexFile.flush();
        if (cursorFile != null) {
            cursorFile.save(cursor);
        }
    }

    @Override
    public void close() {
        try {
            flush();
        } finally {
            keyIndexFile.close();
        }
    }

    public KeyIndexFile getKeyIndexFile() {
        return keyIndexFile;
    }

    /**
     * 查询某 hash 的全部候选物理 offset
     */
    public List<Long> lookup(long keyHash) {
        return keyIndexFile.get(keyHash);
    }

    public long entryCount() {
        return keyIndexFile.count();
    }

}
