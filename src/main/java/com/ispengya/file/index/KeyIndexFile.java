package com.ispengya.file.index;

import com.ispengya.file.core.SimpleMappedFile;
import com.ispengya.file.core.SimpleMappedFileQueue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


public class KeyIndexFile {
    public static final int INDEX_UNIT_SIZE = 16;

    private final SimpleMappedFileQueue mappedFileQueue;

    /**
     * 默认构造:不做启动恢复,直接在文件尾部追加(历史兼容行为)
     */
    public KeyIndexFile(String storePath, int fileSize) {
        this.mappedFileQueue = new SimpleMappedFileQueue(storePath, fileSize);
    }

    /**
     * @param recover true 时启动按 16 字节单元扫描已有文件并恢复追加位置,
     *                避免重启后从文件头覆盖旧索引条目
     */
    public KeyIndexFile(String storePath, int fileSize, boolean recover) throws IOException {
        if (recover) {
            this.mappedFileQueue = SimpleMappedFileQueue.recoverForKeyIndex(storePath, fileSize);
        } else {
            this.mappedFileQueue = new SimpleMappedFileQueue(storePath, fileSize);
        }
    }

    public synchronized void put(long keyHash, long physicalOffset) {
        byte[] unit = new byte[INDEX_UNIT_SIZE];
        ByteBuffer buffer = ByteBuffer.wrap(unit);
        buffer.putLong(keyHash);
        buffer.putLong(physicalOffset);
        try {
            SimpleMappedFile mappedFile = mappedFileQueue.getLastMappedFile(true);
            if (mappedFile == null) {
                throw new IllegalStateException("no mapped file for key index");
            }
            if (!mappedFile.append(unit, 0, INDEX_UNIT_SIZE)) {
                mappedFile = mappedFileQueue.getLastMappedFile(true);
                if (mappedFile == null) {
                    throw new IllegalStateException("no mapped file for key index");
                }
                if (!mappedFile.append(unit, 0, INDEX_UNIT_SIZE)) {
                    throw new IllegalStateException("append key index failed");
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized List<Long> get(long keyHash) {
        List<Long> result = new ArrayList<>();
        long logicalIndex = 0;
        while (true) {
            long offsetInIndexFile = logicalIndex * INDEX_UNIT_SIZE;
            SimpleMappedFile mappedFile = mappedFileQueue.findMappedFileByOffset(offsetInIndexFile);
            if (mappedFile == null) {
                break;
            }
            long fileStart = mappedFile.getFileFromOffset();
            int position = (int) (offsetInIndexFile - fileStart);
            ByteBuffer buffer = mappedFile.read(position, INDEX_UNIT_SIZE);
            long storedKeyHash = buffer.getLong();
            long physicalOffset = buffer.getLong();
            if (storedKeyHash == keyHash) {
                result.add(physicalOffset);
            }
            logicalIndex++;
        }
        return result;
    }

    public synchronized long flush() {
        return mappedFileQueue.flush();
    }

    /**
     * 当前 key 索引条目数(供 Reput 观察进度;精确日志位点无法自推导,进度以 checkpoint 为准)
     */
    public synchronized long count() {
        return mappedFileQueue.getMaxOffset() / INDEX_UNIT_SIZE;
    }

    public void close() {
        flush();
        mappedFileQueue.close();
    }
}
