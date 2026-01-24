package com.ispengya.file.core;

import java.io.IOException;


/**
 * 基于内存映射文件实现的顺序日志
 * 对外提供简单的 append/flush 接口，内部通过 SimpleMappedFileQueue 管理文件滚动
 */
public class MmapSequentialLog implements SequentialLog {
    /**
     * 底层文件队列
     */
    private final SimpleMappedFileQueue fileQueue;
    /**
     * 配置信息
     */
    private final SequentialLogConfig config;

    public MmapSequentialLog(SequentialLogConfig config) {
        this.config = config;
        try {
            this.fileQueue = SimpleMappedFileQueue.recoverForRecordStore(config.getStorePath(), config.getFileSize());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 追加一段数据到日志末尾
     * 等价于从下标 0 开始写入整个数组
     */
    @Override
    public synchronized long append(byte[] data) {
        return append(data, 0, data.length);
    }

    /**
     * 追加一段数据到日志末尾
     * 会自动处理当前文件写满时的文件滚动
     */
    @Override
    public synchronized long append(byte[] data, int offset, int length) {
        try {
            SimpleMappedFile mappedFile = fileQueue.getLastMappedFile(true);
            if (mappedFile == null) {
                throw new IllegalStateException("no mapped file");
            }
            if (!mappedFile.append(data, offset, length)) {
                mappedFile = fileQueue.getLastMappedFile(true);
                if (mappedFile == null) {
                    throw new IllegalStateException("no mapped file");
                }
                if (!mappedFile.append(data, offset, length)) {
                    throw new IllegalStateException("append failed");
                }
            }
            long maxOffset = fileQueue.getMaxOffset();
            if (config.isSyncFlush()) {
                flush();
            }
            return maxOffset;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 从指定物理偏移读取一段数据
     */
    @Override
    public synchronized byte[] read(long offset, int length) {
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative");
        }
        if (length == 0) {
            return new byte[0];
        }
        SimpleMappedFile mappedFile = fileQueue.findMappedFileByOffset(offset);
        if (mappedFile == null) {
            throw new IllegalArgumentException("offset out of range: " + offset);
        }
        long fileStart = mappedFile.getFileFromOffset();
        int position = (int) (offset - fileStart);
        byte[] result = new byte[length];
        mappedFile.read(position, length).get(result);
        return result;
    }

    /**
     * 刷新所有数据到磁盘
     */
    @Override
    public synchronized long flush() {
        return fileQueue.flush();
    }

    /**
     * 获取当前最大物理偏移量
     */
    @Override
    public synchronized long getMaxOffset() {
        return fileQueue.getMaxOffset();
    }

    /**
     * 关闭底层文件队列
     */
    @Override
    public synchronized void close() {
        flush();
        fileQueue.close();
    }
}
