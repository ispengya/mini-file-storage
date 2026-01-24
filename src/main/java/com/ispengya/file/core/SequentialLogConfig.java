package com.ispengya.file.core;

/**
 * 顺序日志配置
 * 封装存储路径、单个文件大小以及刷盘策略
 */
public class SequentialLogConfig {
    /**
     * 存储根目录路径
     */
    private final String storePath;
    /**
     * 单个数据文件大小
     */
    private final int fileSize;
    /**
     * 是否同步刷盘
     * true 表示每次追加后立刻 flush，false 表示调用方自行控制 flush 时机
     */
    private final boolean syncFlush;

    public SequentialLogConfig(String storePath, int fileSize, boolean syncFlush) {
        this.storePath = storePath;
        this.fileSize = fileSize;
        this.syncFlush = syncFlush;
    }

    public String getStorePath() {
        return storePath;
    }

    public int getFileSize() {
        return fileSize;
    }

    public boolean isSyncFlush() {
        return syncFlush;
    }
}

