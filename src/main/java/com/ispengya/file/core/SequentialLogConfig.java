package com.ispengya.file.core;

/**
 * 顺序日志配置
 * 封装存储路径、单个文件大小、刷盘策略与恢复扫描的 body 上限
 */
public class SequentialLogConfig {
    /**
     * 兼容旧调用的 body 上限:不再额外限制帧长(仅受单文件大小约束)
     */
    public static final int NO_BODY_LIMIT = Integer.MAX_VALUE;

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
    /**
     * 恢复扫描允许的最大 body 长度,必须与读路径的 maxBodySize 一致,
     * 否则配置调小后旧帧会被恢复判为合法、读取时才报错
     */
    private final int maxBodySize;

    public SequentialLogConfig(String storePath, int fileSize, boolean syncFlush) {
        this(storePath, fileSize, syncFlush, NO_BODY_LIMIT);
    }

    public SequentialLogConfig(String storePath, int fileSize, boolean syncFlush, int maxBodySize) {
        this.storePath = storePath;
        this.fileSize = fileSize;
        this.syncFlush = syncFlush;
        this.maxBodySize = maxBodySize;
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

    /**
     * 恢复扫描允许的最大 body 长度
     */
    public int getMaxBodySize() {
        return maxBodySize;
    }
}

