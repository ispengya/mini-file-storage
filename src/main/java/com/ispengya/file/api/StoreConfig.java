package com.ispengya.file.api;

/**
 * 存储引擎配置
 *
 * <p>调用方只需要指定一个根目录,内部各组件(CommitLog、ConsumeQueue、
 * KeyIndex、Checkpoint)使用约定好的子路径,避免手动装配时搞错目录关系。
 * 通过 {@link #builder(String)} 创建,未指定的项使用默认值。</p>
 */
public class StoreConfig {

    public static final int DEFAULT_COMMIT_LOG_FILE_SIZE = 8 * 1024 * 1024;
    public static final int DEFAULT_CONSUME_QUEUE_FILE_SIZE = 200 * 1024;
    public static final int DEFAULT_KEY_INDEX_FILE_SIZE = 64 * 1024;

    /**
     * 存储根目录
     */
    private final String baseDir;
    /**
     * 单个 CommitLog 文件大小
     */
    private final int commitLogFileSize;
    /**
     * 单个 ConsumeQueue 索引文件大小
     */
    private final int consumeQueueFileSize;
    /**
     * 单个 KeyIndex 索引文件大小
     */
    private final int keyIndexFileSize;
    /**
     * 是否同步刷盘(每次 append 后立即 flush)
     */
    private final boolean syncFlush;

    private StoreConfig(Builder builder) {
        this.baseDir = builder.baseDir;
        this.commitLogFileSize = builder.commitLogFileSize;
        this.consumeQueueFileSize = builder.consumeQueueFileSize;
        this.keyIndexFileSize = builder.keyIndexFileSize;
        this.syncFlush = builder.syncFlush;
    }

    public static Builder builder(String baseDir) {
        return new Builder(baseDir);
    }

    public String getBaseDir() {
        return baseDir;
    }

    public int getCommitLogFileSize() {
        return commitLogFileSize;
    }

    public int getConsumeQueueFileSize() {
        return consumeQueueFileSize;
    }

    public int getKeyIndexFileSize() {
        return keyIndexFileSize;
    }

    public boolean isSyncFlush() {
        return syncFlush;
    }

    /**
     * CommitLog 数据目录
     */
    public String commitLogPath() {
        return baseDir + "/data";
    }

    /**
     * ConsumeQueue 索引目录
     */
    public String consumeQueuePath() {
        return baseDir + "/consumequeue";
    }

    /**
     * KeyIndex 索引目录
     */
    public String keyIndexPath() {
        return baseDir + "/keyindex";
    }

    /**
     * Checkpoint 文件路径
     */
    public String checkpointPath() {
        return baseDir + "/checkpoint.properties";
    }

    @Override
    public String toString() {
        return "StoreConfig{baseDir='" + baseDir + "'"
                + ", commitLogFileSize=" + commitLogFileSize
                + ", consumeQueueFileSize=" + consumeQueueFileSize
                + ", keyIndexFileSize=" + keyIndexFileSize
                + ", syncFlush=" + syncFlush + '}';
    }

    public static class Builder {
        private final String baseDir;
        private int commitLogFileSize = DEFAULT_COMMIT_LOG_FILE_SIZE;
        private int consumeQueueFileSize = DEFAULT_CONSUME_QUEUE_FILE_SIZE;
        private int keyIndexFileSize = DEFAULT_KEY_INDEX_FILE_SIZE;
        private boolean syncFlush = false;

        private Builder(String baseDir) {
            if (baseDir == null || baseDir.trim().isEmpty()) {
                throw new IllegalArgumentException("baseDir must not be blank");
            }
            this.baseDir = baseDir;
        }

        public Builder commitLogFileSize(int fileSize) {
            checkPositive(fileSize, "commitLogFileSize");
            this.commitLogFileSize = fileSize;
            return this;
        }

        public Builder consumeQueueFileSize(int fileSize) {
            checkPositive(fileSize, "consumeQueueFileSize");
            this.consumeQueueFileSize = fileSize;
            return this;
        }

        public Builder keyIndexFileSize(int fileSize) {
            checkPositive(fileSize, "keyIndexFileSize");
            this.keyIndexFileSize = fileSize;
            return this;
        }

        public Builder syncFlush(boolean syncFlush) {
            this.syncFlush = syncFlush;
            return this;
        }

        public StoreConfig build() {
            return new StoreConfig(this);
        }

        private static void checkPositive(int value, String name) {
            if (value <= 0) {
                throw new IllegalArgumentException(name + " must be positive, got " + value);
            }
        }
    }
}
