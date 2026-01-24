package com.ispengya.file.model;

/**
 * 示例用日志记录类型
 * 仅作为演示，说明通用存储引擎可以承载任意结构化数据
 */
public class LogRecord {
    private final long timestamp;
    private final String level;
    private final String message;

    public LogRecord(long timestamp, String level, String message) {
        this.timestamp = timestamp;
        this.level = level;
        this.message = message;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getLevel() {
        return level;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return "LogRecord{" +
                "timestamp=" + timestamp +
                ", level='" + level + '\'' +
                ", message='" + message + '\'' +
                '}';
    }
}

