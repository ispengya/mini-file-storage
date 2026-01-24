package com.ispengya.file.codec;

import com.ispengya.file.model.LogRecord;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;


/**
 * LogRecord 的编解码实现
 * 编码格式：
 * [8 字节 timestamp][4 字节 level 长度][level 字节][4 字节 message 长度][message 字节]
 */
public class LogRecordCodec implements Codec<LogRecord> {
    @Override
    public byte[] encode(LogRecord value) {
        byte[] levelBytes = value.getLevel().getBytes(StandardCharsets.UTF_8);
        byte[] msgBytes = value.getMessage().getBytes(StandardCharsets.UTF_8);
        int length = 8 + 4 + levelBytes.length + 4 + msgBytes.length;
        ByteBuffer buffer = ByteBuffer.allocate(length);
        buffer.putLong(value.getTimestamp());
        buffer.putInt(levelBytes.length);
        buffer.put(levelBytes);
        buffer.putInt(msgBytes.length);
        buffer.put(msgBytes);
        return buffer.array();
    }

    @Override
    public LogRecord decode(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        long ts = buffer.getLong();
        int levelLen = buffer.getInt();
        byte[] levelBytes = new byte[levelLen];
        buffer.get(levelBytes);
        int msgLen = buffer.getInt();
        byte[] msgBytes = new byte[msgLen];
        buffer.get(msgBytes);
        String level = new String(levelBytes, StandardCharsets.UTF_8);
        String msg = new String(msgBytes, StandardCharsets.UTF_8);
        return new LogRecord(ts, level, msg);
    }
}


