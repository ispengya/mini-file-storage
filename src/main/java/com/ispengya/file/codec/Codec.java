package com.ispengya.file.codec;

/**
 * 通用编码解码接口
 * 用于在内存对象与字节数组之间转换
 */
public interface Codec<T> {
    /**
     * 将对象编码为字节数组
     */
    byte[] encode(T value);

    /**
     * 从字节数组解码出对象
     */
    T decode(byte[] data);
}

