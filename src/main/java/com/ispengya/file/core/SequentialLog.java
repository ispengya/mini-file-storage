package com.ispengya.file.core;

/**
 * 顺序写日志接口抽象
 * 表示一个只能追加写入的日志文件集合，提供追加、刷盘、查询最大偏移等能力
 */
public interface SequentialLog extends AutoCloseable {
    /**
     * 追加一条记录到日志末尾
     *
     * @param data 要追加的字节数组
     * @return 追加完成后的最大物理偏移量
     */
    long append(byte[] data);

    /**
     * 从指定偏移和长度追加一段数据
     *
     * @param data   原始字节数组
     * @param offset 起始下标
     * @param length 写入长度
     * @return 追加完成后的最大物理偏移量
     */
    long append(byte[] data, int offset, int length);

    /**
     * 从指定物理偏移读取一段数据
     *
     * @param offset  全局物理偏移
     * @param length  要读取的字节数
     * @return 长度为 length 的字节数组
     */
    byte[] read(long offset, int length);

    /**
     * 将内存中的数据刷到磁盘
     *
     * @return 刷盘后的最大物理偏移量
     */
    long flush();

    /**
     * 获取当前已经写入的最大物理偏移量
     *
     * @return 最大偏移量
     */
    long getMaxOffset();

    /**
     * 关闭资源
     */
    @Override
    void close();
}

