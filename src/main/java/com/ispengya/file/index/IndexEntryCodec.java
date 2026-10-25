package com.ispengya.file.index;

import java.nio.ByteBuffer;

/**
 * 定长索引 entry 编解码,供 {@link FixedUnitIndexFile} 复用。
 *
 * <p>用于自定义 ConsumeQueue / IndexFile 等"每条记录固定字节数"的派生索引。
 * 恢复时遇到 {@link #isValid(Object)} 为 false 的 entry 即停止推进。</p>
 *
 * @param <E> entry 类型
 */
public interface IndexEntryCodec<E> {

    /**
     * 单条 entry 的固定字节数(必须大于 0)
     */
    int unitSize();

    /**
     * 解码一条 entry
     *
     * @param source position 为 entry 起点,limit 为 entry 终点
     */
    E decode(ByteBuffer source);

    /**
     * 编码一条 entry
     *
     * @param target 容量为 unitSize 的缓冲区,position=0;实现必须写满整个 entry
     */
    void encode(E entry, ByteBuffer target);

    /**
     * 结构校验:恢复扫描遇到无效 entry 即停止
     */
    boolean isValid(E entry);
}
