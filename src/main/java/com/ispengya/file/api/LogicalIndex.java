package com.ispengya.file.api;

/**
 * 逻辑位点查询能力(由 ConsumeQueue 类索引提供)。
 *
 * <p>只有启用了对应索引的存储实例才具备该能力;未启用时
 * {@link MiniFileStore#logical()} 返回空。</p>
 *
 * @param <T> 业务记录类型
 */
public interface LogicalIndex<T> {

    /**
     * 按逻辑位点(第 N 条记录,从 0 开始)读取
     *
     * @return 记录对象;位点不存在时返回 null
     */
    T getByLogicalIndex(long logicalIndex);

    /**
     * 当前索引条目数(可安全遍历的位点范围是 [0, logicalCount))
     */
    long logicalCount();
}
