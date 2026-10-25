package com.ispengya.file.api;

import java.util.List;

/**
 * 业务 key 查询能力(由 KeyIndex 类索引提供)。
 *
 * <p>只有启用了对应索引的存储实例才具备该能力;未启用时
 * {@link MiniFileStore#keys()} 返回空。</p>
 *
 * @param <T> 业务记录类型
 */
public interface KeyIndexView<T> {

    /**
     * 按业务 key 查询候选记录(32 位 hash 冲突时返回候选集,调用方需二次校验)
     */
    List<T> getByKey(String key);
}
