package com.ispengya.file.api;

import com.ispengya.file.core.RecordFrame;

/**
 * 解码后的记录访问器,供 {@link MiniFileStore#forEach(RecordVisitor)} 使用。
 *
 * @param <T> 业务记录类型
 */
@FunctionalInterface
public interface RecordVisitor<T> {

    /**
     * @param offset 记录起始物理 offset
     * @param frame  帧头信息
     * @param value  解码后的业务对象
     * @return false 表示提前停止扫描
     */
    boolean visit(long offset, RecordFrame.Frame frame, T value);
}
