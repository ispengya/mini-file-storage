package com.ispengya.file.api;

/**
 * 读取到损坏记录时抛出
 *
 * <p>触发场景:CommitLog 帧的魔数不符、bodyLen 非法或 CRC 校验不匹配
 * (典型成因:断电页撕裂、盘上静默损坏、误读非帧数据区)。</p>
 *
 * <p>语义提示:损坏位置之后的数据是否可读取决于恢复策略,
 * 正常流程中恢复扫描已停在损坏帧之前,读路径抛错说明 offset 本身越界或数据被外部改脏。</p>
 */
public class StoreCorruptedException extends RuntimeException {

    /**
     * 发生损坏的记录物理 offset
     */
    private final long physicalOffset;

    public StoreCorruptedException(long physicalOffset, String reason) {
        super("commit log record corrupted at offset " + physicalOffset + ": " + reason);
        this.physicalOffset = physicalOffset;
    }

    public long getPhysicalOffset() {
        return physicalOffset;
    }
}
