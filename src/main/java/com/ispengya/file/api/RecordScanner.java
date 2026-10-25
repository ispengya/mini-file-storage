package com.ispengya.file.api;

import com.ispengya.file.core.RecordFrame;

/**
 * CommitLog 只读顺序扫描器。
 *
 * <p>创建时会快照当前日志合法尾位点;之后的 {@link #next()} 只在该快照范围内推进,
 * 不会追逐创建之后的新写入。需要"快照 + 尾部追赶"时,记录 {@link #endOffset()},
 * 之后用 {@link MiniFileStore#scanFrom(long)} 从该位点继续扫描。</p>
 *
 * <p>典型用途:Redis 类 KV 的启动加载、离线 compaction/rewrite、自定义索引重建。
 * 扫描器不持有文件资源,但应与创建它的 store 生命周期一致;
 * store 关闭后继续读取会失败。</p>
 *
 * <p>线程模型:非线程安全,一个实例只能被单个线程使用。</p>
 */
public interface RecordScanner extends AutoCloseable {

    /**
     * 前进到下一条完整记录
     *
     * @return false 表示已到达快照尾
     * @throws StoreCorruptedException 合法前缀内出现无法解析的帧
     */
    boolean next();

    /**
     * 当前记录的起始物理 offset(仅在 {@link #next()} 返回 true 后有效)
     */
    long offset();

    /**
     * 快照尾位点(创建扫描器时的日志合法尾)
     */
    long endOffset();

    /**
     * 当前记录的帧头信息(仅在 {@link #next()} 返回 true 后有效)
     */
    RecordFrame.Frame frame();

    /**
     * 当前记录的 Codec 编码字节(仅在 {@link #next()} 返回 true 后有效)
     *
     * <p>每次调用返回上一帧的独立数组,调用方可以自行持有/修改。</p>
     */
    byte[] body();

    @Override
    void close();
}
