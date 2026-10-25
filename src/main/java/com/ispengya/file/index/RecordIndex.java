package com.ispengya.file.index;

import com.ispengya.file.core.RecordFrame;

/**
 * 可插拔的派生索引构建接口。
 *
 * <p>内核只承诺 CommitLog 是一致的前缀真相源;索引是"可弃的派生视图",
 * 由使用方决定建哪些、怎么建。本接口就是内核与索引实现之间唯一的契约:</p>
 *
 * <ul>
 *   <li>启动时内核调用一次 {@link #replayFrom(long)},由索引声明续传起点:
 *       新索引返回 0(全量回放),已有索引返回自己持久化的游标;</li>
 *   <li>回放与实时追加共用 {@link #onFrame(long, RecordFrame.Frame, byte[])}:
 *       内核按 CommitLog 顺序逐帧调用,索引必须保证从游标重放是幂等的;</li>
 *   <li>索引在每帧处理完成后更新 {@link #progress()},用于自身持久化与诊断;
 *       内核按帧独立推进覆盖游标,不以实现返回的 progress 作为等待谓词;</li>
 *   <li>{@link #flush()} 负责把索引数据与游标按"先数据后游标"的顺序落盘;</li>
 *   <li>关闭由内核统一调用,实现应释放自己持有的资源。</li>
 * </ul>
 *
 * <p>线程模型:{@link #onFrame} 可能由后台索引线程调用,查询视图必须自行保证并发安全;
 * SYNC 模式下 put 返回时,内核的 happens-before 只保证同一调用线程能看到本帧更新。</p>
 *
 * <p><b>禁止在 {@link #onFrame} 中回调 {@code MiniFileStore} 的读写方法</b>:
 * 该回调运行在索引线程,而 SYNC put 会持有门面锁等待索引追平,回调会形成
 * 门面锁 → 索引等待 → 门面锁的环。插件需要的数据应通过传入的 frame/body
 * 或自身在启动时持有的外部状态获取。</p>
 *
 * <p>注意:消费位点、分布式锁这类"权威状态"不是派生视图,不能从 CommitLog 重建,
 * 不应实现本接口;它们应该走独立的持久化机制。</p>
 */
public interface RecordIndex extends AutoCloseable {

    /**
     * 索引唯一名字,用于诊断、元数据与去重
     */
    String name();

    /**
     * 声明本次回放的起点(启动时调用一次)。
     *
     * <p>实现应在这里做自身状态校验/清理:丢弃指向 {@code logMaxOffset} 之后的陈旧条目,
     * 或返回 0 全量重建。之后每帧的结束位点由内核通过 {@link #onFrame} 推进。</p>
     *
     * @param logMaxOffset CommitLog 全局前缀恢复后的合法尾位点
     * @return 开始回放的物理位点
     */
    long replayFrom(long logMaxOffset);

    /**
     * 处理一帧(回放与实时追加共用入口)。
     *
     * <p>必须幂等:进程可能在"索引已写、游标未落盘"的位置崩溃,
     * 重启后会从旧游标重放同一批帧。</p>
     *
     * @param physicalOffset 帧在 CommitLog 中的起始物理偏移
     * @param frame          已通过 magic/CRC 校验的帧头与长度信息
     * @param body           Codec 编码后的业务字节
     */
    void onFrame(long physicalOffset, RecordFrame.Frame frame, byte[] body);

    /**
     * 已覆盖到的 CommitLog 物理位点(通常是最近一帧的结束位置)
     */
    long progress();

    /**
     * 把索引数据与游标落盘;内核保证在 CommitLog 之后、checkpoint 之前调用
     */
    void flush();

    /**
     * 释放索引资源;默认无操作
     */
    @Override
    default void close() {
    }
}
