package com.ispengya.file.api;

import java.util.List;

/**
 * 嵌入式记录存储门面
 *
 * <p>面向"任意中间件可直接嵌入"的通用能力抽象:调用方只接触本接口与
 * {@link com.ispengya.file.codec.Codec},不需要感知 CommitLog、ConsumeQueue、
 * KeyIndex、Checkpoint 的装配细节。存储的内容是与业务无关的泛型记录 T:</p>
 *
 * <ul>
 *   <li>消息存储:把消息编成 T(tagCode 用作时间戳或队列位点)</li>
 *   <li>日志/事件溯源:事件对象即 T,按 tag 范围回放</li>
 *   <li>简单 KV/文档:业务键通过 key 参数落 KeyIndex</li>
 * </ul>
 *
 * <h3>一致性语义(取决于 {@code StoreConfig.IndexMode})</h3>
 * <table border="1">
 *   <tr><th>方法</th><th>SYNC(默认)</th><th>ASYNC</th></tr>
 *   <tr><td>{@code get(offset)}</td><td>强一致</td><td>强一致(只读真相源)</td></tr>
 *   <tr><td>{@code getByLogicalIndex/getByKey/queryByTagRange}</td>
 *       <td>强一致(put 内等待索引追平)</td><td>最终一致,需先 {@link #awaitIndexed}</td></tr>
 * </table>
 *
 * <p>线程模型:实现类的方法允许并发调用,单条记录的写入是串行的。</p>
 *
 * <p>索引是可选的:{@link #getByLogicalIndex} 与 {@link #queryByTagRange} 需要
 * ConsumeQueue 类索引,{@link #getByKey} 需要 KeyIndex。{@code MiniFileStore}
 * 的兼容构造器默认启用两者;通过 builder 可以只启用部分或完全不启用。
 * 未启用时调用对应方法会抛 {@code IllegalStateException};需要按能力判断时,
 * 使用 {@code MiniFileStore.logical()} / {@code keys()} 或自定义索引的视图接口。</p>
 *
 * @param <T> 业务记录类型
 */
public interface FileStore<T> extends AutoCloseable {

    /**
     * 写入一条记录,tagCode 默认为写入时刻(System.currentTimeMillis()),
     * 因此 {@link #queryByTagRange(long, long)} 可直接作为时间范围查询使用。
     *
     * @return 该记录在 CommitLog 中的起始物理 offset
     */
    long put(T value);

    /**
     * 写入一条记录并显式指定 tagCode(写入 ConsumeQueue 索引,用于范围查询)。
     *
     * @return 该记录在 CommitLog 中的起始物理 offset
     */
    long put(T value, long tagCode);

    /**
     * 写入一条记录并建立业务 key 索引(key → 物理 offset)。
     *
     * @param key 业务键,不能为 null
     * @return 该记录在 CommitLog 中的起始物理 offset
     */
    long put(T value, String key);

    /**
     * 按物理 offset 精确读取一条记录。
     */
    T get(long physicalOffset);

    /**
     * 从物理 offset 0 开始只读扫描当前 CommitLog 的稳定快照。
     *
     * <p>扫描范围在创建时固定,不追逐之后的新写入;典型用于启动加载、
     * 离线 compaction/rewrite、自定义索引重建。</p>
     */
    RecordScanner scan();

    /**
     * 从指定物理 offset 开始只读扫描。
     *
     * <p>起始位点应是帧边界(例如 put 返回值或上一次扫描的
     * {@link RecordScanner#endOffset()}),否则会因帧校验失败抛
     * {@link StoreCorruptedException}。</p>
     */
    RecordScanner scanFrom(long startOffset);

    /**
     * 按 CommitLog 顺序解码并访问记录;visitor 返回 false 可提前停止。
     */
    void forEach(RecordVisitor<T> visitor);

    /**
     * 按逻辑位点(第 N 条记录,从 0 开始)读取,经由 ConsumeQueue 定位。
     *
     * <p>需要 ConsumeQueue 索引已启用;未启用时抛 {@code IllegalStateException}。</p>
     *
     * @return 记录对象;位点不存在时返回 null
     */
    T getByLogicalIndex(long logicalIndex);

    /**
     * 按业务 key 查询候选记录。
     *
     * <p>需要 KeyIndex 已启用;未启用时抛 {@code IllegalStateException}。</p>
     *
     * <p>注意:当前 KeyIndex 仅存储 key 的 32 位 hash,hash 冲突时返回的
     * 是候选集合,调用方需要自行按业务字段二次校验。</p>
     */
    List<T> getByKey(String key);

    /**
     * 按 tagCode 范围查询 [beginTag, endTag] 内的记录。
     *
     * <p>需要 ConsumeQueue 索引已启用;未启用时抛 {@code IllegalStateException}。</p>
     *
     * <p>依赖 tagCode 单调(默认写当前时间戳)的前提,实现为顺序扫描。</p>
     */
    List<T> queryByTagRange(long beginTag, long endTag);

    /**
     * 等待所有启用索引完整覆盖"起始位置为 offset 的记录"。
     *
     * <p>ASYNC 模式下索引读的最终一致由本方法显式升级为强一致;SYNC 模式下恒立即返回 true;
     * 没有启用任何索引时也立即返回 true。{@code get(offset)} 只读真相源,任何模式都无需等待。</p>
     *
     * @param offset  put 返回的记录起始物理 offset
     * @return true 已覆盖;false 超时或回放因帧损坏/索引失败停止(见实现类的错误可见性)
     */
    boolean awaitIndexed(long offset, long timeoutMillis);

    /**
     * 所有启用索引覆盖位点的最小值(单调不减);没有索引时返回日志合法尾位点
     */
    long maxIndexedOffset();

    /**
     * 将 CommitLog、ConsumeQueue、KeyIndex 全部强制刷盘,并按"数据 → 索引 → 水位"顺序保存 checkpoint。
     */
    void flush();

    /**
     * 刷盘并释放所有文件资源,幂等。
     */
    @Override
    void close();
}
