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
     * 按逻辑位点(第 N 条记录,从 0 开始)读取,经由 ConsumeQueue 定位。
     *
     * @return 记录对象;位点不存在时返回 null
     */
    T getByLogicalIndex(long logicalIndex);

    /**
     * 按业务 key 查询候选记录。
     *
     * <p>注意:当前 KeyIndex 仅存储 key 的 32 位 hash,hash 冲突时返回的
     * 是候选集合,调用方需要自行按业务字段二次校验。</p>
     */
    List<T> getByKey(String key);

    /**
     * 按 tagCode 范围查询 [beginTag, endTag] 内的记录。
     *
     * <p>依赖 tagCode 单调(默认写当前时间戳)的前提,实现为顺序扫描。</p>
     */
    List<T> queryByTagRange(long beginTag, long endTag);

    /**
     * 等待索引(ConsumeQueue 与 KeyIndex)完整覆盖"起始位置为 offset 的记录"。
     *
     * <p>ASYNC 模式下索引读({@code getByLogicalIndex/getByKey/queryByTagRange})的最终一致
     * 由本方法显式升级为强一致;SYNC 模式下恒立即返回 true。
     * {@code get(offset)} 只读真相源,任何模式都无需等待。</p>
     *
     * @param offset  put 返回的记录起始物理 offset
     * @return true 已覆盖;false 超时或回放因帧损坏停止(见实现类的错误可见性)
     */
    boolean awaitIndexed(long offset, long timeoutMillis);

    /**
     * 索引已覆盖到的 CommitLog 字节位点(单调不减)
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
