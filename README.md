# mini-file-storage

> 通用嵌入式顺序记录存储引擎:CommitLog 是唯一真相源,ConsumeQueue / KeyIndex / 自定义索引都是可选插件。

Java 8+ · Maven · 零运行时依赖 · 当前 76 个测试

`mini-file-storage` 不是完整的 MQ 或 Redis,而是一个可以嵌入 Java 应用的顺序存储底座:在 mmap 分段文件之上提供记录格式、崩溃恢复、可选索引、只读扫描和自定义索引工具。消息日志、事件溯源、审计流水、简易 KV/WAL 都可以基于它搭建。

## 你会得到什么

| 能力 | 说明 |
|---|---|
| 顺序追加 + 自动滚动 | CommitLog 按帧写入,单文件写满自动滚动,文件名即起始物理 offset |
| 帧自校验 + 崩溃恢复 | magic + bodyLen + CRC32;恢复扫描遇到坏帧停止,全局前缀截断,残片隔离 |
| 可选索引 | 不装索引就是纯 WAL;按需启用 ConsumeQueue / KeyIndex / 自定义插件 |
| 自定义索引 SPI | 实现 `RecordIndex`,由 `IndexHost` 统一驱动回放、游标和生命周期 |
| 多种查询 | 物理 offset、逻辑位点、业务 key、tagCode 范围 |
| 持久化 | `syncFlush` 默认开启;数据 → 索引 → checkpoint 有序刷盘 |
| 只读扫描 | 固定快照遍历 CommitLog,支撑启动加载、索引重建、离线 rewrite |
| 索引文件工具 | `FixedUnitIndexFile` + `IndexEntryCodec` + `IndexCursorFile`,复用分段、恢复与截断 |
| 运行保障 | 进程独占锁、`store.properties` 格式校验、全局前缀隔离、物理截断 |

## 快速开始

要求 JDK 8+ 和 Maven。当前按源码/IDE 使用;执行 `mvn install` 后可按 `com.ispengya:file-storage:1.0-SNAPSHOT` 从本地仓库引入。

```bash
mvn test          # 运行全部测试
mvn -DskipTests package
```

最小示例:

```java
StoreConfig config = StoreConfig.builder("./data/demo").build();

try (FileStore<LogRecord> store = new MiniFileStore<>(config, new LogRecordCodec())) {
    long now = System.currentTimeMillis();

    long offset = store.put(new LogRecord(now, "INFO", "hello"));
    LogRecord byOffset = store.get(offset);
    LogRecord byLogical = store.getByLogicalIndex(0);

    store.put(new LogRecord(now, "INFO", "order-1-body"), "order-1");
    List<LogRecord> byKey = store.getByKey("order-1");
    List<LogRecord> recent = store.queryByTagRange(now - 60_000, now);

    store.flush();
}
```

- `new MiniFileStore<>(config, codec)` 保持旧行为:默认启用 CQ + KeyIndex。
- `put(value)` 的默认 tagCode 是当前时间,可直接当时间范围查询使用。
- 对象与字节的互转由 `Codec<T>` 负责,内置 `LogRecordCodec` 只是示例。

## 按需装配索引

builder 默认**不启用任何索引**,也就是纯 WAL:

```java
// 纯 WAL:不建索引目录、不启动索引线程
MiniFileStore<LogRecord> wal = MiniFileStore.builder(config, codec).build();

// 任选其一:
MiniFileStore<LogRecord> cqOnly  = MiniFileStore.builder(config, codec)
        .withConsumeQueue().build();
MiniFileStore<LogRecord> keyOnly = MiniFileStore.builder(config, codec)
        .withKeyIndex().build();
MiniFileStore<LogRecord> both    = MiniFileStore.builder(config, codec)
        .withDefaultIndexes().build();

// 自定义派生索引
MiniFileStore<LogRecord> custom  = MiniFileStore.builder(config, codec)
        .withIndex(new MemoryKvIndex(codec))
        .build();
```

按能力访问,未启用的返回空:

```java
cqOnly.logical().orElseThrow().getByLogicalIndex(0);
keyOnly.keys().orElseThrow().getByKey("order-1");
custom.index(KvView.class).orElseThrow().get("order-1");
```

| 能力 | 依赖 |
|---|---|
| `put` / `get(offset)` / `scan` / `forEach` | 仅 CommitLog |
| `getByLogicalIndex` / `queryByTagRange` | ConsumeQueue |
| `getByKey` | KeyIndex |
| 自定义查询视图 | 对应自定义插件 |

未启用对应索引时,调用 `getByLogicalIndex` / `getByKey` / `queryByTagRange` 会抛 `IllegalStateException`。

两个实用性质:

- **后加索引不需要迁移数据**。新索引 `replayFrom()` 返回 0,内核从 CommitLog 全量回放;已有索引按自己的游标跳过重叠区。纯 WAL 期间写入的 tagCode/keyHash 仍在帧头里,后开 CQ/KeyIndex 可以补建历史索引。
- **禁用一段时间再启用**时,内置索引会从各自游标续传,补齐禁用期间写入的帧。

## 自定义索引

索引是 CommitLog 的派生视图,由 `RecordIndex` 接口定义:

```java
public interface RecordIndex extends AutoCloseable {
    String name();                                                  // 唯一名字
    long replayFrom(long logMaxOffset);                             // 新索引返回 0,已有索引返回游标
    void onFrame(long offset, RecordFrame.Frame frame, byte[] body); // 回放与实时追加共用
    long progress();                                                // 自身进度(诊断/持久化)
    void flush();                                                   // 先数据后游标
    default void close() {}
}
```

契约:

- `onFrame` 必须幂等:崩溃可能从旧游标重放同一批帧。
- `replayFrom` 里应校验自身状态;全局截断后丢弃 `offset >= logMaxOffset` 的陈旧状态。
- `flush` 先刷索引数据,再原子写游标。
- `onFrame` 运行在后台索引线程,查询视图需自行保证并发安全。
- **不要在 `onFrame` 里回调 `MiniFileStore` 的读写方法**:SYNC put 持有门面锁等待索引追平,会形成死锁环。

内存索引示例:

```java
final class MemoryKvIndex implements RecordIndex {
    private final Map<String, Entry> map = new ConcurrentHashMap<>();
    private final Codec<Entry> codec;
    private volatile long cursor;

    public String name() { return "memory-kv"; }
    public long replayFrom(long max) { return 0; }       // 每次启动全量重建
    public void onFrame(long off, RecordFrame.Frame f, byte[] body) {
        Entry e = codec.decode(body);
        if (e.deleted()) map.remove(e.key()); else map.put(e.key(), e);
        cursor = off + f.getTotalLen();
    }
    public long progress() { return cursor; }
    public void flush() { /* 日志即真相源,无需落盘 */ }
}
```

定长文件索引不需要自己重写文件管理:

```java
IndexEntryCodec<MyEntry> codec = ...;   // unitSize / decode / encode / isValid
FixedUnitIndexFile<MyEntry> file =
        FixedUnitIndexFile.open("./data/my-index", 1024 * 1024, codec);

file.append(entry);
MyEntry e = file.get(0);
file.flush();
```

`FixedUnitIndexFile` 复用 `SimpleMappedFileQueue` 的分段、滚动、恢复与物理截断;`IndexCursorFile` 提供原子游标;`SimpleMappedFileQueue.recoverWith(scanner)` 允许自定义格式定义"每个文件的有效前缀"。

参考测试:`CustomIndexTest`(内存 KV)、`IndexFileToolkitTest`(定长 entry + cursor + `RecordIndex` 组合)。

## 只读扫描与离线重写

`scan()` / `scanFrom()` 返回 CommitLog 的稳定快照:

```java
try (RecordScanner scanner = store.scan()) {
    while (scanner.next()) {
        long offset = scanner.offset();
        LogRecord r = codec.decode(scanner.body());
        // 加载内存 Map / 重建自定义索引 / compaction
    }
}
```

- 扫描范围在创建时固定,不追逐之后的新写入。
- 需要"快照 + 尾部追赶"时记录 `scanner.endOffset()`,再用 `store.scanFrom(endOffset)` 续扫。
- `forEach(visitor)` 是解码版;visitor 返回 false 可提前停止。
- 扫描跨文件时会跳过封存文件尾部的浪费区。

**离线 rewrite / Redis AOF rewrite 类场景**:

1. 停止写入并关闭旧 store,或接受"快照 + 尾部追赶";
2. `scan()` 遍历旧日志,业务层做 compaction(每个 key 只保留最新值、丢弃 tombstone);
3. 写入新的 store / 新 generation 目录,flush/close;
4. 原子切换目录或指针,重新打开;
5. 新日志从 0 开始,CQ/KeyIndex/自定义索引按游标从 0 重建。

> 当前没有内置在线 rewrite/generation 热切换,也没有删头/保留策略。物理 offset 在重写后会改变,所有存物理 offset 的索引都必须重建;消费位点等权威状态必须单独保存。

## 架构与一致性

![mini-file-storage 整体架构](assets/readme/mini-file-storage-architecture.png)

*CommitLog 是唯一真相源;`IndexHost` 驱动可选的 ConsumeQueue / KeyIndex / 自定义索引。查询由门面转发到已启用插件,恢复与扫描细节见下文。*

**写路径**:put 只写 CommitLog;启用索引时,`IndexHost` 在后台线程按帧回调插件。SYNC 模式(默认)等待所有启用索引覆盖本帧;没有索引则立即返回。ASYNC 模式立即返回,读侧用 `awaitIndexed(offset, timeout)` 升级一致性。

**启动路径**:

1. 抢 `baseDir/.store.lock`(`StoreLockHeldException` 拒绝双开);
2. 校验 `store.properties` 与本次启用的索引尺寸;
3. CommitLog 全局前缀恢复:连续文件、残尾清零、损坏截断、后续文件隔离;
4. 只构造启用的索引;每个索引调用 `replayFrom` 声明起点;
5. `IndexHost` 从所有游标的最小值逐帧回放,只回调尚未覆盖该帧的索引;
6. 有索引才启动后台 `mini-index` 线程。

**刷盘顺序**:数据 → 各索引 → checkpoint。`syncFlush` 默认 true,每次 append 后 force 当前数据文件;吞吐优先可设 `syncFlush(false)`,由 `flush()` / `close()` 控制。

**索引游标**:每个索引独立维护进度。CQ 用最后一条 entry 的 `offset + size` 自时钟;KeyIndex 用 `keyindex/cursor` 文件;自定义插件自己做(可用 `IndexCursorFile`)。

## 磁盘格式

| 路径 | 内容 |
|---|---|
| `baseDir/store.properties` | 格式版本、commitLogFileSize、maxBodySize、各索引尺寸;打开时校验本次启用的索引,不一致拒绝 |
| `baseDir/data/%020d` | CommitLog 分段文件,文件名是起始物理 offset |
| `baseDir/data/.quarantined` | 全局前缀截断后不再参与恢复的文件起始 offset 清单 |
| `baseDir/consumequeue/%020d` | ConsumeQueue 分段文件 |
| `baseDir/keyindex/%020d` | KeyIndex 分段文件 |
| `baseDir/keyindex/cursor` | KeyIndex 已覆盖到的日志物理位点(文本) |
| `baseDir/checkpoint.properties` | 88 字节双槽 checkpoint(seq + 4 水位 + CRC) |
| `baseDir/.store.lock` | 进程独占锁文件,内容无意义 |

```text
CommitLog 帧: [magic 4][bodyLen 4][storeTs 8][tagCode 8][keyHash 8][CRC 4][body]
CQ entry:     [physicalOffset 8][size 4][tagCode 8]
KeyIndex:     [keyHash 8][physicalOffset 8]
```

`tagCode` / `keyHash` 是帧头的通用元数据:CQ 用前者做范围查询,KeyIndex 用后者做 key 查询,自定义插件也可以直接使用,不需要解码 body。key 只存 32 位 hash,查询结果是候选集,业务需要二次校验。

## 恢复、持久化与失效语义

- **CommitLog 是唯一真相源**。首文件必须从 offset 0 开始、文件起始偏移连续;非末文件的残尾必须全零。损坏或缺号会在断点做全局截断,后续文件登记进 `.quarantined` 并抹掉首帧 magic;首文件缺失直接拒绝启动。
- **索引是可重建的派生视图**。删除 CQ/KeyIndex 目录后重开,会从 CommitLog 回放重建;单独丢失 KeyIndex 也能重建(它有独立游标文件)。
- **截断是物理的**。恢复/截断会清零被丢弃区域,重启不会复活旧 entry。
- **checkpoint 只做诊断/兼容**。续传以各索引自身游标为准;现有 checkpoint 仍保存 log/CQ/key 水位。
- **单写者**。同一 `baseDir` 只能有一个进程/实例打开。
- **默认同步刷盘**。put 返回前数据文件已 force;索引可以落后,但可从日志重建。

## 限制与已知缺口

- 没有在线 rewrite、generation 热切换、删头/保留策略、TTL 或磁盘水位回收;只有离线 rewrite 的扫描入口。
- 没有消费位点 / 消费组 / ack / 重试 / 事务 / 批量写 / HA;这些属于上层协议或独立 `StateStore`。
- ConsumeQueue 与 KeyIndex 没有 entry 级 CRC;CQ 恢复只做尾部交叉校验,KeyIndex 仍是线性扫描,没有 hash 槽。
- `queryByTagRange` 假设 tagCode 单调(默认写时间戳)。
- 门面方法基本是 `synchronized`,同步刷盘默认开启;高并发/高吞吐场景需要进一步分层或组提交。
- 不是生产级 MQ/Redis,定位是可嵌入的存储内核和学习/实验底座。

## 项目结构与测试

```text
src/main/java/com/ispengya/file/
  api/     FileStore, MiniFileStore, RecordScanner, LogicalIndex, KeyIndexView
  core/    MmapSequentialLog, SimpleMappedFile(Queue), RecordFrame, SequentialLog
  index/   RecordIndex, ConsumeQueueIndex, KeyIndexPlugin, FixedUnitIndexFile,
           IndexEntryCodec, IndexCursorFile, SimpleConsumeQueue, KeyIndexFile
  store/   RecordStore, IndexHost, StoreCheckpoint, StoreMetadata, ReputService(兼容门面)
  codec/   Codec, LogRecordCodec
  model/   LogRecord
src/test/java/com/ispengya/file/
```

```bash
mvn test    # 当前 76 个测试
```

主要测试:

| 测试 | 覆盖 |
|---|---|
| `MiniFileStoreTest` | 门面读写、重启恢复、关闭幂等、默认刷盘 |
| `CommitLogCorruptionTest` / `CommitLogPrefixRecoveryTest` | 撕裂帧、脏尾、全局前缀、隔离 |
| `CommitLogRolloverTest` | 跨文件写入、截断后续写 |
| `OptionalIndexTest` | 纯 WAL、单索引、后加索引、禁用再启用 |
| `CustomIndexTest` | 自定义索引全量回放/增量驱动、重名拒绝 |
| `RecordScanTest` / `IndexFileToolkitTest` | 只读扫描、离线 rewrite、定长索引文件 + 游标 |
| `RebuildInvariantTest` | 派生索引可重建 |
| `ReputServiceTest` / `ReputConsistencyTest` / `ReputCheckpointTest` | 索引宿主/兼容门面、await、checkpoint |
| `StoreMetadataTest` | 配置持久化与不一致拒绝 |
| `FileDemoTest` / `RecordFrameTest` / `IndexCursorTest` | 底层演示、帧编解码、索引截断原语 |

推荐阅读顺序:`MiniFileStore` → `RecordStore` → `IndexHost` / `RecordIndex` → 内置索引插件 → `SimpleMappedFileQueue` → `FixedUnitIndexFile` / `RecordScanner` → 对应测试。

## 与 RocketMQ 的关系

| 维度 | RocketMQ | 本项目 |
|---|---|---|
| 物理日志 | CommitLog 多文件队列 | `MmapSequentialLog` + `SimpleMappedFileQueue` |
| 逻辑索引 | ConsumeQueue | 内置 CQ 插件(单队列) |
| Key 索引 | IndexFile(hash 槽) | 简化 KeyIndex(线性扫描) |
| 索引构建 | Reput 回放 | `IndexHost` 驱动 `RecordIndex` 插件 |
| 恢复 | CommitLog + CQ 校验 + 索引重建 | 全局前缀恢复 + CQ 交叉截断 + 按索引游标回放 |
| 扩展 | Topic/Queue/消费位点 | 自定义 `RecordIndex` + 定长索引工具包;消费位点需独立 StateStore |
| compaction/保留 | 支持 | 仅提供扫描入口,未内置 |
| HA/事务/批量 | 支持 | 未实现 |

可以把它理解为 RocketMQ 存储模型的**最小可运行子集**:保留 CommitLog、逻辑索引、Key 索引、checkpoint 和回放恢复的核心思路,把索引做成可选插件,方便替换成 MQ / KV / 自定义索引。它适合学习、实验和嵌入式基础组件,不适合直接当生产级 MQ/Redis。
