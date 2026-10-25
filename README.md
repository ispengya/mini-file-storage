## mini-file-storage:通用嵌入式顺序记录存储引擎

mini-file-storage 是一个 **零第三方依赖(仅 JDK)** 的迷你顺序存储引擎:在 mmap 分段顺序文件之上封装记录格式、可选的索引插件与 checkpoint,任何中间件都可以直接嵌入作为存储底座——消息日志、事件溯源、审计流水、简易 KV 皆可。

### 功能一览

| 功能 | 门面入口 |
|------|----------|
| 顺序追加写(mmap 分段文件,自动滚动) | `put(T)` |
| 物理 offset 点读 | `get(offset)` |
| 逻辑位点读取(ConsumeQueue 20 字节定长索引,O(1)) | `getByLogicalIndex(n)`(需启用 CQ) |
| 业务 key 索引写入与查询(KeyIndex,hash 候选) | `put(value, key)` / `getByKey(key)`(需启用 KeyIndex) |
| tagCode 范围查询(默认 tag = 写入时刻,可当时间查询) | `queryByTagRange(begin, end)`(需启用 CQ) |
| 同步刷盘(默认开启)+ checkpoint 落盘 | `flush()` / `StoreConfig.syncFlush`(默认 `true`) |
| 帧自校验 | magic + CRC32 覆盖帧头与内容,恢复扫描遇撕裂帧/静默损坏必停,读取抛 `StoreCorruptedException` |
| 脏尾清除 | 恢复扫描停止后,停点之后整段尾部置零落盘 |
| 进程独占锁 | 同一 `baseDir` 双开直接被 `.store.lock` 拒绝(`StoreLockHeldException`) |
| IndexHost 索引宿主 | put 只写 CommitLog;启用的索引插件(CQ/KeyIndex/自定义)由后台线程按帧回放构建 |
| 可选索引装配 | builder 默认纯 WAL;`withConsumeQueue()` / `withKeyIndex()` / `withDefaultIndexes()` / `withIndex(custom)` |
| 自定义索引 SPI | 实现 `RecordIndex`(`replayFrom`/`onFrame`/`progress`/`flush`),启动回放与实时追加共用同一驱动 |
| 一致性分级 | `IndexMode.SYNC`(默认,put 等所有启用索引追平;无索引立即返回)/ `ASYNC`(最终一致 + `awaitIndexed` 显式升级) |
| 重启恢复(CommitLog / ConsumeQueue / KeyIndex 均扫描恢复追加位置) | 相同 `StoreConfig` 重新打开 |
| CommitLog 全局前缀校验 | 首文件从 0、文件连续、非末文件残尾必须全零;损坏/缺号即全局截断并登记 `.quarantined`,`getRecoveryTruncatedOffset()` 可观测 |
| 磁盘格式元数据 | 首次建库写 `store.properties`;文件尺寸/maxBodySize 与磁盘不一致直接拒绝打开,避免 `setLength` 静默截断 |
| 索引可重建不变式 | 每个索引独立游标:删除某个索引目录后重开,该索引全量重建;后加索引从 0 回放,已有索引不重复 |

### 快速开始(门面 API)

```java
StoreConfig config = StoreConfig.builder("./data/demo").build();
FileStore<LogRecord> store = new MiniFileStore<>(config, new LogRecordCodec());

long now = System.currentTimeMillis();
long offset = store.put(new LogRecord(now, "INFO", "hello"));   // 泛型记录,任意业务对象
LogRecord r1 = store.get(offset);                               // 按物理 offset 读
LogRecord r2 = store.getByLogicalIndex(0);                      // 按逻辑位点读
store.put(new LogRecord(now, "INFO", "order-1-body"), "order-1"); // 带业务 key 索引
List<LogRecord> hits = store.getByKey("order-1");
List<LogRecord> recent = store.queryByTagRange(now - 60_000, now);

store.flush();
store.close();
```

对象与字节的互转通过 `Codec<T>` 注入(内置 `LogRecordCodec` 示例)。默认 `SYNC` 模式下 put 返回即索引可见;吞吐优先可用 `indexMode(ASYNC)` + `awaitIndexed` 显式控制一致性(见下文"整体流程图 → 一致性速查")。

持久化默认开启:每次 `put` 在返回前已对数据文件 `force()`(整文件 msync),进程崩溃或掉电不会丢已确认的写入;吞吐优先可 `syncFlush(false)`,改由调用方 `flush()` / `close()` 控制刷盘时机。

首次打开会在 `baseDir` 写 `store.properties`,记录格式版本与所有影响磁盘布局的尺寸;之后用不同的 `commitLogFileSize` / `maxBodySize` 等配置打开会被直接拒绝,避免 `setLength` 静默截断数据。完整行为参考单元测试 `MiniFileStoreTest` / `CommitLogPrefixRecoveryTest` / `CommitLogRolloverTest` / `StoreMetadataTest`。

### 按需装配索引

`new MiniFileStore<>(config, codec)` 保持旧行为(CQ + KeyIndex 全开);新代码可以用 builder 只装需要的索引,或者完全不装:

```java
// 纯 WAL:不建索引目录、不启动索引线程,只有 put / get(offset)
MiniFileStore<LogRecord> wal = MiniFileStore.builder(config, codec).build();

// 只启用 CQ / 只启用 KeyIndex / 全部
MiniFileStore<LogRecord> mq = MiniFileStore.builder(config, codec)
        .withConsumeQueue().build();
MiniFileStore<LogRecord> kv = MiniFileStore.builder(config, codec)
        .withKeyIndex().build();
MiniFileStore<LogRecord> full = MiniFileStore.builder(config, codec)
        .withDefaultIndexes().build();

// 自定义派生索引:启动时从 CommitLog 回放,写入时由内核自动驱动
MiniFileStore<LogRecord> custom = MiniFileStore.builder(config, codec)
        .withIndex(new MemoryKvIndex(codec))
        .build();

// 能力访问:未启用时返回空,不会静默给出错误结果
full.logical().orElseThrow().getByLogicalIndex(0);
full.keys().orElseThrow().getByKey("order-1");
```

- 未启用的能力调用 `getByLogicalIndex` / `getByKey` / `queryByTagRange` 会抛 `IllegalStateException`。
- **后加索引不需要迁移数据**:新索引 `replayFrom()` 返回 0,内核从 CommitLog 全量回放;已有索引按自己的游标跳过重叠区。纯 WAL 期间写入的 tag/keyHash 仍在帧头里,后开 CQ/KeyIndex 可以补建历史索引。
- 自定义索引只需实现 `RecordIndex`;`CustomIndexTest` 中有一个内存 KV 的最小实现,`OptionalIndexTest` 覆盖纯 WAL / 单索引 / 后加内置索引。

自定义索引的契约:

```java
public interface RecordIndex {
    String name();                                             // 唯一名字
    long replayFrom(long logMaxOffset);                        // 新索引返回 0,已有索引返回自己的游标
    void onFrame(long offset, RecordFrame.Frame frame, byte[] body); // 回放与实时追加共用,必须幂等
    long progress();                                           // 自身进度(诊断/持久化)
    void flush();                                              // 先数据后游标
    default void close() {}
}
```

`onFrame` 可能由后台索引线程调用,自定义索引的查询视图需要自行保证并发安全;全局截断后,插件应在 `replayFrom(logMaxOffset)` 中清理越界状态,或让查询视图丢弃 `offset >= logMaxOffset` 的陈旧结果。**不要在 `onFrame` 里回调 `MiniFileStore` 的读写方法**——SYNC put 正持有门面锁等待索引追平,会形成死锁环;需要的数据从传入的 frame/body 取。

以下章节展开磁盘格式与内部机制,用于理解门面背后的实现。本工程最初是一组演示:

- 如何基于 mmap 管理多段顺序文件;
- 如何在物理日志之上封装“记录格式”;
- 如何用一个固定长度的逻辑索引（ConsumeQueue）实现按逻辑 offset 访问;
- 如何用一个简单的 KeyIndex 文件按 key 查询;
- 如何用 checkpoint 记录刷盘位置，并在重启时做最基本的恢复。

---

## 整体流程图（IndexHost 异步模型）

### 写路径:一次 put 的旅程

```mermaid
sequenceDiagram
    autonumber
    participant B as 业务线程
    participant F as MiniFileStore 门面
    participant L as CommitLog(唯一真相源)
    participant R as IndexHost 线程(独立锁)
    participant I as 已启用索引插件(派生视图)
    participant C as Checkpoint(水位)

    B->>F: put(value [, tag | key])
    F->>L: 组帧(storeTs/tagCode/keyHash/CRC) 同步 append
    L-->>F: 帧起始 offset
    F->>R: signal()
    alt SYNC 模式(默认)
        F->>R: awaitCovered(offset) 阻塞等待
        R->>I: 对每个未覆盖该帧的插件 onFrame<br/>(默认 KeyIndex 先、CQ 后)
        R->>R: 各索引独立游标推进 + notifyAll
        R-->>F: 所有索引就绪
        F-->>B: 返回(索引立即可见)
    else ASYNC 模式
        F-->>B: 立即返回(索引最终一致)
        R->>I: 稍后异步回放
        B->>F: 需要强一致时 awaitIndexed(offset, timeout)
    end
    B->>F: flush()
    F->>L: force 数据
    F->>I: force 索引
    F->>C: save(logMax, cqMax, cqCursor, keyCursor)
    Note over F,C: 顺序即协议:水位永远滞后但真实
```

### 启动路径:打开即自愈

```mermaid
flowchart TD
    A["new MiniFileStore(config)"] --> B{"抢 baseDir/.store.lock"}
    B -- 失败 --> X["StoreLockHeldException<br/>(单写者前提,直接拒绝)"]
    B -- 成功 --> M["StoreMetadata 校验<br/>store.properties ↔ 当前配置"]
    M --> C["CommitLog 全局前缀恢复<br/>连续 / 残尾 / 缺号 → 截断 + .quarantined"]
    C --> I["IndexHost 初始化<br/>每个启用索引 replayFrom 自己的起点<br/>(新索引=0,已有索引=持久化游标)"]
    I --> D["同步回放:从最小游标逐帧扫描<br/>只回调尚未覆盖该帧的索引<br/>CQ 尾部撒谎条目由 CQ 插件自检截断"]
    D --> F["catch-up 追平日志合法尾<br/>冷启动补索引 = 崩溃自愈 = 删目录重建 = 后加索引"]
    F --> G["启动 mini-index 后台线程(有索引才启动)"]
    G --> H["可服务"]
```

### 一致性速查

| 读方法 | SYNC(默认) | ASYNC |
|---|---|---|
| `get(offset)` 物理点读 | 强一致 | 强一致(只碰真相源) |
| `getByLogicalIndex` / `getByKey` / `queryByTagRange` | 强一致(put 已等) | 最终一致,`awaitIndexed` 升级 |

> **可重建不变式**(有专门测试):删除 `consumequeue/`、`keyindex/` 与 checkpoint 文件,重开后全部索引查询结果与删除前一致——索引是缓存,CommitLog 才是本体。

---

## 一、文件与协议格式（按组件）

### 1. CommitLog：物理日志文件

- 对应类：
  - [MmapSequentialLog]
  - [SimpleMappedFileQueue]
  - [SimpleMappedFile]
  - [RecordStore]
  - RecordFrame（帧编解码，格式知识的唯一来源）
- 目录与文件名：

| 维度       | 说明                                                                 |
|------------|----------------------------------------------------------------------|
| 目录       | 由 `SequentialLogConfig.storePath` 决定，例如 `./docs/mini-file-storage-data-test` |
| 单文件大小 | `SequentialLogConfig.fileSize`，例如 8MB                            |
| 文件命名   | `SimpleMappedFileQueue.formatFileName` 使用 `%020d` 格式化起始偏移，如 `00000000000000000000` |
| 起始偏移   | 文件名对应的 long 值，即该文件覆盖的 CommitLog 起始物理 offset          |

- 记录格式（RecordStore 视角，**格式 v2**）：

磁盘单元不再是裸 `[length][payload]`，而是**自校验帧**（`core/RecordFrame`，全部大端）：

| 偏移 | 长度 | 字段             | 说明                                          |
|------|------|------------------|-----------------------------------------------|
| 0    | 4    | magic            | `0x484F544B`（"HOTK"），兼作格式版本标识       |
| 4    | 4    | bodyLen          | Codec 编码后的 payload 字节数                  |
| 8    | 8    | storeTimestamp   | 引擎盖章的写入时间（服务端接收语义）            |
| 16   | 8    | tagCode          | 业务 tag，与写入 ConsumeQueue 的值同源          |
| 24   | 8    | keyHash          | 业务 key 的 32 位 hash（0 = 无 key）            |
| 32   | 4    | bodyCRC          | CRC32，覆盖 `magic..keyHash` 与 body，不含自身 |
| 36   | var  | body             | 编码后的业务对象（示例为 LogRecord）            |

帧总长 = 36 + bodyLen。`put(v, key)` 时 keyHash 随帧头落入 CommitLog——索引所需信息全部在日志里，ConsumeQueue / KeyIndex 因此原则上可由日志重建（唯一真相源）。

> ⚠️ v2 与旧版 `[4B length][payload]` 二进制不兼容：旧数据开头读不到 magic，会被视为空洞从头覆盖。升级前清空旧数据目录。

- 单个文件恢复协议（`SimpleMappedFile.recoverRecordStoreWrotePosition`）：

| 步骤 | 条件                                                | 行为                                              |
|------|-----------------------------------------------------|---------------------------------------------------|
| 1    | 从停点起剩余不足 36B，或 magic 不符                  | 判为帧边界终止，停止扫描（NOT_MATCHED）             |
| 2    | `bodyLen` 为负、超过 `maxBodySize` 或超出文件剩余     | 脏长度值，停止扫描（NOT_MATCHED）                   |
| 3    | 重算 CRC 与帧内 bodyCRC 不一致                       | **内容撕裂/静默损坏，停止扫描（CORRUPTED）**        |
| 4    | 全部通过                                             | position 前移 `36 + bodyLen`，继续下一帧            |
| 5    | 扫描结束                                             | 停点即 `wrotePosition` / `flushedPosition`          |
| 6    | 末文件停点后存在非零字节                             | **整段脏尾置零并立即 force**，从断点继续追加         |

- 全局前缀恢复协议（`SimpleMappedFileQueue.recoverForRecordStore`）：

| 条件 | 行为 |
|------|------|
| 首文件不是从偏移 0 开始 | 拒绝启动（前缀起点已丢失，不能当空库覆盖） |
| 文件起始偏移不连续（缺号/多出非法文件） | 在期望起点全局截断，之后文件登记隔离 |
| 非末文件为空，或停点之后存在非零残尾 | 在该文件断点全局截断，之后文件登记隔离 |
| 末文件存在撕裂残尾 | 允许恢复，整段清零后从断点继续写 |
| 隔离落盘 | 之后文件起始偏移写入 `data/.quarantined`，并抹掉首帧 magic（清单丢失时的安全网） |
| 日志增长到隔离文件起点 | 先整文件清零，再从清单移除并复用 |
| 截断发生 | `MiniFileStore.getRecoveryTruncatedOffset()` 返回断点，`-1` 表示日志完整连续 |

> 隔离采用“清单 + 清零”而不是 rename：Windows 上 mmap 未释放时无法重命名/删除数据文件，两种平台语义一致。

- IndexHost 跨文件回放：帧不跨文件，封存文件尾部“装不下下一帧”的浪费区不可读；游标落在该区域时通过 `nextFileStartOffset` 跳到下一个文件起点，不会把零字节当坏帧，也不会卡死（`CommitLogRolloverTest` 覆盖）。

**为什么必须有帧头 CRC**：mmap 写回以 4KB 页为单位、页间无顺序保证，`force()`（≈ msync）同样不保证文件内页序——一条跨页记录在断电后可能"头部页已落、体部页丢失"。v1 只校验 length，面对这种"结构合法的坏数据"会静默越过；v2 用 magic+CRC 把判定从"结构像不像"升级为"内容对不对"，恢复必停在损坏帧之前。同一校验在 `read()` 路径复用，盘上静默损坏（bit rot）也会被立即发现并抛 `StoreCorruptedException`。

---

### 2. LogRecord：业务对象编码格式

- 对应类：
  - [LogRecord]
  - [LogRecordCodec]
- 字段：

| 字段       | 类型    | 含义     |
|------------|---------|----------|
| timestamp  | long    | 时间戳   |
| level      | String  | 日志级别 |
| message    | String  | 日志内容 |

- 编码协议（`LogRecordCodec.encode`）：

| 顺序 | 字段             | 长度（字节）    | 类型                     | 说明                             |
|------|------------------|-----------------|--------------------------|----------------------------------|
| 1    | timestamp        | 8               | long                     | 记录时间                         |
| 2    | levelLength      | 4               | int                      | level 的 UTF-8 字节长度          |
| 3    | levelBytes       | levelLength     | bytes (UTF-8)            | level 内容                       |
| 4    | messageLength    | 4               | int                      | message 的 UTF-8 字节长度        |
| 5    | messageBytes     | messageLength   | bytes (UTF-8)            | message 内容                     |

整个 payload 长度为上述字段总和，对应 `RecordStore` 中的 `payload`。

---

### 3. ConsumeQueue：逻辑消费队列索引

- 对应类：
  - [SimpleConsumeQueue]
  - [SimpleIndexEntry]

- 目录与文件名：

| 维度       | 说明                                                                 |
|------------|----------------------------------------------------------------------|
| 目录       | 由构造函数参数 `storePath` 决定，例如 `./docs/mini-file-storage-cq-test`    |
| 单文件大小 | 由构造函数参数 `mappedFileSize` 决定，运行时会向下对齐为 `CQ_STORE_UNIT_SIZE` 的整数倍 |
| 文件命名   | 与 CommitLog 相同，用 `%020d` 格式化起始“索引文件内偏移”             |
| 起始偏移   | 文件名对应的 long 值，表示该索引文件覆盖的索引字节起始位置           |

- 每条索引 entry 协议（固定 20 字节）：

| 顺序 | 字段           | 长度（字节） | 类型  | 说明                                          |
|------|----------------|--------------|-------|-----------------------------------------------|
| 1    | physicalOffset | 8            | long  | 对应 CommitLog 中记录的起始物理 offset        |
| 2    | size           | 4            | int   | 该记录在 CommitLog 中占用的帧总长（36B 帧头 + body） |
| 3    | tagCode        | 8            | long  | mini 实现中直接存放 timestamp，用于时间查询   |

- 逻辑 offset → 索引映射：

| 逻辑 offset | 对应索引文件内偏移                      |
|-------------|-----------------------------------------|
| `n`         | `offsetInIndexFile = n * CQ_STORE_UNIT_SIZE` |

通过 `SimpleConsumeQueue.get(logicalOffset)`：

1. 计算 `offsetInIndexFile`；
2. 通过 `SimpleMappedFileQueue.findMappedFileByOffset` 找到具体文件；
3. 在文件内按 `[8 offset][4 size][8 tagCode]` 读取一条索引；
4. 若 `physicalOffset < 0` 或 `size <= 0`，视为无效，返回 null。

- 单个索引文件恢复协议（`SimpleMappedFile.recoverConsumeQueueWrotePosition`）：

| 步骤 | 条件                                      | 行为                                                      |
|------|-------------------------------------------|-----------------------------------------------------------|
| 1    | 从文件头开始，每次按 20 字节读取          | 若剩余空间不足 20 字节，则停止                            |
| 2    | 读取 `physicalOffset`、`size`             | 若 `physicalOffset < 0` 或 `size <= 0`，则停止            |
| 3    | 否则                                      | 将 position 前移 20 字节，继续下一条                      |
| 4    | 扫描结束                                  | 将最后一个合法位置设置为 `wrotePosition` 与 `flushedPosition` |

> `ConsumeQueueIndex` 插件以最后一条合法 entry 的 `offset + size` 作为自时钟;目录可删除,删除后 `replayFrom()` 返回 0 全量重建。

---

### 4. KeyIndexFile：按 keyHash 查找物理 offset

- 对应类：
  - [KeyIndexFile]

- 目录与文件名：

| 维度       | 说明                                                                |
|------------|---------------------------------------------------------------------|
| 目录       | 由构造函数参数 `storePath` 决定，例如 `./docs/mini-file-storage-key-index-test` |
| 单文件大小 | 构造函数参数 `fileSize`，例如 1MB                                   |
| 文件命名   | 同样用 `%020d` 表示起始索引字节偏移                                   |

- 每条索引 entry 协议（固定 16 字节）：

| 顺序 | 字段        | 长度（字节） | 类型  | 说明                             |
|------|-------------|--------------|-------|----------------------------------|
| 1    | keyHash     | 8            | long  | 业务 key 的 hash                 |
| 2    | physicalOffset | 8         | long  | 对应 CommitLog 中记录的物理 offset |

- 读取逻辑（`KeyIndexFile.get`）：

| 步骤 | 描述                                           |
|------|------------------------------------------------|
| 1    | 从 logicalIndex = 0 开始，每次偏移 `INDEX_UNIT_SIZE` |
| 2    | 用 `findMappedFileByOffset` 找到所在文件        |
| 3    | 读取一条 `[keyHash][physicalOffset]`           |
| 4    | 如果 storedKeyHash == 目标 keyHash，则加入结果  |
| 5    | 文件队列找不到映射文件时停止扫描               |

这里是最简单的顺序扫全表实现，没有 hash 槽，也没有头部元数据。

> 作为索引插件运行时，同目录还会维护 `cursor` 文件（文本十进制日志偏移），记录已索引到的 CommitLog 位点；`KeyIndexFile` 恢复逻辑会忽略非数字文件。游标缺失、越界或索引为空时从 0 全量回放，重叠区产生的重复条目由查询侧按 offset 去重。
>
> 全局前缀截断后，KeyIndex 可能残留指向截断区的陈旧条目；门面查询会按当前日志合法尾过滤 `offset >= logMaxOffset` 的候选，避免 `offset out of range`。

---

### 5. StoreCheckpoint：水位检查点（v2 双槽原子）

- 对应类：
  - `store.StoreCheckpoint`（独立类；v1 为 RecordStore 内部类，已废弃）

- 文件格式：**两个 44 字节槽交替覆写**（文件固定 88 字节），单槽布局：

| 偏移 | 长度 | 字段          | 说明                                        |
|------|------|---------------|---------------------------------------------|
| 0    | 4    | magic         | `0x43503201`（"CP2"），旧 16B 格式无 magic 视为不可用 |
| 4    | 4    | seq           | 单调递增；save 写"落后槽"，seq = max+1       |
| 8    | 8    | logFlushMax   | CommitLog 已 flush 的最大物理 offset          |
| 16   | 8    | cqFlushMax    | CQ 文件已 flush 字节位点（-1 = 无 CQ）        |
| 24   | 8    | cqCursor      | CQ 插件已覆盖的日志位点（-1 = 未启用）         |
| 32   | 8    | keyCursor     | KeyIndex 插件已覆盖的位点（-1 = 未启用）       |
| 40   | 4    | crc           | 覆盖前 40 字节                                |

- **为什么要双槽**：v1 的 16B 原地覆写切在断电中间会产出"每个字段都合法、组合却是假的"的杂交水位——这也是 v1 时代 checkpoint 不敢被恢复消费的原因。双槽交替保证任何时刻至少一槽完整，`load()` 校验 magic+CRC 后取 seq 大的槽；两槽全坏返回 null，调用方退化为"文件自推导"。
- **消费方**：启动时各索引读取自己的游标（CQ 自时钟 / KeyIndex 的 `cursor` 文件），checkpoint 水位保留用于诊断与兼容，不再作为续传依据；`logFlushMax` 保留给未来的"checkpoint 驱动扫描起点"（当前恢复仍全量扫，属已知成本）。
- 不变式由写入顺序保证：**checkpoint ≤ 索引 ≤ 数据**（flush 按此顺序 force，水位最后写），水位永远"滞后但真实"。

---

## 二、生命周期与调用链路（IndexHost 异步模型）

### 1. 启动阶段（门面模式，`new MiniFileStore<>(config, codec)`）

1. **抢进程锁**：`baseDir/.store.lock` `tryLock`，失败抛 `StoreLockHeldException`——单写者前提从此有人守；
2. **元数据校验**：`store.properties` 与本次启用的索引尺寸/日志尺寸一致，否则拒绝；
3. **CommitLog 恢复**：按全局前缀协议扫描（帧 CRC、残尾清零、损坏截断、后续文件隔离）；
4. **只构造启用的索引**：每个插件 `replayFrom(maxOffset)` 声明续传起点——CQ 用自时钟并截断撒谎尾条目，KeyIndex 读自己的 `cursor` 文件，新索引返回 0；
5. **catch-up 同步回放**：`IndexHost` 从所有游标的最小值逐帧扫描，只回调尚未覆盖该帧的索引——**冷启动补索引、崩溃自愈、删索引目录重建、后加索引共用这一段代码**；
6. 有索引时才启动后台 `mini-index` 线程（daemon）。

> 兼容构造器 `new MiniFileStore<>(config, codec)` 默认启用 CQ + KeyIndex；builder 可只启用部分或完全不启用。

### 2. 写入链路（put 只碰真相源）

1. `put(value[, tag|key])` → `RecordStore.append`：encode body → `RecordFrame.encode` 盖 storeTimestamp/tagCode/keyHash/CRC → `log.append` 写 CommitLog → 返回帧起始 offset；
2. `indexHost.signal()` 唤醒后台线程；
3. **SYNC（默认）**：put 内 `awaitCovered(offset)` 等**所有启用索引**覆盖本帧才返回；没有索引时立即返回；
   **ASYNC**：立即返回，索引读需 `awaitIndexed(offset, timeout)` 显式升级；
4. `IndexHost` 对每帧按插件注册顺序回调 `onFrame`：默认顺序仍是 **KeyIndex → CQ**（KeyIndex 容忍重复，最坏只产生重复条目）；但每个索引有独立游标，不再共享 CQ 时钟；
5. `flush()` 按**数据 → 各索引 → 水位**顺序 force；KeyIndex 先写数据再原子替换 `cursor` 文件。

### 3. 查询链路

- `get(offset)`：CommitLog 帧头 → bodyLen → 整帧 → magic+CRC 校验 → decode。只读真相源，**任何模式强一致**；
- `getByLogicalIndex(n)`：需要 CQ 已启用；未启用抛 `IllegalStateException`；启用后 CQ `n×20` 定位 → 回表；
- `queryByTagRange(b,e)`：需要 CQ 已启用；顺序扫 CQ tagCode，`<b` 跳过、`>e` 截断（依赖 tag 单调写入的前提）；
- `getByKey(key)`：需要 KeyIndex 已启用；全表扫 hash 命中 offset 集合 → **LinkedHashSet 去重** → 逐个回表；语义是候选集，hash 冲突需业务自校验。

### 4. 关闭与重启

- `close()`：`indexHost.stopAndDrain()`（停线程 + 调用线程收尾回放）→ `flushAll()` → 关各索引/日志组件 → 释放进程锁。**ASYNC 下 close 保证不欠账**（有回归测试）；
- 重启：见"启动阶段"六步——同一套扫描+校验+按索引 catch-up,任意断点对恢复都是安全的。

---

## 三、各文件协议一览表（汇总）

| 文件类型        | 单条记录/entry 协议                                                |
|-----------------|---------------------------------------------------------------------|
| CommitLog       | `[36B 帧头(magic/bodyLen/storeTs/tagCode/keyHash/CRC)][body]`        |
| LogRecord payload | `[8 ts][4 levelLen][levelBytes][4 msgLen][msgBytes]`            |
| ConsumeQueue    | `[8 physicalOffset][4 size][8 tagCode]`（size=帧总长）               |
| KeyIndexFile    | `[8 keyHash][8 physicalOffset]`                                     |
| keyindex/cursor | KeyIndex 独立游标（文本十进制物理偏移）；数据先 flush、游标后原子替换 |
| StoreCheckpoint | 88B 双槽文件，每槽 `[magic][seq][4×水位][crc]`（v2 原子交替）        |
| store.properties | 首次建库写入的格式元数据：`format.version / commitLogFileSize / consumeQueueFileSize / keyIndexFileSize / maxBodySize`，启动强校验 |
| data/.quarantined | 全局前缀截断后不再参与恢复的数据文件起始偏移清单（每行一个），复用该偏移时自动移除 |
| .store.lock     | 空文件，仅作进程独占锚（内容无意义，永不删除）                       |

---

## 四、与 RocketMQ 存储的对比与待改进点

本 mini 只实现了 RocketMQ 存储中的“骨架”，很多地方是刻意简化的。下面按能力维度列出对比和可以改进的方向。

### 1. 记录格式与校验

| 维度           | mini 实现                                           | RocketMQ 真实实现（目标方向）                              | 可改进点                                       |
|----------------|-----------------------------------------------------|------------------------------------------------------------|------------------------------------------------|
| CommitLog 条目 | v2 帧头含 magic+storeTs+tagCode+keyHash，CRC 覆盖头体，结构+内容双校验 | 头部包含 magic、crc、flag、时间戳等，严格校验长度和 CRC     | ✅ 已对齐核心思路（flag/主题等字段仍缺，按需再加）              |
| 业务 payload   | LogRecordCodec 自定义结构                           | MessageExtStorage 结构更复杂，含 topic、queueId、flag 等   | 在 mini 中增加 topic、queueId 等，模拟多队列场景 |
| 索引 entry 校验 | ConsumeQueue 只校验 `offset >= 0 && size > 0`       | 还会校验 `offset + size <= commitLogMaxOffset`            | 在恢复时增加与 CommitLog 的交叉校验             |

### 2. 恢复与重建

| 能力         | mini 实现                                                   | RocketMQ 真实实现                                        | 可改进点                                                         |
|--------------|-------------------------------------------------------------|----------------------------------------------------------|------------------------------------------------------------------|
| CommitLog 恢复 | 逐帧 CRC 扫描 + 全局前缀校验：首文件从 0、文件连续、非末文件残尾必须全零；损坏/缺号即整体截断，后续文件登记 `.quarantined`；末文件脏尾整段清零 | 从 checkpoint 起步，按完整消息结构解析，严格截断尾部    | 利用 checkpoint 的 commitLog 偏移缩小扫描范围                    |
| ConsumeQueue 恢复 | 只根据自身结构校验 `[offset][size][tagCode]`             | 额外校验 offset 是否落在 CommitLog 合法范围内          | 增加 `offset + size <= commitLogMaxOffset` 的校验                |
| 索引重建     | 每个索引独立游标:单独删除 KeyIndex 目录（游标随目录消失）→ 从 0 全量重建;CQ 不受影响;后加索引只回放新索引 | 通过 Reput 服务从 CommitLog 回放，自动重建 ConsumeQueue 与 IndexFile | 消费位点等权威状态应走独立 StateStore，不复用派生索引的"可重建"语义 |
| IndexFile 恢复 | 按 16 字节单元顺序扫描恢复追加位置(全零空洞即停);尚无头部元数据与校验 | 读取头部元数据，检查槽位/entry 是否合理，必要时截断或删除 | 为 KeyIndexFile 增加头部和简单的损坏检测策略                    |

### 3. 索引能力

| 维度         | mini 实现                                         | RocketMQ 真实实现                                             | 可改进点                                             |
|--------------|---------------------------------------------------|---------------------------------------------------------------|------------------------------------------------------|
| ConsumeQueue | 单一队列，逻辑 offset = 行号                      | 多 Topic、多 QueueId，按 Topic.QueueId 维护多条 ConsumeQueue | 在 mini 中加入“逻辑队列类型/ID”字段，模拟多队列     |
| Key 索引     | 顺序扫全表，`keyHash -> 多个 physicalOffset`      | 基于 hash 槽 + 冲突链的倒排索引，带时间范围过滤              | 为 KeyIndexFile 增加 slot 区域和简单的 hash 冲突链  |
| 时间查询     | 依赖 ConsumeQueue 中的 tagCode = timestamp        | 结合文件时间、ConsumeQueue、IndexFile 多层过滤               | 在 mini 中模拟按时间段定位 CommitLog 起始文件的逻辑 |

### 4. 刷盘与性能

| 维度           | mini 实现                                  | RocketMQ 真实实现                            | 可改进点                                           |
|----------------|--------------------------------------------|----------------------------------------------|----------------------------------------------------|
| 刷盘策略       | `syncFlush` 默认 true(每条 append 后 force);关闭后由调用方 `flush()` 控制，无后台刷盘线程 | 同步/异步刷盘，单独的 flush/commit 线程      | 将 flush 抽成单独线程，模拟高吞吐异步 flush 情况  |
| Page Cache 利用 | 使用 mmap，简化版实现                      | 同样大量使用 mmap 和 Page Cache              | 增加写入压力测试，观察 flush 策略对延迟的影响       |
| 零拷贝发送     | 未实现                                     | 发送时使用 FileRegion/sendfile 等            | 在 mini 的“读取/发送”路径中模拟直接从文件到 socket |

---

## 五、如何阅读和扩展本 mini

建议的阅读顺序：

1. 看协议表格（本 README 第一、三部分），脑中先有“磁盘上到底长什么样”；
2. 看核心类：
   - [SimpleMappedFileQueue]
   - [RecordStore]
   - [IndexHost] / [RecordIndex]（索引宿主与插件 SPI）
   - [ConsumeQueueIndex] / [KeyIndexPlugin]
   - [MiniFileStore]（可选索引装配与能力视图）
3. 跑一遍单测 [FileDemoTest]、[OptionalIndexTest]、[CustomIndexTest]，尤其是：
   - 纯 WAL / 单索引 / 双索引装配；
   - 自定义索引的启动全量回放与写入增量驱动；
   - 物理/逻辑 offset 读取；
   - 时间范围查询；
   - key 查询；
   - 重启恢复场景。
4. 对照"待改进点"表格动手增强，当前状态一览：
   - ~~为记录增加 CRC 和魔数~~（已实现：帧格式 v2）；
   - ~~Reput 回放重建索引~~（已实现：`IndexHost` + 每索引独立游标，删索引目录可重建、后加索引可回放）；
   - ~~可选索引 / 自定义索引 SPI~~（已实现：`RecordIndex` + builder 装配）；
   - ~~Checkpoint 双槽原子 + 被启动流程消费~~（已实现：v2 四水位；剩余——用 `logFlushMax` 缩小数据扫描起点）；
   - 待做：ConsumeQueue 恢复时的帧级 CRC 全量交叉校验（当前只做尾部截断）；
   - 待做：KeyIndexFile hash 槽结构（替代线性扫描）；
   - 待做：独立 FlushService 异步刷盘线程、过期文件删除与磁盘水位、munmap。

这样，这个 mini 就不仅仅是“能跑的 demo”，而是你可以不断对标 RocketMQ 存储实现、逐步演进的实验场。

> 历史备注：本节曾整段描述"未来的 Checkpoint/Reput 服务应该怎么做"。该设计已在
> `store/IndexHost` + `store/StoreCheckpoint(v2)` 落地；`ReputService` 现为兼容门面。
> 启动生命周期与写入顺序见"整体流程图"与"二、生命周期与调用链路"两节。

---

## 四、RocketMQ 与 mini-file-storage mini 对比表

下面用一个表格对比 RocketMQ 真实实现和本 demo mini 实现的关系，方便对号入座：

| 维度/特性                  | RocketMQ                                                   | mini-file-storage mini 实现                                                                |
|---------------------------|------------------------------------------------------------|------------------------------------------------------------------------------------|
| 物理存储                  | CommitLog 多文件队列（MappedFileQueue）                   | `MmapSequentialLog` + `SimpleMappedFileQueue`                                      |
| 单文件管理                | 固定大小，文件名为起始 offset，滚动创建                   | 同样：固定大小文件，文件名为起始 offset，滚动创建                                  |
| 消息物理格式              | 头 + 体，包含多种字段（魔数、CRC、主题、队列 ID 等）      | v2 帧头：magic + storeTs + tagCode + keyHash + CRC + body                          |
| 多 Topic/Queue 支持       | 所有消息写同一 CommitLog；按 Topic.Queue 维护 ConsumeQueue | 只做“单队列”模型，但 ConsumeQueue 结构与 RocketMQ 类似                            |
| 逻辑队列索引结构          | ConsumeQueue：20 字节 `[offset][size][tagCode]`           | `SimpleConsumeQueue`：20 字节结构完全对齐                                          |
| 按 Topic.Queue + offset 读 | 逻辑 offset → ConsumeQueue → CommitLog                    | `readByLogicalOffset` → `SimpleConsumeQueue` → `RecordStore.read`                  |
| 按时间查询                | 结合 CommitLog 时间戳、文件名、tagCode/IndexFile          | 使用 `SimpleConsumeQueue` 的 tagCode 存时间戳，`queryByTimeRange` 顺序过滤         |
| 按 key 查询               | IndexFile：hash 槽 + index entry 结构                     | 简化 `KeyIndexFile`：顺序存储 `[keyHash][physicalOffset]`，线性扫描                |
| 刷盘策略                  | 同步/异步刷盘，多线程 flush/commit                        | `flush()` 数据→索引→水位顺序 force，支持配置 syncFlush；独立 FlushService 未做     |
| 索引构建                  | put 只写 CommitLog，IndexHost 线程回放构建已启用索引 | ✅ 同模型：put 只写日志，`IndexHost` 按帧驱动 CQ/KeyIndex/自定义插件；SYNC/ASYNC 一致性分级 |
| 宕机恢复                  | CommitLog 扫描 + ConsumeQueue 校验/截断 + IndexHost 重建索引 | 帧协议扫描(含全局前缀/脏尾) + CQ 交叉校验截断 + 按索引游标 catch-up，删索引目录/后加索引可重建 |
| Checkpoint                | 独立 Checkpoint 文件记录多组件刷盘时间                    | v2 双槽原子(4水位+seq+CRC)；续传以各索引自身游标为准，checkpoint 保留诊断/兼容，logFlushMax 待用于缩小扫描范围 |
| 磁盘空间回收              | 定时删除过期 CommitLog/ConsumeQueue/IndexFile             | 未实现，只关注写入/读取和索引逻辑                                                  |
| HA 与多副本               | 主从同步 CommitLog，支持高可用                            | 未实现                                                                             |
| 事务、重试、顺序消息等    | 上层协议和存储配合实现                                    | 未实现，聚焦底层存储原理                                                          |
| 实现复杂度                | 完整生产级实现，代码量大                                  | 核心逻辑浓缩到少量类，适合学习和实验                                               |

可以简单理解为：**mini-file-storage 把 RocketMQ 存储里的“CommitLog + ConsumeQueue + KeyIndex + Checkpoint”抽出一个最小可运行子集，用来演示顺序写、索引和恢复的基本思路**。

---

## 五、Demo 入口与运行方式

### 1. Demo 入口（手动运行）

- 行为：
  - 构造 `SequentialLogConfig` 和 `MmapSequentialLog`；
  - 构造 `RecordStore<LogRecord>` 和 `LogRecordCodec`；
  - 写入若干条 `LogRecord`，记录每条的起始物理 offset；
  - 使用 `SimpleConsumeQueue` 维护逻辑 offset 索引；
  - 可选地使用 `KeyIndexFile` 维护 key → offset 索引；
  - 调用 `store.flush()` 和 `keyIndexFile.flush()` 刷盘；
  - 再根据物理 offset、逻辑 offset、时间范围、key 等方式读回，并打印到控制台。

推荐直接通过门面 `MiniFileStore`（见开头“快速开始”）走通以上全部行为，参考单元测试
`com.ispengya.file.MiniFileStoreTest`。

### 2. 单元测试（推荐阅读）

- 测试类：`com.ispengya.file.FileDemoTest` 与门面测试 `com.ispengya.file.MiniFileStoreTest`
- 所在目录：`src/test/java/com/ispengya/file/`
- 覆盖内容包括：
  - `testAppendAndReadByPhysicalOffset`：验证物理 offset 读写；
  - `testReadByLogicalOffset`：验证通过 ConsumeQueue 的逻辑 offset 读取；
  - `testQueryByTimeRange`：验证按时间范围查询；
  - `testKeyIndexQuery`：验证按 key 查询；
  - `testCheckpointWrittenOnFlush`：验证 flush 时 checkpoint 正确写入；
  - `testRecoverCommitLogAfterRestart`：验证 CommitLog 基于文件扫描的启动恢复；
  - `testRecoverConsumeQueueAfterRestart`：验证 ConsumeQueue 启动恢复后在旧索引后继续追加并按逻辑 offset 读取；
  - `MiniFileStoreTest#restartRecoveryKeepsAllIndexTypes`：验证三类文件(数据/CQ/KeyIndex)重启后均可继续追加与读取。
- 可靠性与异步测试（本次重构新增）：
  - `CommitLogCorruptionTest`：撕裂帧恢复停位 / 读损坏抛错 / 双开拒绝 / 脏尾清零；
  - `RebuildInvariantTest`：**删除 CQ/KeyIndex 目录与 checkpoint 后重开，全部索引查询自动重建一致**（唯一真相源契约）；
  - `ReputServiceTest` / `ReputConsistencyTest`：回放顺序、awaitCovered 语义、close drain 不欠账、getByKey 去重；
  - `OptionalIndexTest` / `CustomIndexTest`：纯 WAL / 单索引 / 后加内置索引 / 自定义索引全量回放与增量驱动；
  - `ReputCheckpointTest` / `RecordFrameTest`：双槽原子、帧编解码三态判定。

在模块根目录执行：

```bash
cd mini-file-storage
mvn test
```

可以一次性跑完所有测试，帮助你从测试用例的角度理解整个 mini 存储引擎的行为。  

---

## 六、整体流程调用图（循序渐进）

这一节用纯文本画出几条关键调用链，并给出一个推荐的阅读顺序，方便从“概念 → mini 设计 → 实际调用路径 → 恢复流程”循序渐进地理解。

### 1. 推荐阅读顺序

- 第一步：先阅读“RocketMQ 底层存储整体设计概览”，掌握 CommitLog / ConsumeQueue / IndexFile / Checkpoint / Reput 的整体关系；
- 第二步：再看“mini-file-storage mini 存储引擎设计”，对照 RocketMQ 概念理解 `MmapSequentialLog`、`RecordStore`、`SimpleConsumeQueue`、`KeyIndexFile` 各自的职责；
- 第三步：结合下面的“写入 / 读取 / 启动恢复 / 重放”流程图，建立模块之间的调用图像；
- 第四步：最后对照 `FileDemoTest` / `MiniFileStoreTest` / `CommitLogCorruptionTest`，从测试验证自己对流程与损坏语义的理解。

### 2. 写入流程（put 只写真相源，索引归 IndexHost）

```text
业务线程（SYNC 模式在此等待索引）
  └─ MiniFileStore.put(value[, tag | key])
       ├─ RecordStore.append(value, tagCode, keyHash)
       │    ├─ Codec.encode(value)                    （对象 → body）
       │    ├─ RecordFrame.encode 组帧                （盖 storeTs/tagCode/keyHash + CRC）
       │    └─ MmapSequentialLog.append(frameBytes)   （写 CommitLog，满则滚动）
       ├─ indexHost.signal()                          （唤醒后台，put 关键路径到此为止）
       └─ SYNC: indexHost.awaitCovered(offset)  ← ─ ─ ─ ┐ 两把锁不嵌套，无死锁
                                                         │
IndexHost 线程（mini-index，独立 monitor）  ─ ─ ─ ─ ─ ─ ┘
  └─ 从 min(cursors) 起逐帧回放 log 区间 [min, maxOffset)
       ├─ RecordFrame.parse 校验 magic+CRC（坏帧停点，绝不越过）
       ├─ 对每个尚未覆盖该帧的插件调用 onFrame(offset, frame, body)
       │    ├─ KeyIndexPlugin：keyHash != 0 → put(keyHash, offset)   （默认先注册）
       │    └─ ConsumeQueueIndex：append(offset, 帧总长, tagCode)     （默认后注册）
       └─ 各插件独立游标推进到帧尾; lock.notifyAll()
```

刷盘/Checkpoint（有序落盘协议）：

```text
MiniFileStore.flush()/close()
  ├─ recordStore.flush()   → log.force                    ① 数据
  ├─ 每个启用索引 index.flush()                            ② 索引
  │    ├─ KeyIndexPlugin：keyIndex.flush() → 原子写 cursor 文件
  │    └─ ConsumeQueueIndex：cq.flush()
  └─ checkpoint.save(logMax, cqMax, cqCursor, keyCursor)   ③ 水位最后
       → 不变式 checkpoint ≤ 索引 ≤ 数据：水位只可能滞后，绝不超前撒谎
```

### 3. 读取流程（按物理 offset / 逻辑 offset / 时间 / key）

按物理 offset 读取一条记录：

```text
业务代码
  └─ RecordStore.read(offset)
       ├─ log.read(offset, 36)             读帧头，取 bodyLen
       ├─ log.read(offset, 36 + bodyLen)   读整帧
       ├─ RecordFrame.parse                magic + CRC 校验（失败抛 StoreCorruptedException）
       └─ codec.decode(body)               反序列化为业务对象
```

按逻辑 offset 读取（通过 ConsumeQueue 索引）：

```text
业务代码
  └─ RecordStore.readByLogicalOffset(logicalOffset)
       ├─ SimpleConsumeQueue.get(logicalOffset)
       │    └─ 计算 indexPos = logicalOffset * 20
       │    ├─ SimpleMappedFileQueue.findMappedFileByOffset(indexPos)
       │    ├─ SimpleMappedFile.read(...) 读取 20B
       │    └─ 解析出 physicalOffset / size / tagCode
       └─ RecordStore.read(entry.physicalOffset)         （回到物理读取）
```

按时间范围查询（利用 tagCode 存时间戳）：

```text
业务代码
  └─ RecordStore.queryByTimeRange(beginTs, endTs)
       ├─ 从 logicalOffset = 0 开始循环：
       │    ├─ SimpleConsumeQueue.get(logicalOffset)
       │    ├─ 取出 tagCode 作为时间戳
       │    ├─ 若 ts < beginTs: logicalOffset++，继续
       │    ├─ 若 ts > endTs:  break
       │    └─ 命中范围: RecordStore.read(physicalOffset)，加入结果列表
       └─ 返回所有命中的业务记录
```

按 key 查询（通过 KeyIndexFile）：

```text
业务代码
  └─ KeyIndexFile.get(keyHash)
       ├─ 从 logicalIndex = 0 开始
       │    ├─ 计算 offsetInIndexFile = logicalIndex * 16
       │    ├─ SimpleMappedFileQueue.findMappedFileByOffset(offsetInIndexFile)
       │    ├─ SimpleMappedFile.read(...) 读取 16B
       │    ├─ 解析 storedKeyHash / physicalOffset
       │    ├─ 若 storedKeyHash == keyHash: 记录 physicalOffset
       │    └─ logicalIndex++，继续
       └─ 返回所有 candidate 物理 offset

业务代码
  └─ 对每个 candidate offset 调用 RecordStore.read(offset) 做最后校验
```

### 4. 启动生命周期（IndexHost 模型，门面模式）

```text
MiniFileStore.builder(config, codec) / new MiniFileStore(config, codec)
  ├─ ① 抢进程锁 baseDir/.store.lock
  │    └─ tryLock==null / 同 JVM 重叠 → StoreLockHeldException（装配失败则释放锁再抛）
  ├─ ② 校验 store.properties（只校验本次启用的索引尺寸）
  ├─ ③ CommitLog 全局前缀恢复
  │    └─ 帧协议扫描 + 停点后整段尾部清零；损坏/缺号 → 截断 + data/.quarantined
  ├─ ④ 构造启用的索引（CQ / KeyIndex / 自定义）
  │    ├─ ConsumeQueueIndex.replayFrom：cq.truncateBeyond(maxOffset)，返回自时钟
  │    ├─ KeyIndexPlugin.replayFrom：读 keyindex/cursor，缺失/为空/越界则从 0
  │    └─ 自定义索引：返回 0(全量) 或自己的持久化游标
  ├─ ⑤ IndexHost 从 min(cursors) 同步回放到日志合法尾
  │    └─ 同一路径覆盖：冷启动补索引 / 崩溃自愈 / 删索引目录重建 / 后加索引
  └─ ⑥ 有索引才启动 daemon 线程 mini-index 持续追尾
```

close 顺序与⑤呼应：`stopAndDrain`（停线程 + 调用线程收尾回放）→ 有序落盘 → 关索引/日志 → 放锁，
保证 ASYNC 模式下"close 之后无欠账"——跨重启的索引读因此永远完整（ReputConsistencyTest 有回归用例）。

历史注：v1 时代本节只有"扫描校正 wrotePosition"三步，索引一致性靠 put 内联双写硬扛；
现在"追平"成为一等公民，恢复、重建、补账统一为按索引游标的 catch-up。
