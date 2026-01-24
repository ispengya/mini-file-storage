## mini-file-storage：mini 顺序存储引擎说明

mini-file-storage 是一个极简版的“顺序存储引擎”示例，用来演示：

- 如何基于 mmap 管理多段顺序文件；
- 如何在物理日志之上封装“记录格式”；
- 如何用一个固定长度的逻辑索引（ConsumeQueue）实现按逻辑 offset 访问；
- 如何用一个简单的 KeyIndex 文件按 key 查询；
- 如何用 checkpoint 记录刷盘位置，并在重启时做最基本的恢复。

---

## 一、文件与协议格式（按组件）

### 1. CommitLog：物理日志文件

- 对应类：
  - [MmapSequentialLog]
  - [SimpleMappedFileQueue]
  - [SimpleMappedFile]
  - [RecordStore]
- 目录与文件名：

| 维度       | 说明                                                                 |
|------------|----------------------------------------------------------------------|
| 目录       | 由 `SequentialLogConfig.storePath` 决定，例如 `./docs/mini-file-storage-data-test` |
| 单文件大小 | `SequentialLogConfig.fileSize`，例如 8MB                            |
| 文件命名   | `SimpleMappedFileQueue.formatFileName` 使用 `%020d` 格式化起始偏移，如 `00000000000000000000` |
| 起始偏移   | 文件名对应的 long 值，即该文件覆盖的 CommitLog 起始物理 offset          |

- 记录格式（RecordStore 视角）：

| 顺序 | 字段            | 长度（字节） | 类型   | 说明                         |
|------|-----------------|--------------|--------|------------------------------|
| 1    | bodyLength      | 4            | int    | 后续 payload 的长度          |
| 2    | payload         | bodyLength   | bytes  | 编码后的业务对象（LogRecord） |

`RecordStore.append` 会构造上述 `[length][payload]`，整体视为一条“记录”写入日志。

- 单个文件恢复协议（`SimpleMappedFile.recoverRecordStoreWrotePosition`）：

| 步骤 | 条件                                      | 行为                                            |
|------|-------------------------------------------|-------------------------------------------------|
| 1    | 从文件头开始，读取 4 字节 length          | 如果读不到完整的 4 字节，则停止                 |
| 2    | 若 `length <= 0`                          | 判定为无效，停止扫描                            |
| 3    | 若 `当前位置 + 4 + length > fileSize`     | 判定 payload 越界，停止扫描                     |
| 4    | 否则                                      | 将 position 前移 `4 + length`，继续下一条记录   |
| 5    | 扫描结束                                  | 将最后一个合法位置设置为 `wrotePosition` 与 `flushedPosition` |

这样能够在宕机后，从每个 CommitLog 文件中找回“最后一条完整记录结束的位置”。

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
93→| 目录       | 由构造函数参数 `storePath` 决定，例如 `./docs/mini-file-storage-cq-test`    |
94→| 单文件大小 | 由构造函数参数 `mappedFileSize` 决定，运行时会向下对齐为 `CQ_STORE_UNIT_SIZE` 的整数倍 |
| 文件命名   | 与 CommitLog 相同，用 `%020d` 格式化起始“索引文件内偏移”             |
| 起始偏移   | 文件名对应的 long 值，表示该索引文件覆盖的索引字节起始位置           |

- 每条索引 entry 协议（固定 20 字节）：

| 顺序 | 字段           | 长度（字节） | 类型  | 说明                                          |
|------|----------------|--------------|-------|-----------------------------------------------|
| 1    | physicalOffset | 8            | long  | 对应 CommitLog 中记录的起始物理 offset        |
| 2    | size           | 4            | int   | 该记录在 CommitLog 中占用的总字节数（含 length 头） |
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

---

### 5. StoreCheckpoint：刷盘检查点

- 对应类：
  - `RecordStore.StoreCheckpoint`（内部静态类）

- 文件格式：

| 顺序 | 字段                 | 长度（字节） | 类型  | 说明                                        |
|------|----------------------|--------------|-------|---------------------------------------------|
| 1    | commitLogMaxOffset   | 8            | long  | 最近一次 flush 后 CommitLog 的最大物理 offset |
| 2    | consumeQueueMaxOffset| 8            | long  | 最近一次 flush 后 ConsumeQueue 的最大索引字节 offset |

写入流程（`save`）：

1. 确保目录存在；
2. 分配 16 字节 ByteBuffer；
3. 顺序写入两个 long；
4. `RandomAccessFile` 设置文件长度为 16 字节，写入并 `force(true)`。

读取流程（`load`）：

1. 文件不存在或长度小于 16 字节，返回 null；
2. 读取 16 字节并解析两个 long，返回 `[commitLogMaxOffset, consumeQueueMaxOffset]`。

---

## 二、启动调用链路（按生命周期）

下面按“启动 → 写入 → 查询 → 关闭/重启”的顺序描述主要调用链。

### 1. 启动阶段

典型构造过程（以 `FileDemoTest.setUp` 为例）：

1. 创建顺序日志：
   - `SequentialLogConfig config = new SequentialLogConfig(dataDir, fileSize, syncFlush);`
   - `SequentialLog log = new MmapSequentialLog(config);`
   - 内部调用：
     - `SimpleMappedFileQueue.recoverForRecordStore(storePath, fileSize)` 扫描已有 CommitLog 文件；
     - 每个文件调用 `recoverRecordStoreWrotePosition` 计算有效 wrotePosition。
2. 创建 ConsumeQueue：
   - 首次启动：`new SimpleConsumeQueue(cqDir, fileSize);`（空目录）；
   - 重启恢复：`new SimpleConsumeQueue(cqDir, fileSize, true);`
     - 内部调用 `SimpleMappedFileQueue.recoverForConsumeQueue`；
     - 每个索引文件调用 `recoverConsumeQueueWrotePosition` 找到有效索引尾部。
3. 创建 KeyIndexFile（可选）：
   - `KeyIndexFile keyIndex = new KeyIndexFile(keyDir, fileSize);`
   - 当前实现启动时不做恢复，只是继续在文件尾部顺序追加。
4. 创建 checkpoint：
   - `StoreCheckpoint checkpoint = new StoreCheckpoint(checkpointPath);`
5. 构造 RecordStore：
   - `RecordStore<LogRecord> store = new RecordStore<>(log, new LogRecordCodec(), consumeQueue, checkpoint);`

### 2. 写入链路

以写入一条 LogRecord 为例：

1. 业务构造 `LogRecord(ts, level, msg)`；
2. 调用 `store.append(record, tagCode)`，其中 mini 中通常让 `tagCode = timestamp`；
3. `RecordStore.append`：
   - `LogRecordCodec.encode` 生成 payload；
   - 构造 `[length][payload]`；
   - 调用 `log.append(recordBytes)` 写入 CommitLog；
   - 拿到写入完成后的 `maxOffset`，计算该记录的 `recordOffset = maxOffset - recordBytes.length`；
   - 调用 `consumeQueue.append(recordOffset, recordBytes.length, tagCode)` 生成逻辑索引；
4. 调用 `store.flush()`：
   - `log.flush()`：
     - `SimpleMappedFileQueue.flush()` 遍历所有文件并 `force()`；
   - `consumeQueue.flush()`：
     - `SimpleMappedFileQueue.flush()` 刷索引文件；
   - `checkpoint.save(commitLogMaxOffset, consumeQueueMaxOffset)` 记录刷盘位置。

### 3. 查询链路

- 按物理 offset 查询（`testAppendAndReadByPhysicalOffset`）：

1. 业务持有物理 offset（`recordOffset`）；
2. 调用 `store.read(recordOffset)`：
   - 从 CommitLog 读取 4 字节 length；
   - 再读取 payload；
   - 通过 `LogRecordCodec.decode` 还原 `LogRecord`。

- 按逻辑 offset 查询（`testReadByLogicalOffset`）：

1. 业务只关心“第 N 条消息”，传入 `logicalOffset = N`；
2. 调用 `store.readByLogicalOffset(logicalOffset)`：
   - `SimpleConsumeQueue.get(logicalOffset)` 读取一条索引；
   - 得到 `physicalOffset` 和 `size`；
   - 再调用 `read(physicalOffset)` 读取真实记录。

- 按时间范围查询（`testQueryByTimeRange`）：

1. 确定 `beginTs`、`endTs`；
2. 调用 `store.queryByTimeRange(beginTs, endTs)`：
   - 从逻辑 offset 0 开始顺序遍历 ConsumeQueue：
     - 读取每条索引中的 `tagCode`（这里就是 timestamp）；
     - 如果 < beginTs，跳过继续；
     - 如果 > endTs，停止扫描；
     - 命中范围则根据 `physicalOffset` 读取对应记录并加入结果列表。

- 按 key 查询（`testKeyIndexQuery`）：

1. 写入时，业务将 key 的 hash 与物理 offset 写入 `KeyIndexFile`：
   - `long keyHash = key.hashCode() & 0xffffffffL;`
   - `keyIndexFile.put(keyHash, physicalOffset);`
2. 查询时，调用 `keyIndexFile.get(keyHash)`：
   - 顺序扫所有索引条目，收集 `storedKeyHash == keyHash` 的 `physicalOffset`；
   - 对每个 offset 调用 `store.read(offset)`，并校验 message 是否匹配。

### 4. 关闭与重启

- 关闭（`RecordStore.close`）：
  - 调用 `flush()`，确保 CommitLog 和 ConsumeQueue 以及 checkpoint 都刷盘；
  - 关闭顺序日志和 ConsumeQueue。

- 重启：
  - `MmapSequentialLog` 重建文件队列并按 `[length][payload]` 扫描校正 wrotePosition；
  - `SimpleConsumeQueue(..., true)` 重建索引文件队列并按 `[offset][size][tagCode]` 扫描校正 wrotePosition；
  - 新写入会从这些有效区域的尾部继续 append，不覆盖旧数据。

---

## 三、各文件协议一览表（汇总）

| 文件类型        | 单条记录/entry 协议                                                |
|-----------------|---------------------------------------------------------------------|
| CommitLog       | `[4 字节 length][payload (LogRecord 编码)]`                        |
| LogRecord payload | `[8 ts][4 levelLen][levelBytes][4 msgLen][msgBytes]`            |
| ConsumeQueue    | `[8 physicalOffset][4 size][8 tagCode]`                            |
| KeyIndexFile    | `[8 keyHash][8 physicalOffset]`                                     |
| StoreCheckpoint | `[8 commitLogMaxOffset][8 consumeQueueMaxOffset]`                  |

---

## 四、与 RocketMQ 存储的对比与待改进点

本 mini 只实现了 RocketMQ 存储中的“骨架”，很多地方是刻意简化的。下面按能力维度列出对比和可以改进的方向。

### 1. 记录格式与校验

| 维度           | mini 实现                                           | RocketMQ 真实实现（目标方向）                              | 可改进点                                       |
|----------------|-----------------------------------------------------|------------------------------------------------------------|------------------------------------------------|
| CommitLog 条目 | `[length][payload]`，只校验 length 合法性            | 头部包含 magic、crc、flag、时间戳等，严格校验长度和 CRC     | 为记录增加魔数和 CRC，增强误判保护              |
| 业务 payload   | LogRecordCodec 自定义结构                           | MessageExtStorage 结构更复杂，含 topic、queueId、flag 等   | 在 mini 中增加 topic、queueId 等，模拟多队列场景 |
| 索引 entry 校验 | ConsumeQueue 只校验 `offset >= 0 && size > 0`       | 还会校验 `offset + size <= commitLogMaxOffset`            | 在恢复时增加与 CommitLog 的交叉校验             |

### 2. 恢复与重建

| 能力         | mini 实现                                                   | RocketMQ 真实实现                                        | 可改进点                                                         |
|--------------|-------------------------------------------------------------|----------------------------------------------------------|------------------------------------------------------------------|
| CommitLog 恢复 | 逐文件扫描 `[length][payload]`，遇到非法长度停止            | 从 checkpoint 起步，按完整消息结构解析，严格截断尾部    | 利用 checkpoint 的 commitLog 偏移缩小扫描范围                    |
| ConsumeQueue 恢复 | 只根据自身结构校验 `[offset][size][tagCode]`             | 额外校验 offset 是否落在 CommitLog 合法范围内          | 增加 `offset + size <= commitLogMaxOffset` 的校验                |
| 索引重建     | 无自动重建逻辑，业务可以手工重跑写入代码                  | 通过 Reput 服务从 CommitLog 回放，自动重建 ConsumeQueue 与 IndexFile | 在 mini 中增加一个简单的“重放线程/方法”，按 CommitLog 顺序重放   |
| IndexFile 恢复 | 启动时不做任何校验，认为 index 文件是“可有可无”的        | 读取头部元数据，检查槽位/entry 是否合理，必要时截断或删除 | 为 KeyIndexFile 增加头部和简单的损坏检测策略                    |

### 3. 索引能力

| 维度         | mini 实现                                         | RocketMQ 真实实现                                             | 可改进点                                             |
|--------------|---------------------------------------------------|---------------------------------------------------------------|------------------------------------------------------|
| ConsumeQueue | 单一队列，逻辑 offset = 行号                      | 多 Topic、多 QueueId，按 Topic.QueueId 维护多条 ConsumeQueue | 在 mini 中加入“逻辑队列类型/ID”字段，模拟多队列     |
| Key 索引     | 顺序扫全表，`keyHash -> 多个 physicalOffset`      | 基于 hash 槽 + 冲突链的倒排索引，带时间范围过滤              | 为 KeyIndexFile 增加 slot 区域和简单的 hash 冲突链  |
| 时间查询     | 依赖 ConsumeQueue 中的 tagCode = timestamp        | 结合文件时间、ConsumeQueue、IndexFile 多层过滤               | 在 mini 中模拟按时间段定位 CommitLog 起始文件的逻辑 |

### 4. 刷盘与性能

| 维度           | mini 实现                                  | RocketMQ 真实实现                            | 可改进点                                           |
|----------------|--------------------------------------------|----------------------------------------------|----------------------------------------------------|
| 刷盘策略       | `SequentialLogConfig.isSyncFlush`，要么同步要么简单异步 | 同步/异步刷盘，单独的 flush/commit 线程      | 将 flush 抽成单独线程，模拟高吞吐异步 flush 情况  |
| Page Cache 利用 | 使用 mmap，简化版实现                      | 同样大量使用 mmap 和 Page Cache              | 增加写入压力测试，观察 flush 策略对延迟的影响       |
| 零拷贝发送     | 未实现                                     | 发送时使用 FileRegion/sendfile 等            | 在 mini 的“读取/发送”路径中模拟直接从文件到 socket |

---

## 五、如何阅读和扩展本 mini

建议的阅读顺序：

1. 看协议表格（本 README 第一、三部分），脑中先有“磁盘上到底长什么样”；
2. 看核心类：
   - [SimpleMappedFileQueue]
   - [RecordStore]
   - [SimpleConsumeQueue]
   - [KeyIndexFile]
3. 跑一遍单测 [FileDemoTest]，尤其是：
   - 物理/逻辑 offset 读取；
   - 时间范围查询；
   - key 查询；
   - 重启恢复场景。
4. 按前面“待改进点”的表格，挑一两项动手增强 mini，例如：
   - 为记录增加 CRC 和魔数；
   - 为 ConsumeQueue 增加与 CommitLog 的一致性校验；
   - 为 KeyIndexFile 增加简单的 hash 槽结构；
   - 加一个真正的“从 checkpoint 起点往后重放”的重建方法。

这样，这个 mini 就不仅仅是“能跑的 demo”，而是你可以不断对标 RocketMQ 存储实现、逐步演进的实验场。
  - mini 的改进点：
    - 可以参考 RocketMQ，对 KeyIndexFile 增加简单的头部/长度校验和启动截断逻辑；
    - 在未来若实现简易 Reput，也可以顺带重放生成 KeyIndex 索引。

- Checkpoint / Reput 服务
  - RocketMQ 的恢复策略：
    - 独立的 Checkpoint 文件记录 CommitLog/ConsumeQueue/IndexFile 的刷盘时间戳；
    - 启动时先恢复 Checkpoint，再根据其中的时间戳决定：
      - 从哪个 CommitLog 文件开始扫描；
      - 从哪里开始向 ConsumeQueue/IndexFile 做重放；
    - Reput 服务常驻，负责将 CommitLog 新写入的数据“回放”到 ConsumeQueue 和 IndexFile 中。
  - RocketMQ 的重建策略：
    - 如果某一类索引文件被删除，只要 Checkpoint 和 CommitLog 还在，就能通过 Reput 从 Checkpoint 指定的位置开始重建；
    - 这样可以在不影响 CommitLog 完整性的前提下，逐步修复所有索引。
  - mini 当前实现：
    - `RecordStore.StoreCheckpoint` 记录 CommitLog 与 ConsumeQueue 最新一次刷盘偏移；
    - 目前主要用于示例“如何把偏移写到一个单独的小文件”，并未在启动时驱动实际恢复流程；
    - 没有常驻的 Reput 服务，ConsumeQueue/KeyIndex 的写入都在 append 时直接完成。
  - mini 的改进点（思路强化版）：
    - 利用 `StoreCheckpoint` 缩小重放范围：
      - 启动时先 `load()` 出最新一次刷盘的 `commitLogMaxOffset` 与 `consumeQueueMaxOffset`；
      - 结合 CommitLog 启动扫描结果，计算出“从哪个物理位置开始需要重放”（例如从上次已索引到的物理 offset 之后开始）；
      - 这样无需每次都从 0 重放整个 CommitLog，只处理“Checkpoint 之后的新数据”。
    - 增加一个简化版 Reput 线程（或一次性重放过程）：
      - 从上述“重放起点 offset”开始，顺序遍历 CommitLog：
        - 解析出每条消息的物理 offset、长度以及业务字段（比如时间戳、key 等）；
        - 为每条消息写入 ConsumeQueue：`(physicalOffset, size, tagCode)`，其中 `tagCode` 可以继续用时间戳；
        - 可选地为每条消息写入 KeyIndex：`(keyHash, physicalOffset)`。
      - 重放到当前 CommitLog 末尾后，再更新 `StoreCheckpoint` 中记录的偏移，表示“索引已经追平到哪个位置”。
    - 后续可以进一步演进为常驻线程：
      - 正常写入路径只负责把消息写入 CommitLog；
      - 一个后台 Reput 线程基于 Checkpoint 持续“追尾”构建 ConsumeQueue/KeyIndex；
      - 宕机重启时，依靠 CommitLog + Checkpoint + Reput，可以自动修复/重建大部分索引，使 mini 的整体恢复/重建流程更接近 RocketMQ。

---

## 四、RocketMQ 与 mini-file-storage mini 对比表

下面用一个表格对比 RocketMQ 真实实现和本 demo mini 实现的关系，方便对号入座：

| 维度/特性                  | RocketMQ                                                   | mini-file-storage mini 实现                                                                |
|---------------------------|------------------------------------------------------------|------------------------------------------------------------------------------------|
| 物理存储                  | CommitLog 多文件队列（MappedFileQueue）                   | `MmapSequentialLog` + `SimpleMappedFileQueue`                                      |
| 单文件管理                | 固定大小，文件名为起始 offset，滚动创建                   | 同样：固定大小文件，文件名为起始 offset，滚动创建                                  |
| 消息物理格式              | 头 + 体，包含多种字段（魔数、CRC、主题、队列 ID 等）      | 简化为 `[4 字节 length][payload]`                                                  |
| 多 Topic/Queue 支持       | 所有消息写同一 CommitLog；按 Topic.Queue 维护 ConsumeQueue | 只做“单队列”模型，但 ConsumeQueue 结构与 RocketMQ 类似                            |
| 逻辑队列索引结构          | ConsumeQueue：20 字节 `[offset][size][tagCode]`           | `SimpleConsumeQueue`：20 字节结构完全对齐                                          |
| 按 Topic.Queue + offset 读 | 逻辑 offset → ConsumeQueue → CommitLog                    | `readByLogicalOffset` → `SimpleConsumeQueue` → `RecordStore.read`                  |
| 按时间查询                | 结合 CommitLog 时间戳、文件名、tagCode/IndexFile          | 使用 `SimpleConsumeQueue` 的 tagCode 存时间戳，`queryByTimeRange` 顺序过滤         |
| 按 key 查询               | IndexFile：hash 槽 + index entry 结构                     | 简化 `KeyIndexFile`：顺序存储 `[keyHash][physicalOffset]`，线性扫描                |
| 刷盘策略                  | 同步/异步刷盘，多线程 flush/commit                        | `flush()` 直接调用底层 `MappedByteBuffer.force()`，支持配置 syncFlush              |
| 宕机恢复                  | CommitLog 扫描 + ConsumeQueue 校验/截断 + Reput 重建索引  | 示例中具备扫描 + checkpoint 能力的基础设施（SimpleConsumeQueue + StoreCheckpoint） |
| Checkpoint                | 独立 Checkpoint 文件记录多组件刷盘时间                    | `StoreCheckpoint` 记录 CommitLog / ConsumeQueue 已刷盘偏移                         |
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

在 IDE 中直接运行 `FileDemoMain.main` 即可观察完整读写过程。

### 2. 单元测试（推荐阅读）

256→- 测试类：`com.example.filedemo.FileDemoTest`
257→- 所在目录：`src/test/java/com/example/filedemo/demo/FileDemoTest.java`
258→- 覆盖内容包括：
259→  - `testAppendAndReadByPhysicalOffset`：验证物理 offset 读写；
260→  - `testReadByLogicalOffset`：验证通过 ConsumeQueue 的逻辑 offset 读取；
261→  - `testQueryByTimeRange`：验证按时间范围查询；
262→  - `testKeyIndexQuery`：验证按 key 查询；
263→  - `testCheckpointWrittenOnFlush`：验证 flush 时 checkpoint 正确写入；
264→  - `testRecoverCommitLogAfterRestart`：验证 CommitLog 基于文件扫描的启动恢复；
265→  - `testRecoverConsumeQueueAfterRestart`：验证 ConsumeQueue 启动恢复后在旧索引后继续追加并按逻辑 offset 读取。

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
- 第四步：最后对照 `FileDemoTest` 和 `FileDemoMain`，从测试/运行示例验证自己对流程的理解。

### 2. 写入流程（从业务对象到磁盘与索引）

以 `RecordStore<LogRecord>.append(record, tagCode)` 为例：

```text
业务代码
  └─ RecordStore.append(value, tagCode)
       ├─ Codec.encode(value)                           （对象 → payload）
       ├─ 构造 [length(4B) + payload] 记录
       ├─ SequentialLog.append(recordBytes)             （顺序写 CommitLog）
       │    └─ MmapSequentialLog.append(...)
       │         ├─ SimpleMappedFileQueue.getLastMappedFile(true)
       │         │    └─ SimpleMappedFile.append(...)   （mmap 顺序写单个文件）
       │         └─ fileQueue.getMaxOffset()            （返回当前最大物理 offset）
       ├─ 计算 recordOffset = maxOffset - recordBytes.length
       ├─ 如果配置了 ConsumeQueue:
       │    └─ SimpleConsumeQueue.append(recordOffset, length, tagCode)
       │         └─ SimpleMappedFileQueue.getLastMappedFile(true)
       │              └─ SimpleMappedFile.append(20B 索引单元)
       └─ 如果业务配置了 KeyIndex:
            └─ KeyIndexFile.put(keyHash, recordOffset)
                 └─ SimpleMappedFileQueue.getLastMappedFile(true)
                      └─ SimpleMappedFile.append(16B 索引单元)
```

刷盘/Checkpoint 写入：

```text
业务代码
  └─ RecordStore.flush()
       ├─ log.flush()
       │    └─ MmapSequentialLog.flush()
       │         └─ SimpleMappedFileQueue.flush()
       │              └─ 遍历所有 SimpleMappedFile.flush() （MappedByteBuffer.force）
       ├─ consumeQueue.flush() （如存在）
       │    └─ SimpleMappedFileQueue.flush()
       └─ checkpoint.save(commitLogMaxOffset, consumeQueueMaxOffset) （如配置）
            └─ StoreCheckpoint.save(...) 写入 checkpoint 文件
```

### 3. 读取流程（按物理 offset / 逻辑 offset / 时间 / key）

按物理 offset 读取一条记录：

```text
业务代码
  └─ RecordStore.read(offset)
       ├─ log.read(offset, 4)          读取 length
       ├─ log.read(offset + 4, length) 读取 payload
       └─ codec.decode(payload)        反序列化为业务对象
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

### 4. 启动恢复流程（当前实现）

这一节只描述当前代码中真实存在的启动恢复路径，围绕 CommitLog 与 ConsumeQueue 的 wrotePosition 校正。前文表格中提到的“基于 checkpoint 的一次性重放 / 后台 Reput 线程”等能力属于待改进方向，这里不再给出具体伪代码。

```text
应用启动
  ├─ 构造 MmapSequentialLog(config)
  │    └─ SimpleMappedFileQueue.recoverForRecordStore(storePath, fileSize)
  │         ├─ 扫描 data 目录下所有 CommitLog 文件
  │         └─ 对每个文件调用 SimpleMappedFile.recoverRecordStoreWrotePosition()
  │              └─ 按 [length][payload] 顺序扫描，确定 wrotePosition
  │
  ├─ 构造 SimpleConsumeQueue(cqPath, size, recover = true)
  │    └─ SimpleMappedFileQueue.recoverForConsumeQueue(cqPath, fileSize)
  │         ├─ 扫描 cq 目录下所有索引文件
  │         └─ 对每个文件调用 SimpleMappedFile.recoverConsumeQueueWrotePosition()
  │              └─ 按 20B 单元扫描，检查 offset>=0 && size>0
  │
  ├─ 构造 StoreCheckpoint(checkpointPath)
  │    └─ 可选：调用 StoreCheckpoint.load() 读取历史偏移（当前实现尚未在恢复流程中主动使用）
  │
  └─ 构造 RecordStore(log, codec, consumeQueue, checkpoint)
```

通过这条启动恢复流程，你可以从“应用启动”一路追踪到 `SimpleMappedFile` 的扫描逻辑，理解 mini 当前实现如何在宕机后恢复 CommitLog 与 ConsumeQueue 的写入位置。  
