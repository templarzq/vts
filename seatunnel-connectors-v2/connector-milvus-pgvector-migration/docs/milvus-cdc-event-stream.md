# Milvus CDC 事件流同步（event_stream 策略）

## 1. 概述

### 1.1 设计目标

`event_stream` 是 Milvus → pgvector 迁移连接器提供的第三种 CDC（变更数据捕获）策略。与前两种策略（`polling_incremental`、`grpc_replicate`）基于"主键差分"检测新增数据不同，本策略**直接订阅 Milvus 服务端的 WAL（Write-Ahead Log）事件流**，从而获得对**删除事件**和**同主键更新**的捕获能力。

实现方式：通过 Milvus 提供的服务端流式 gRPC 接口 `DumpMessages` 拉取 `ImmutableMessage` 帧，解析其 `payload` 中的 `InsertRequest` / `DeleteRequest`，转换为 SeaTunnel 的 `SeaTunnelRow`（带 `RowKind`），下游 sink 即可据以应用插入或删除。

### 1.2 三种 CDC 策略对比

| 能力 | polling_incremental | grpc_replicate | **event_stream（本策略）** |
|------|---------------------|----------------|---------------------------|
| 全量快照 | ✅ | ✅ | ✅（共用快照阶段） |
| 增量插入捕获 | ✅ | ✅ | ✅ |
| **删除事件捕获** | ❌ | ❌ | **✅** |
| 同主键更新捕获 | ❌（仅检测新增 PK） | ❌ | ✅（WAL 中以 INSERT 形式出现，依赖 sink upsert） |
| 实现机制 | 轮询 query 迭代 | 轮询 query + 位点记录 | **gRPC 服务端流（DumpMessages）** |
| 依赖 | 仅 Milvus SDK | 仅 Milvus SDK | Milvus 服务端启用 `collectionReplicateEnable` |
| 故障恢复 | 重置 watermark | 从 checkpoint 恢复 | 流断开 → `GetReplicateInfo` 重新 bootstrap |
| 性能 | 受 query 轮询频率限制 | 同 polling | gRPC 流式拉取，吞吐高、延迟低 |

> **核心差异**：`event_stream` 是**唯一能捕获删除事件**的策略，是真正意义上的 CDC。

---

## 2. 实现原理

### 2.1 整体架构

```
┌──────────────────────────────────────────────────────────────┐
│                       Milvus Server                          │
│  ┌────────────────┐         ┌─────────────────────────────┐   │
│  │  Collection    │         │      WAL (RocksMQ / Pulsar) │   │
│  │  (Insert/Delete)│ ───►  │  ImmutableMessage 流        │   │
│  └────────────────┘         └────────────┬────────────────┘   │
│                                          │                    │
│  ┌───────────────────────────────────────▼────────────────┐  │
│  │  MilvusService gRPC                                    │  │
│  │  ├── GetReplicateInfo (unary)   → ReplicateCheckpoint │  │
│  │  └── DumpMessages  (server-stream) → ImmutableMessage │  │
│  └────────────────────────────┬───────────────────────────┘  │
└────────────────────────────────┼─────────────────────────────┘
                                 │ gRPC (shaded: milvus.io.grpc)
                                 ▼
┌──────────────────────────────────────────────────────────────┐
│              CdcEventStreamStrategy (本模块)                 │
│  ┌──────────────────┐   ┌─────────────────┐   ┌────────────┐ │
│  │ MilvusCdcGrpcClient│ ─►│ StreamObserver │ ─►│ Iterator   │ │
│  │  (asyncStub)     │   │ → LinkedBlockingQueue │ (timeout) │ │
│  └──────────────────┘   └─────────────────┘   └─────┬──────┘ │
│                                                │          │
│  ┌─────────────────────────────────────────────▼──────┐  │
│  │ MilvusEventParser                                   │  │
│  │  ├── InsertRequest → SeaTunnelRow[] (INSERT)        │  │
│  │  └── DeleteRequest → SeaTunnelRow[] (DELETE)       │  │
│  │        └─ UnknownFieldSet 提取 primary_keys (字段12) │  │
│  └────────────────────────────────────────────────────┘  │
└────────────────────────────┬─────────────────────────────┘
                             │ SeaTunnelRow + ReplicatePosition
                             ▼
                    ┌──────────────────┐
                    │  pgvector Sink   │
                    │  (INSERT / DELETE) │
                    └──────────────────┘
```

### 2.2 Milvus CDC gRPC 接口

本模块在 `src/main/proto/milvus_cdc.proto` 中自维护了一份与 Milvus 服务端 wire 兼容的 proto 定义（服务路径 `/milvus.proto.milvus.MilvusService/DumpMessages`），仅声明 CDC 所需的两个 RPC：

| RPC | 类型 | 用途 |
|-----|------|------|
| `GetReplicateInfo` | 一元 RPC | 获取复制位点（`checkpoint` + `salvage_checkpoint`），用于首次 bootstrap 与故障恢复 |
| `DumpMessages` | 服务端流式 RPC | 持续推送 `ImmutableMessage` 帧，是事件流的真正数据通道 |

`GetReplicateInfoResponse` 包含两个位点字段：
- `checkpoint`：当前已复制位点（由 Milvus 内部复制系统维护）
- `salvage_checkpoint`：force failover 前最后一次同步位点（仅在 failover 后存在）

策略优先使用 `checkpoint`，若为空则回退到 `salvage_checkpoint`，两者都为空则 `isAvailable()` 返回 false。

### 2.3 ImmutableMessage 结构

每帧 `ImmutableMessage` 由三部分组成：

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | `MessageID` | WAL 消息标识，含 `id` (string) 和 `wALName` (枚举) |
| `payload` | `bytes` | 序列化的 DML 消息体（`InsertRequest` / `DeleteRequest` 等） |
| `properties` | `map<string,string>` | 元数据，关键 key 见下表 |

`properties` 关键 key：

| Key | 含义 | 示例值 |
|-----|------|--------|
| `messages.type` | WAL 消息类型名 | `"Insert"` / `"Delete"` / `"TimeTick"` / ... |
| `messages.collection` | 源 collection ID（字符串） | `"45154321654321"` |
| `messages.vchannel` | 虚拟通道名 | `"cdc_es_avail_43554321654321_0"` |
| `messages.timetick` | 消息 timetick（uint64 字符串） | `"1782799350009"` |

### 2.4 事件解析（MilvusEventParser）

`MilvusEventParser` 根据 `messages.type` 分派：

#### Insert 消息

- `payload` 反序列化为 `io.milvus.grpc.InsertRequest`
- 按 schema 字段顺序，将列式 `FieldData` 切分为逐行 `Object[]`
- 每行封装为 `SeaTunnelRow`，`RowKind = INSERT`
- 支持的数据类型：`Bool` / `Int8` / `Int16` / `Int32` / `Int64` / `Float` / `Double` / `VarChar` / `String` / `JSON` / `FloatVector` / `BinaryVector`
- 未支持的类型（如 `Text` / `Float16Vector` / `BFloat16Vector` / `Int8Vector` / `SparseFloatVector`）走 `default` 分支并以 debug 日志跳过

#### Delete 消息（核心能力）

- `payload` 反序列化为 `io.milvus.grpc.DeleteRequest`
- **关键问题**：SDK 2.6.8 的 `DeleteRequest` 类比 WAL 协议落后一个版本，**未暴露 `primary_keys` 字段（proto 字段 12，类型 `milvus.proto.schema.IDs`）**
- **解决方案**：通过 protobuf 的 `UnknownFieldSet` 提取字段 12 的原始 bytes，再 `IDs.parseFrom(raw)` 还原为 `IDs` 消息，进而读取 `int_id` (LongArray) 或 `str_id` (StringArray)
- 若字段 12 不存在，回退到字段 9（`int64_primary_keys`，已废弃的 LongArray）
- 每个 PK 生成一个 `SeaTunnelRow`，`RowKind = DELETE`，仅填充主键字段，其余字段为 null

```
DeleteRequest (wire format)
├── 字段 1: collection_name (string)
├── 字段 2: partition_name (string)
├── ...
├── 字段 9: int64_primary_keys (LongArray, 已废弃) ← 回退方案
├── 字段 11: expr (string)
└── 字段 12: primary_keys (IDs) ← 当前方案，SDK 未暴露，通过 UnknownFieldSet 提取
    ├── int_id: LongArray
    └── str_id: StringArray
```

#### 非 DML 消息

`TimeTick` / `CreateCollection` / `DropCollection` / `CreatePartition` / `DropPartition` 等控制消息走 `default` 分支，返回空列表，不做处理。

### 2.5 位点管理（ReplicatePosition）

每个产出的事件都附带 `ReplicatePosition`：

| 字段 | 类型 | 说明 |
|------|------|------|
| `clusterId` | `String` | 源集群 ID（来自 `cdc_source_cluster_id` 或 checkpoint） |
| `pchannel` | `String` | 物理通道名（如 `by-dev-rootcoord-dml_0`） |
| `messageId` | `String` | WAL 消息 ID（字符串形式，可 round-trip 回 `DumpMessages`） |
| `timeTick` | `long` | 消息 timetick（uint64） |
| `timestamp` | `long` | 处理时间戳（`System.currentTimeMillis()`） |

`messageId` 是断点续传的关键：作业失败重启后，从 checkpoint 中的 `messageId` 重新调用 `DumpMessages` 即可恢复，不会重复消费也不会漏消息。

### 2.6 故障恢复

#### 流断开 / 错误

`CdcEventStreamStrategy.hasNextWithTimeout()` 检测到流结束或异常时，将 `currentStream` 置为 `null`，下一次 `pollChanges()` 调用会重新走 `openStream()` → `resolveStartMessageId()` → `GetReplicateInfo` 流程重新 bootstrap。

#### 超时驱动

`DumpMessages` 是阻塞式服务端流。当 Milvus 端无新消息时，`Iterator.hasNext()` 会阻塞等待。为避免阻塞 SeaTunnel 引擎的 checkpoint 循环，本策略用单线程 `ExecutorService` + `Future.get(pollIntervalMs)` 包装 `hasNext()` 调用：

- 在 `pollIntervalMs`（默认 1000ms）内有新消息 → 正常返回
- 超时 → 返回 false（不关闭流），下一轮 poll 继续 waiting
- 异常 → 关闭流，下一轮 poll 重新 bootstrap

#### 自动回退

`MilvusCdcSourceReader.createCdcStrategy()` 在以下情况会**自动回退到 `polling_incremental`**：
1. `cdc_pchannel` 未配置
2. `CdcEventStreamStrategy.isAvailable()` 返回 false（`GetReplicateInfo` 返回空 checkpoint）

回退会以 WARN 日志记录，作业不会失败，但会失去删除捕获能力。

---

## 3. 配置参数

| 参数 | 类型 | 默认值 | 必填 | 说明 |
|------|------|--------|------|------|
| `url` | string | — | ✅ | Milvus 服务地址，如 `http://localhost:19530` |
| `token` | string | `""` | | 认证 token（用户名:密码 base64 或空） |
| `database` | string | `default` | | Milvus 数据库名 |
| `collection` | string | — | ✅ | 要捕获变更的 collection 名 |
| `cdc_strategy` | string | `polling_incremental` | | 设为 **`event_stream`** 启用本策略 |
| `cdc_pchannel` | string | — | ✅（本策略） | 物理通道名，如 `by-dev-rootcoord-dml_0` |
| `cdc_source_cluster_id` | string | — | | `GetReplicateInfo` 的 `source_cluster_id`，留空则由服务端默认 |
| `cdc_start_message_id` | string | — | | 显式指定起始 WAL message ID（覆盖 checkpoint bootstrap） |
| `incremental_batch_size` | long | `500` | | 单次 poll 最大事件数 |
| `poll_interval_ms` | long | `1000` | | poll 超时阈值（同时也是无消息时的等待间隔） |
| `primary_key_field` | string | `id` | | 主键字段名，用于 delete 事件的字段定位 |
| `channel_timeout_ms` | long | `30000` | | gRPC channel 连接/读超时（毫秒） |
| `startup_mode` | string | `INITIAL` | | `INITIAL`（全量+增量）或 `LATEST`（仅增量） |
| `batch_size` | int | `1000` | | 全量快照阶段每批查询行数 |
| `parallelism` | int | `1` | | 全量快照阶段并发度 |

> **TLS 配置**（可选）：`client_pem_path` / `client_key_path` / `ca_pem_path` / `server_name` 用于启用 mTLS 连接 Milvus。

---

## 4. 使用示例

### 4.1 完整 HOCON 配置示例

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  Milvus-CDC {
    url = "http://localhost:19530"
    token = ""
    database = "default"
    collection = "my_collection"

    # 启用事件流 CDC
    cdc_strategy = "event_stream"

    # 物理通道名（必填）
    cdc_pchannel = "by-dev-rootcoord-dml_0"

    # 可选：源集群 ID
    # cdc_source_cluster_id = "my-cluster"

    # 可选：显式指定起始位点（覆盖 GetReplicateInfo）
    # cdc_start_message_id = "1782799350009"

    # 增量参数
    incremental_batch_size = 500
    poll_interval_ms = 1000

    # 全量快照参数
    startup_mode = "INITIAL"
    batch_size = 1000
    primary_key_field = "id"

    # gRPC 超时
    channel_timeout_ms = 30000

    result_table_name = "milvus_cdc_events"
  }
}

sink {
  jdbc {
    url = "jdbc:postgresql://localhost:5432/mydb"
    driver = "org.postgresql.Driver"
    user = "postgres"
    password = "secret"
    query = "INSERT INTO my_table (id, vector, category) VALUES (?, ?, ?) ON CONFLICT (id) DO UPDATE SET vector = EXCLUDED.vector, category = EXCLUDED.category"
    # 删除事件需要 sink 端单独处理（见 4.2）
  }
}
```

### 4.2 启动命令

```bash
# 启动 SeaTunnel 集群
./bin/seatunnel-cluster.sh -d

# 提交作业
./bin/seatunnel.sh --config ./config/milvus_cdc_event_stream.conf
```

**Sink 端删除处理**：pgvector JDBC sink 默认不区分 `RowKind`。若要应用 DELETE 事件，需要在 sink 配置中使用支持 upsert/delete 的 sink（如支持 `primary_keys` 的 jdbc sink），或自定义 transform 路由 DELETE 事件到单独的删除 SQL。

### 4.3 pchannel 名称确定方法

`pchannel` 名称由 Milvus 服务端的 `etcd.rootPath` 决定。standalone 默认 `rootPath = by-dev`，对应 pchannel：

```
{rootPath}-rootcoord-dml_0
```

**确定方法**：

1. **查看 milvus.yaml**：
   ```yaml
   etcd:
     rootPath: by-dev   # ← 此处
   ```
   则 pchannel = `by-dev-rootcoord-dml_0`

2. **运行中的 Milvus 容器**：
   ```bash
   docker exec milvus-standalone cat /milvus/configs/milvus.yaml | grep rootPath
   ```

3. **常见值**：
   - 默认（开发）：`by-dev-rootcoord-dml_0`
   - 生产环境：`<your-prefix>-rootcoord-dml_0`

> **注意**：pchannel 名称必须完全匹配（区分大小写）。错误的 pchannel 会导致 `isAvailable()` 返回 false，策略回退到 polling。

---

## 5. 与其他 CDC 策略对比

| 维度 | polling_incremental | grpc_replicate | **event_stream** |
|------|---------------------|----------------|------------------|
| **实现机制** | 周期性 query + watermark | 周期性 query + gRPC 位点 | **gRPC 服务端流（DumpMessages）** |
| **新增捕获** | ✅ 通过 PK 差分 | ✅ 通过 PK 差分 | ✅ WAL Insert 事件 |
| **删除捕获** | ❌ 无法检测 | ❌ 无法检测 | **✅ WAL Delete 事件** |
| **同 PK 更新** | ❌ | ❌ | ✅（WAL 中以 INSERT 出现，sink 端 upsert） |
| **延迟** | 取决于 poll 间隔 | 取决于 poll 间隔 | gRPC 流式，亚秒级 |
| **吞吐** | 受 query 限制 | 受 query 限制 | gRPC 流，高吞吐 |
| **位点语义** | PK 值 | timestamp + PK | **WAL message ID**（精确到消息） |
| **故障恢复** | 重置 watermark | 从 checkpoint 恢复 | 从 messageId 恢复 + GetReplicateInfo bootstrap |
| **服务端依赖** | 无 | 无 | `collectionReplicateEnable: true` |
| **SDK 依赖** | 仅 SDK | 仅 SDK | SDK + 自维护 proto |
| **自动回退** | 不回退 | 不回退 | 不可用时回退到 polling |
| **适用场景** | 仅插入、对延迟不敏感 | 仅插入、需位点 | **需要删除/更新捕获、低延迟** |

---

## 6. 限制与注意事项

### 6.1 服务端前置条件

Milvus 服务端必须启用以下配置（`milvus.yaml`）：

```yaml
common:
  ttMsgEnabled: true                 # 启用 timetick 消息（CDC 依赖）
  collectionReplicateEnable: true    # 启用 collection 复制（暴露 CDC 接口）
```

否则 `GetReplicateInfo` 返回空 checkpoint，`isAvailable()` 返回 false，策略自动回退到 polling。

### 6.2 pchannel 名称严格匹配

`cdc_pchannel` 必须与 Milvus 服务端实际 pchannel 完全匹配。错误的名称不会报错，但会导致 `isAvailable()` 返回 false 并静默回退。

### 6.3 删除事件的主键提取（SDK 兼容性问题）

SDK 2.6.8 的 `io.milvus.grpc.DeleteRequest` 类**未暴露 `primary_keys` 字段**（proto 字段 12，类型 `IDs`）。本策略通过 protobuf 的 `UnknownFieldSet` 提取该字段的原始 bytes 并重新解析为 `IDs` 消息。

**风险**：
- 若未来 SDK 升级后 `DeleteRequest` 暴露了 `primary_keys` getter，字段将不再位于 `UnknownFieldSet` 中，本策略的提取逻辑会失效。
- 升级 SDK 时需同步检查：若 `DeleteRequest.getPrimaryKeys()` 可用，应直接调用而弃用 `UnknownFieldSet` 方案。

### 6.4 proto 微差异（不影响 wire 兼容）

本模块 `src/main/proto/milvus_cdc.proto` 的 `DumpMessagesRequest` 缺少字段 5 `bool start_position_exclusive`（上游设计文档列出）。由于 proto3 忽略未知字段，wire 兼容性不受影响——服务端会以默认值 `false` 处理（即不排除起始位点本身）。

若未来需要"排除起始位点"语义，可在 proto 中添加该字段，无需重新生成 SDK。

### 6.5 同 PK 更新在 WAL 中的表现

Milvus 的 upsert 操作在 WAL 中表现为 **DELETE + INSERT 两条消息**或**单条 INSERT 消息**（取决于版本和配置）。本策略将其作为两条独立事件传递给下游，由 sink 端通过 `ON CONFLICT ... DO UPDATE`（PostgreSQL）等机制实现幂等写入。

### 6.6 自动回退的副作用

当 `event_stream` 不可用时，`MilvusCdcSourceReader` 会自动回退到 `polling_incremental`。这意味着：
- 作业不会失败
- 但会**丢失删除捕获能力**
- 日志中会有 WARN 级别的回退记录

生产环境应监控该 WARN 日志，确保 CDC 能力符合预期。

### 6.7 未支持的数据类型

以下数据类型当前未在 `MilvusEventParser.populateColumn()` 中处理，会以 null 返回并记录 debug 日志：
- `Text`（proto 中有，但 SDK 的 `DataType` 枚举未包含）
- `Float16Vector` / `BFloat16Vector` / `Int8Vector` / `SparseFloatVector`

如需支持，在 `MilvusEventParser` 的 switch 中添加对应 case 即可。

---

## 7. 测试

### 7.1 单元测试

| 测试类 | 场景数 | 说明 |
|--------|--------|------|
| `MilvusEventParserTest` | 11 | 直接构造 protobuf DML 消息，验证 parser 输出 |
| `CdcEventStreamStrategyTest` | 9 | 使用 Mockito mock `MilvusCdcGrpcClient`，验证策略行为 |

**关键测试场景**：
- Insert：Int64 PK + FloatVector / VarChar PK / BinaryVector / 多标量字段
- Delete：Int64 PK（注入 field 12 原始 bytes）/ String PK / 无主键（仅 expr）
- 非 DML：TimeTick 跳过
- Collection 过滤：mismatched vs matched
- Strategy：availability 检查 / 空流 / insert/delete 事件 / maxEventsPerPoll 截断 / 位点追踪 / 流错误后重新 bootstrap

**运行命令**：
```bash
./mvnw test -pl seatunnel-connectors-v2/connector-milvus-pgvector-migration \
    -Dtest=MilvusEventParserTest,CdcEventStreamStrategyTest \
    -DskipUT=false -Dmaven.test.skip=false
```

预期：`Tests run: 20, Failures: 0, Errors: 0, Skipped: 0`。

### 7.2 E2E 测试

`MilvusCdcEventStreamE2E` 提供 7 个有序 E2E 场景，需要运行中的 Milvus 2.6.x + pgvector 环境：

| 顺序 | 测试方法 | 说明 |
|------|----------|------|
| 1 | `testEventStreamAvailability` | 验证 CDC 在 Milvus 上可用（否则跳过后续） |
| 2 | `testEventStreamFullSnapshotSync` | 全量快照同步 100 行 |
| 3 | `testEventStreamIncrementalInsert` | 增量插入 50 行捕获 |
| 4 | **`testEventStreamDeleteCapture`** | **核心：删除 30 行，验证 DELETE 事件被捕获并应用到 pgvector** |
| 5 | `testEventStreamSamePkUpdate` | 同 PK 更新 20 行，验证 upsert 语义 |
| 6 | `testEventStreamVsPollingComparison` | 对比 event_stream（能捕获删除）与 polling（不能） |
| 7 | `testEventStreamUnavailableFallback` | 错误 pchannel 时回退到 polling |

**门禁**：整个类使用 `@EnabledIfSystemProperty(named = "migration.cdc.e2e.enabled", matches = "true")`，未启用时全部跳过。此外，测试 1 通过 `Assumptions.assumeTrue(cdcAvailable)` 决定是否跳过后续测试。

**运行命令**：
```bash
./mvnw test -pl seatunnel-connectors-v2/connector-milvus-pgvector-migration \
    -Dtest=MilvusCdcEventStreamE2E \
    -Dmigration.cdc.e2e.enabled=true \
    -Dmaven.test.skip=false -DskipUT=false
```

**报告产物**：
- JSON：`target/cdc-e2e-reports/cdc-e2e-report-<timestamp>.json`
- Markdown：`target/cdc-e2e-reports/CDC-E2E-Test-Report.md`

---

## 8. 故障排查

### 8.1 `isAvailable()` 返回 false

**症状**：日志 `event_stream CDC not available: GetReplicateInfo returned no checkpoint for pchannel=...`

**排查清单**：
1. **检查 `cdc_pchannel` 是否配置**：未配置会直接返回 false
2. **检查 Milvus 配置**：`common.collectionReplicateEnable` 和 `common.ttMsgEnabled` 是否为 `true`
3. **检查 pchannel 名称**：是否与 `milvus.yaml` 的 `etcd.rootPath` 推导一致
4. **检查 Milvus 是否重启过**：配置变更后需重启 Milvus 容器
5. **检查 GetReplicateInfo RPC 是否可达**：`grpcClient.getReplicateInfo()` 抛异常会返回 false（日志级别 WARN）

### 8.2 流断开后无事件

**症状**：日志 `DumpMessages stream ended, will re-bootstrap on next poll` 反复出现

**排查**：
- 检查 pchannel 是否有数据写入（无写入则无事件）
- 检查 `start_message_id` 是否过旧（已被 WAL 清理）
- 调整 `poll_interval_ms`（默认 1000ms）以匹配事件频率

### 8.3 删除事件为空

**症状**：删除操作后，`MilvusEventParser` 输出 0 行 DELETE 事件

**排查**：
1. **检查 `DeleteRequest` 是否携带 `primary_keys`**：现代 WAL 消息应携带字段 12。若仅 `expr`（如 `"id in [1,2,3]"`），parser 无法提取 PK，会返回空
2. **检查 SDK 版本**：若 SDK 升级后 `DeleteRequest` 暴露 `primary_keys` getter，`UnknownFieldSet` 方案会失效。需检查 `io.milvus.grpc.DeleteRequest` 是否有 `getPrimaryKeys()` 方法
3. **启用 debug 日志**：`logging.level.org.apache.seatunnel.connectors.migration.milvus2pgvector.cdc=DEBUG`，查看 `Failed to parse primary_keys IDs sub-message` 等警告

### 8.4 自动回退到 polling

**症状**：日志 `event_stream CDC not available ... falling back to polling_incremental strategy`

**影响**：作业继续运行，但失去删除捕获能力。

**处理**：
- 短期：接受回退，监控 polling 策略的 INSERT 捕获是否正常
- 长期：修复 Milvus 服务端 CDC 配置后重启作业

### 8.5 编译错误：`Text` 不是有效的 DataType 枚举常量

**症状**：`MilvusEventParser.java` 编译报错 `case Text:` 不是有效的枚举常量

**原因**：proto 中 `DataType` 有 `Text = 25`，但 SDK 的 `io.milvus.v2.common.DataType` 手写枚举未包含 `Text`。

**解决**：已删除 `case Text:` 标签，Text 字段走 `default` 分支并以 debug 日志跳过。

### 8.6 编译错误：`getBinaryVector().getData()` 不存在

**症状**：`MilvusEventParser.java` 编译报错 `cannot find symbol getData()`

**原因**：proto 中 `binary_vector` 字段类型是 `bytes`（不是 `BinaryVector` 消息），所以 `getBinaryVector()` 直接返回 `ByteString`，无 `getData()` 方法。

**解决**：已删除 `.getData()` 调用，直接使用 `getBinaryVector()` 返回的 `ByteString`。

---

## 9. 参考

### 9.1 相关源码文件

| 文件 | 说明 |
|------|------|
| `src/main/proto/milvus_cdc.proto` | CDC gRPC 接口 proto 定义（自维护） |
| `src/main/java/.../cdc/CdcEventStreamStrategy.java` | 事件流策略主类 |
| `src/main/java/.../cdc/MilvusCdcGrpcClient.java` | gRPC 客户端（GetReplicateInfo + DumpMessages） |
| `src/main/java/.../cdc/MilvusEventParser.java` | WAL 消息解析器（Insert/Delete → SeaTunnelRow） |
| `src/main/java/.../cdc/MilvusCdcSourceConfig.java` | 配置选项定义 |
| `src/main/java/.../cdc/MilvusCdcSourceReader.java` | Reader 主类（含自动回退逻辑） |
| `src/main/java/.../cdc/ReplicatePosition.java` | 位点记录类 |
| `src/main/java/.../cdc/CdcStrategy.java` | CDC 策略接口 |

### 9.2 测试文件

| 文件 | 类型 | 场景数 |
|------|------|--------|
| `src/test/java/.../cdc/MilvusEventParserTest.java` | 单元测试 | 11 |
| `src/test/java/.../cdc/CdcEventStreamStrategyTest.java` | 单元测试（Mockito） | 9 |
| `src/test/java/.../cdc/MilvusCdcEventStreamE2E.java` | E2E 测试 | 7 |

### 9.3 外部参考

- **Milvus CDC 设计文档**：`milvus/docs/design-docs/cdc/20260205-data_salvage_for_force_failover.md`（数据 salvage 机制，引入 `DumpMessages` 与 `salvage_checkpoint`）
- **Milvus WAL 消息接口**：`milvus/pkg/streaming/util/message/message.go`（`ImmutableMessage` Go 接口定义）
- **Milvus WAL Scanner**：`milvus/pkg/streaming/walimpls/scanner.go`（服务端 WAL 扫描器，通过 channel 推送 `ImmutableMessage`）
- **模块 README**：`README.md`（连接器整体使用说明）
- **完成计划**：`.trae/documents/milvus-cdc-event-stream-completion-v3.md`（本实现的设计与验证计划）
