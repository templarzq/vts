# Milvus → pgvector 迁移工具 部署及使用手册

本工具基于 Apache SeaTunnel 构建，用于将 Milvus 向量数据库中的 Collection（向量数据、标量元数据、索引信息）完整迁移到 PostgreSQL + pgvector，并提供数据一致性校验与断点续传能力。

- 源端：Milvus 2.x（≥ 2.3.6，推荐 2.6+）
- 目标端：PostgreSQL 14+ 且安装 pgvector 扩展（≥ 0.8.0）
- 执行引擎：SeaTunnel 2.3.x

---

## 目录

1. [功能概览](#1-功能概览)
2. [架构与迁移流程](#2-架构与迁移流程)
3. [环境前置条件](#3-环境前置条件)
4. [构建与打包](#4-构建与打包)
5. [部署方式](#5-部署方式)
6. [使用方式](#6-使用方式)
7. [示例配置](#7-示例配置)
8. [类型映射参考](#8-类型映射参考)
9. [索引映射参考](#9-索引映射参考)
10. [数据一致性校验](#10-数据一致性校验)
11. [断点续传](#11-断点续传)
12. [审计日志与报告](#12-审计日志与报告)
13. [性能调优](#13-性能调优)
14. [错误码](#14-错误码)
15. [常见问题](#15-常见问题)
16. [测试](#16-测试)

---

## 1. 功能概览

| 能力 | 说明 |
| --- | --- |
| 全量数据迁移 | 迁移向量列、标量列、JSON、数组等所有字段 |
| **CDC 实时增量同步** | **支持 INSERT / DELETE / UPSERT 实时捕获，基于 Milvus WAL 事件流** |
| Schema 迁移 | 自动 introspect Milvus Collection，生成并执行 pgvector 建表 DDL |
| 索引迁移 | 将 Milvus HNSW / IVF_FLAT / AUTOINDEX 等转换为 pgvector 等效索引 |
| 数据一致性校验 | 记录数校验 + 向量相似度采样校验 + 逐字段采样校验 |
| 断点续传 | 基于进度状态文件，失败后可跳过已完成阶段继续执行 |
| 流量控制 | 支持批大小、并行度、令牌桶限流（行/秒） |
| 精度控制 | 可选禁止 BFloat16 → halfvec 的精度损失转换 |
| 审计报告 | 输出控制台摘要 + JSON 报告文件 + 逐 Collection 日志 |

---

## 2. 架构与迁移流程

工具采用 **三阶段编排** 模型，由 `MigrationOrchestrator` 统一调度：

```
┌──────────────────────────────────────────────────────────────┐
│                     MigrationCli (入口)                       │
│         解析参数 → 加载配置 → 构造 MigrationConfig            │
└────────────────────────┬─────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────┐
│                  MigrationOrchestrator                        │
│                                                              │
│   Phase 1: Schema Migration (SchemaMigrator)                 │
│     └─ MilvusSchemaIntrospector → PgVectorSchemaGenerator    │
│        → JDBC 执行 CREATE EXTENSION / SCHEMA / TABLE / INDEX │
│                                                              │
│   Phase 2: Data Migration (SeaTunnelJobSubmitter)            │
│     └─ 渲染 milvus_to_pgvector.conf → 调用 seatunnel CLI     │
│        → Milvus Source → MilvusToPgVector Transform → Jdbc   │
│                                                              │
│   Phase 3: Validation (DataValidator)                        │
│     └─ RecordCountValidator + VectorSimilarityValidator      │
│        + SamplingValidator                                   │
└────────────────────────┬─────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────┐
│   MigrationProgressTracker (状态文件)  +  MigrationReport     │
│   {collection}_progress.json            {collection}_report.json │
└──────────────────────────────────────────────────────────────┘
```

**阶段说明：**

- **Schema 阶段**：内省 Milvus Collection 元数据，在 pgvector 侧创建扩展、Schema、表与向量索引。DDL 均为 `IF NOT EXISTS`，可幂等重跑。
- **Data 阶段**：通过 SeaTunnel 引擎执行批量任务，从 Milvus 读取数据，经 `MilvusToPgVector` Transform 转换后写入 pgvector。
- **Validation 阶段**：直连两端数据库（不依赖 SeaTunnel），对迁移结果做多维校验。

### 2.1 CDC 实时流式迁移 (STREAMING 模式)

除上述三阶段批处理迁移（`job.mode = "BATCH"`）外，工具还支持 **STREAMING 模式**——通过 `Milvus-CDC` Source 插件订阅 Milvus WAL 事件流，
实现全量快照 + 实时增量同步（含 **INSERT / DELETE / UPSERT**）。

**模式对比**：

| 特性 | BATCH 模式 | STREAMING 模式 |
|------|-----------|---------------|
| Source 插件 | `Milvus` | **`Milvus-CDC`** |
| job.mode | `BATCH` | `STREAMING` |
| 全量快照 | ✅ | ✅  (`startup_mode = "INITIAL"`) |
| 增量 INSERT | ❌ (需重新全量) | ✅ 实时捕获 |
| 增量 DELETE | ❌ | ✅ (V2 策略) |
| 增量 UPSERT | ❌ | ✅ (V2 策略) |
| 运行方式 | 一次性任务 | 常驻进程 |
| 适用场景 | 一次性全量迁移 | **生产实时同步、灾备、多活** |

**CDC 策略选择**：

| 策略 | 配置值 | 适用场景 | Delete 支持 |
|------|--------|---------|------------|
| event_stream V2 (⭐推荐) | `cdc_strategy = "event_stream"`, `cdc_use_streaming_node = true` | **Milvus 2.5.5+ standalone/集群** | ✅ |
| event_stream V1 | `cdc_strategy = "event_stream"`, `cdc_pchannel = "..."` | Milvus 2.4+ 集群模式（需 replication） | ✅ |
| grpc_replicate | `cdc_strategy = "grpc_replicate"` | Milvus 2.4+ 启用 CDC 的集群 | ✅ |
| polling_incremental | `cdc_strategy = "polling_incremental"` | 所有版本（降级方案） | ❌ |

**STREAMING 配置示例 (V2 策略)**：

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  Milvus-CDC {
    url = "http://localhost:19530"
    database = "default"
    collection = "my_vectors"
    collections = ["my_vectors"]

    cdc_strategy = "event_stream"
    cdc_use_streaming_node = true
    streaming_node_address = "localhost:22222"
    startup_mode = "INITIAL"

    batch_size = 10000
    incremental_batch_size = 2000
    poll_interval_ms = 100
    primary_key_field = "id"
  }
}

transform {
  MilvusToPgVector {
    pg_schema = "public"
    pg_table = "my_vectors"
    allow_precision_loss = true
  }
}

sink {
  Jdbc {
    url = "jdbc:postgresql://localhost:5432/vectordb?reWriteBatchedInserts=true"
    driver = "org.postgresql.Driver"
    user = "postgres"
    password = "postgres"
    compatible_mode = "pgvector"
    generate_sink_sql = true
    database = "vectordb"
    table = "public.my_vectors"
    batch_size = 2000
  }
}
```

**启动 CDC 同步任务**：

```bash
cd $SEATUNNEL_HOME
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# 本地模式（常驻进程）
nohup ./bin/seatunnel.sh \
    --config /path/to/milvus_to_pgvector_cdc.conf \
    -m local \
    > /path/to/logs/cdc.log 2>&1 &

# 监控同步状态
psql -h localhost -U postgres -d vectordb -c "SELECT COUNT(*) FROM public.my_vectors;"
tail -f /path/to/logs/cdc.log | grep "Poll completed"
```

**多 Collection 场景**（ADR-0004）：

每个 Collection 需要独立 STREAMING 任务，确保故障隔离和延迟独立：

```bash
#!/bin/bash
# 每个 collection 独立启动 CDC 任务
COLLECTIONS=("articles" "products" "images" "qa_pairs")

for col in "${COLLECTIONS[@]}"; do
  cat > /tmp/cdc_${col}.conf <<EOF
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}
source {
  Milvus-CDC {
    url = "http://localhost:19530"
    database = "default"
    collection = "${col}"
    cdc_strategy = "event_stream"
    cdc_use_streaming_node = true
    startup_mode = "INITIAL"
    batch_size = 10000
  }
}
transform { MilvusToPgVector { pg_schema = "public" pg_table = "${col}" } }
sink {
  Jdbc {
    url = "jdbc:postgresql://localhost:5432/vectordb?reWriteBatchedInserts=true"
    driver = "org.postgresql.Driver"
    user = "postgres" password = "postgres"
    compatible_mode = "pgvector"
    generate_sink_sql = true
    database = "vectordb" table = "public.${col}"
  }
}
EOF
  nohup ./bin/seatunnel.sh --config /tmp/cdc_${col}.conf -m local \
    > /path/to/logs/cdc_${col}.log 2>&1 &
  echo "Started CDC for collection: ${col}"
done
```

---

## 3. 环境前置条件

### 3.1 软件版本

| 组件 | 最低版本 | 推荐版本 |
| --- | --- | --- |
| JDK | 8 | 11 或 17 |
| Maven | 3.6+ | mvnd（加速构建） |
| Milvus | 2.3.6 | 2.6+ |
| PostgreSQL | 14 | 16 |
| pgvector | 0.7.0 | 0.8.0+ |
| SeaTunnel | 2.3.8 | 2.3.11 |

### 3.2 网络与权限

- 运行机器需可访问 Milvus gRPC/HTTP 端口（默认 19530）。
- 运行机器需可访问 PostgreSQL 端口（默认 5432）。
- PostgreSQL 账号需具备 `CREATE EXTENSION`、`CREATE SCHEMA`、`CREATE TABLE`、`CREATE INDEX`、目标库的 `INSERT/SELECT` 权限（建议 superuser 或库 owner）。
- Milvus 如启用鉴权，需提供 token。

### 3.3 pgvector 扩展准备

迁移工具会在 Schema 阶段自动执行 `CREATE EXTENSION IF NOT EXISTS vector`，但前提是 PostgreSQL 服务端已安装 pgvector 扩展二进制。安装方式（以 Debian 为例）：

```bash
apt install postgresql-16-pgvector
# 或在 PG 容器中
docker run -d --name pgvector -p 5432:5432 \
  -e POSTGRES_PASSWORD=postgres \
  pgvector/pgvector:pg16
```

---

## 4. 构建与打包

### 4.1 构建迁移工具包

在仓库根目录执行：

```bash
# 仅构建本模块及其依赖（跳过测试，最快）
mvnd clean package -pl seatunnel-connectors-v2/connector-milvus-pgvector-migration -am -Dmaven.test.skip=true
```

构建产物：

```
seatunnel-connectors-v2/connector-milvus-pgvector-migration/target/connector-milvus-pgvector-migration-2.3.11-SNAPSHOT.jar
```

### 4.2 构建完整发行包（含 SeaTunnel 引擎）

```bash
mvnd clean package -pl :seatunnel-dist -am -D"skip.ui"=true -Dmaven.test.skip=true -Prelease
```

发行包位于 `seatunnel-dist/target/apache-seatunnel-*.tar.gz`，解压后即为完整运行环境。

---

## 5. 部署方式

工具有两种部署形态：**CLI 一键编排** 与 **SeaTunnel 任务模板**。

### 5.1 部署 SeaTunnel 引擎（Data 阶段必需）

CLI 的 Data 阶段会通过子进程调用 `seatunnel` CLI，因此需要先部署 SeaTunnel 引擎。

```bash
# 解压发行包
tar -xzf apache-seatunnel-*.tar.gz -C /opt/
cd /opt/apache-seatunnel-*

# 设置环境变量
export SEATUNNEL_HOME=/opt/apache-seatunnel
export PATH=$SEATUNNEL_HOME/bin:$PATH

# 安装连接器（首次部署需下载 connector-milvus / connector-jdbc）
sh bin/install-plugin.sh 2.3.11
```

**安装迁移工具连接器：** 将构建产物拷贝到连接器目录：

```bash
cp connector-milvus-pgvector-migration-*.jar $SEATUNNEL_HOME/connectors/
cp connector-milvus-*.jar $SEATUNNEL_HOME/connectors/
cp connector-jdbc-*.jar $SEATUNNEL_HOME/connectors/
# PostgreSQL JDBC 驱动
cp postgresql-42.4.3.jar $SEATUNNEL_HOME/lib/
```

### 5.2 启动 SeaTunnel 集群（集群模式）

```bash
mkdir -p ./logs
./bin/seatunnel-cluster.sh -d
```

### 5.3 本地模式（无需常驻集群）

若不想启动常驻集群，CLI 会自动以 `seatunnel -c <conf>` 方式提交本地任务。只需确保 `seatunnel` 可执行文件在 `PATH` 或通过 `SEATUNNEL_HOME` 指定即可。

---

## 6. 使用方式

入口类：`org.apache.seatunnel.connectors.migration.milvus2pgvector.cli.MigrationCli`

### 6.1 直接运行 JAR

```bash
java -cp "connector-milvus-pgvector-migration-*.jar:$SEATUNNEL_HOME/lib/*:$SEATUNNEL_HOME/connectors/*" \
  org.apache.seatunnel.connectors.migration.milvus2pgvector.cli.MigrationCli [options]
```

### 6.2 命令行参数

| 参数 | 必填 | 默认值 | 说明 |
| --- | :---: | --- | --- |
| `--milvus-url <url>` | 是 | — | Milvus 地址，如 `http://localhost:19530` |
| `--milvus-collection <name>` | 是 | — | 要迁移的 Collection 名 |
| `--pg-url <jdbc-url>` | 是 | — | PostgreSQL JDBC URL，如 `jdbc:postgresql://localhost:5432/vectordb` |
| `--pg-user <user>` | 是 | — | PostgreSQL 用户名 |
| `--pg-password <pwd>` | 是 | — | PostgreSQL 密码 |
| `--milvus-token <token>` | 否 | 空 | Milvus 鉴权 token |
| `--milvus-database <db>` | 否 | `default` | Milvus 数据库（多租户场景） |
| `--pg-schema <schema>` | 否 | `public` | pgvector 目标 schema |
| `--pg-table <table>` | 否 | 同 collection | pgvector 目标表名 |
| `--batch-size <n>` | 否 | `1000` | 批大小 |
| `--parallelism <n>` | 否 | `1` | 并行度 |
| `--rate-limit <rows/s>` | 否 | `0` | 每秒最大行数，0=不限速 |
| `--skip-index-migration` | 否 | false | 跳过 pgvector 索引创建 |
| `--drop-existing-table` | 否 | false | 迁移前 DROP 目标表 |
| `--no-precision-loss` | 否 | false | 禁止 BFloat16→halfvec 精度损失 |
| `--validate-only` | 否 | false | 仅运行校验阶段 |
| `--schema-only` | 否 | false | 仅运行 Schema 阶段 |
| `--data-only` | 否 | false | 仅运行数据阶段 |
| `--resume` | 否 | false | 从上次断点续传 |
| `--log-dir <dir>` | 否 | `./migration-logs` | 审计日志目录 |
| `--sample-size <n>` | 否 | `100` | 校验采样数 |
| `--similarity-threshold <d>` | 否 | `0.9999` | 余弦相似度阈值 |
| `--seatunnel-home <dir>` | 否 | `$SEATUNNEL_HOME` | SeaTunnel 安装目录 |
| `--config <file>` | 否 | — | Properties 配置文件（CLI 参数覆盖文件值） |
| `--help` | 否 | — | 打印帮助 |

**互斥规则：**
- `--schema-only` 与 `--data-only` 互斥
- `--validate-only` 不能与 `--schema-only` / `--data-only` 同时使用

### 6.3 典型用法示例

```bash
# 1) 全量迁移（schema + data + validation）
java -cp ... MigrationCli \
  --milvus-url http://localhost:19530 \
  --milvus-collection my_collection \
  --pg-url jdbc:postgresql://localhost:5432/vectordb \
  --pg-user postgres --pg-password postgres \
  --batch-size 2000 --parallelism 2

# 2) 仅迁移 Schema（建表 + 建索引）
java -cp ... MigrationCli \
  --milvus-url http://localhost:19530 \
  --milvus-collection my_collection \
  --pg-url jdbc:postgresql://localhost:5432/vectordb \
  --pg-user postgres --pg-password postgres \
  --schema-only --drop-existing-table

# 3) 仅迁移数据（表已存在）
java -cp ... MigrationCli \
  --milvus-url http://localhost:19530 \
  --milvus-collection my_collection \
  --pg-url jdbc:postgresql://localhost:5432/vectordb \
  --pg-user postgres --pg-password postgres \
  --data-only

# 4) 仅校验（迁移已完成，单独跑校验）
java -cp ... MigrationCli \
  --milvus-url http://localhost:19530 \
  --milvus-collection my_collection \
  --pg-url jdbc:postgresql://localhost:5432/vectordb \
  --pg-user postgres --pg-password postgres \
  --validate-only --sample-size 500 --similarity-threshold 0.9999

# 5) 断点续传（某阶段失败后继续）
java -cp ... MigrationCli \
  --milvus-url http://localhost:19530 \
  --milvus-collection my_collection \
  --pg-url jdbc:postgresql://localhost:5432/vectordb \
  --pg-user postgres --pg-password postgres \
  --resume

# 6) 使用配置文件
java -cp ... MigrationCli --config /etc/vts/migration.properties

# 7) 限流迁移（保护生产库，每秒最多 5000 行）
java -cp ... MigrationCli \
  --milvus-url http://localhost:19530 \
  --milvus-collection my_collection \
  --pg-url jdbc:postgresql://localhost:5432/vectordb \
  --pg-user postgres --pg-password postgres \
  --rate-limit 5000 --batch-size 1000

# 8) 精度敏感场景（禁止 BFloat16→halfvec）
java -cp ... MigrationCli \
  --milvus-url http://localhost:19530 \
  --milvus-collection bf16_collection \
  --pg-url jdbc:postgresql://localhost:5432/vectordb \
  --pg-user postgres --pg-password postgres \
  --no-precision-loss
```

### 6.4 直接运行 SeaTunnel 任务（绕过 CLI）

也可跳过 CLI 编排，直接用 SeaTunnel 提交预置任务模板。模板位于 `src/main/resources/templates/`：

```bash
# 数据迁移任务
seatunnel --config $SEATUNNEL_HOME/config/milvus_to_pgvector.conf -m local \
  -i milvus.url=http://localhost:19530 \
  -i milvus.token= \
  -i milvus.database=default \
  -i milvus.collection=my_collection \
  -i pg.url=jdbc:postgresql://localhost:5432/vectordb \
  -i pg.user=postgres \
  -i pg.password=postgres \
  -i pg.database=vectordb \
  -i pg.schema=public \
  -i pg.table=my_collection \
  -i batch.size=1000
```

> 注意：直接运行 SeaTunnel 任务不会生成进度状态文件与审计报告，断点续传不可用。需要审计/续传能力请使用 CLI。

---

## 7. 示例配置

### 7.1 Properties 配置文件（`--config`）

文件格式为 `key=value`，键名与 CLI 选项去掉 `--` 后用点号连接。**CLI 参数优先级高于配置文件。**

`/etc/vts/migration.properties`：

```properties
# ---- Milvus 源端 ----
milvus.url=http://localhost:19530
milvus.token=
milvus.database=default
milvus.collection=my_collection

# ---- pgvector 目标端 ----
pg.url=jdbc:postgresql://localhost:5432/vectordb
pg.user=postgres
pg.password=postgres
pg.schema=public
# pg.table 留空则与 collection 同名
pg.table=

# ---- 性能 ----
batch.size=1000
parallelism=1
# 每秒最大行数；0 = 不限速
rate.limit=0

# ---- 行为 ----
skip.index.migration=false
drop.existing.table=false
# 为 true 时禁止 BFloat16 → halfvec 精度损失
no.precision.loss=false

# ---- 阶段控制（按需开启其一）----
# validate.only=false
# schema.only=false
# data.only=false
# resume=false

# ---- 校验 ----
sample.size=100
similarity.threshold=0.9999

# ---- 审计 ----
audit.log_dir=./migration-logs

# ---- SeaTunnel ----
# 留空则使用 SEATUNNEL_HOME 环境变量
seatunnel.home=
```

使用：

```bash
java -cp ... MigrationCli --config /etc/vts/migration.properties
```

### 7.2 SeaTunnel 数据迁移任务模板

`milvus_to_pgvector.conf`：

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Milvus {
    url = "${milvus.url}"
    token = "${milvus.token}"
    database = "${milvus.database}"
    collections = ["${milvus.collection}"]
    batch_size = ${batch.size}
  }
}

transform {
  MilvusToPgVector {
    pg_schema = "${pg.schema}"
    pg_table = "${pg.table}"
    allow_precision_loss = true
  }
}

sink {
  Jdbc {
    url = "${pg.url}"
    driver = "org.postgresql.Driver"
    user = "${pg.user}"
    password = "${pg.password}"
    compatible_mode = "pgvector"
    generate_sink_sql = true
    database = "${pg.database}"
    table = "${pg.schema}.${pg.table}"
  }
}
```

变量通过 `seatunnel -i key=value` 注入。

### 7.3 SeaTunnel 校验任务模板

`milvus_to_pgvector_validate.conf`（采样比对，结果输出到 Console）：

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Milvus {
    url = "${milvus.url}"
    token = "${milvus.token}"
    database = "${milvus.database}"
    collections = ["${milvus.collection}"]
    batch_size = 100
  }
}

transform {
  VectorValidation {
    pg_url = "${pg.url}"
    pg_user = "${pg.user}"
    pg_password = "${pg.password}"
    pg_table = "${pg.schema}.${pg.table}"
    sample_size = 100
    similarity_threshold = 0.9999
  }
}

sink {
  Console {}
}
```

> 高性能校验建议优先使用 CLI 的 `--validate-only` 模式，直连两端数据库，无需启动 SeaTunnel 任务。

### 7.4 多 Collection 批量迁移（Shell 脚本示例）

```bash
#!/bin/bash
SEATUNNEL_HOME=/opt/apache-seatunnel
JAR=connector-milvus-pgvector-migration-2.3.11-SNAPSHOT.jar
CLASSPATH="$JAR:$SEATUNNEL_HOME/lib/*:$SEATUNNEL_HOME/connectors/*"

COLLECTIONS=("articles" "products" "images" "qa_pairs")

for col in "${COLLECTIONS[@]}"; do
  echo "=== Migrating collection: $col ==="
  java -cp "$CLASSPATH" \
    org.apache.seatunnel.connectors.migration.milvus2pgvector.cli.MigrationCli \
    --milvus-url http://milvus:19530 \
    --milvus-collection "$col" \
    --pg-url jdbc:postgresql://pg:5432/vectordb \
    --pg-user postgres --pg-password postgres \
    --batch-size 2000 --parallelism 2 \
    --drop-existing-table \
    --log-dir ./migration-logs/$col
  echo "=== $col done (exit=$?) ==="
done
```

---

## 8. 类型映射参考

Schema 阶段会按以下规则将 Milvus 字段类型映射为 pgvector 列类型。

### 8.1 标量类型

| Milvus DataType | pgvector 列类型 |
| --- | --- |
| Bool | `BOOLEAN` |
| Int8 / Int16 | `SMALLINT` |
| Int32 | `INTEGER` |
| Int64 | `BIGINT` |
| Float | `REAL` |
| Double | `DOUBLE PRECISION` |
| String | `TEXT` |
| VarChar | `VARCHAR(N)`（缺省 65535） |
| JSON | `JSONB` |
| Geometry | `BYTEA` |
| Timestamptz | `TIMESTAMPTZ` |

### 8.2 向量类型

| Milvus DataType | pgvector 列类型 | 备注 |
| --- | --- | --- |
| FloatVector | `vector(N)` | N 为维度 |
| Float16Vector | `halfvec(N)` | |
| BFloat16Vector | `halfvec(N)` | 会损失精度，受 `--no-precision-loss` 控制 |
| BinaryVector | `bit(N)` | |
| SparseFloatVector | `sparsevec` | |

### 8.3 数组类型

| Milvus Array 元素类型 | pgvector 列类型 |
| --- | --- |
| Bool | `BOOLEAN[]` |
| Int8 / Int16 | `SMALLINT[]` |
| Int32 | `INTEGER[]` |
| Int64 | `BIGINT[]` |
| Float | `REAL[]` |
| Double | `DOUBLE PRECISION[]` |
| VarChar / String | `TEXT[]` |

### 8.4 约束

- Milvus 主键自动映射为 pgvector `PRIMARY KEY` 约束。
- 当 `--drop-existing-table=true` 时，DDL 前置 `DROP TABLE IF EXISTS`；否则使用 `CREATE TABLE IF NOT EXISTS`（幂等）。

---

## 9. 索引映射参考

`MilvusIndexConverter` 负责将 Milvus 索引定义转换为 pgvector `CREATE INDEX` DDL。

### 9.1 索引类型映射

| Milvus 索引类型 | pgvector 索引 | 说明 |
| --- | --- | --- |
| HNSW | `USING hnsw ... WITH (m, ef_construction)` | 参数透传 |
| IVF_FLAT / IVF_SQ8 | `USING ivfflat ... WITH (lists)` | nlist → lists |
| IVF_PQ / SCANN | `USING ivfflat ... WITH (lists=100)` | 退化映射，会告警 |
| AUTOINDEX | `USING hnsw`（默认 m=16, ef=64） | |
| FLAT | 不建索引 | 精确搜索 |
| DISKANN / GPU_* / BIN_* / SPARSE_* / INVERTED / TRIE / STL_SORT | 跳过 | pgvector 无等效，告警 |

### 9.2 度量类型 → ops class

| Milvus MetricType | pgvector ops class |
| --- | --- |
| L2 | `vector_l2_ops` |
| IP | `vector_ip_ops` |
| COSINE | `vector_cosine_ops` |
| HAMMING | `bit_hamming_ops` |
| JACCARD | `bit_jaccard_ops` |

### 9.3 HNSW 默认参数

当 AUTOINDEX 或参数缺失时使用默认值：

- `m = 16`
- `ef_construction = 64`
- IVF `lists = 100`

---

## 10. 数据一致性校验

Validation 阶段由 `DataValidator` 编排三个子校验器：

| 校验器 | 校验内容 | 通过条件 |
| --- | --- | --- |
| `RecordCountValidator` | Milvus 行数 vs pgvector 行数 | 完全相等 |
| `VectorSimilarityValidator` | 随机采样 N 条，比对两端向量的余弦相似度 | 每条相似度 ≥ `similarity.threshold` |
| `SamplingValidator` | 随机采样 N 条，逐字段比对标量值 | 所有字段一致 |

**参数：**
- `--sample-size`：采样数量（默认 100）。数据量大时建议调大至 500~1000。
- `--similarity-threshold`：相似度阈值（默认 0.9999）。FloatVector 精确迁移应为 1.0；halfvec/BFloat16 等低精度场景可适当下调。
- `--rate-limit`：校验阶段同样受令牌桶限流保护，避免压垮源库。

校验结果会写入 `ValidationReport`，并体现在控制台摘要与 JSON 报告中。

---

## 11. 断点续传

### 11.1 原理

`MigrationProgressTracker` 在 `{logDir}/{collection}_progress.json` 中记录三个阶段的状态：

```json
{
  "collectionName": "my_collection",
  "schemaStatus": "SUCCESS",
  "dataStatus": "FAILED",
  "validationStatus": "",
  "lastCheckpointPk": "",
  "rowsMigrated": 0,
  "rowsTotal": 0,
  "timestamp": "2026-06-26T10:00:00Z"
}
```

### 11.2 使用

当迁移中途失败时，追加 `--resume` 重新执行：

```bash
java -cp ... MigrationCli --config /etc/vts/migration.properties --resume
```

- 已标记为 `SUCCESS` 的阶段会被跳过。
- `FAILED` 或未执行的阶段会重新执行。
- Schema 阶段跳过时仍会重新 introspect 元数据供下游使用。

### 11.3 重置进度

如需从头开始，删除进度文件或使用 `--drop-existing-table`：

```bash
rm ./migration-logs/my_collection_progress.json
```

---

## 12. 审计日志与报告

### 12.1 日志文件

每个 Collection 迁移会在 `--log-dir` 目录生成：

| 文件 | 说明 |
| --- | --- |
| `{collection}_{yyyyMMdd_HHmmss}.log` | 逐行迁移日志（INFO/WARN/ERROR + 阶段事件） |
| `{collection}_progress.json` | 进度状态文件（支持续传） |
| `{collection}_report.json` | 最终迁移报告（JSON） |
| `migration_{yyyyMMdd_HHmmss}.conf` | Data 阶段渲染后的 SeaTunnel 任务文件 |

### 12.2 控制台报告示例

```
========== Migration Report ==========
Collection      : my_collection
PG Table        : my_collection
Start           : 2026-06-26T10:00:00Z
End             : 2026-06-26T10:15:32Z
Duration        : 932s
Rows Migrated   : 1000000
Schema Migration: SUCCESS
Data Migration  : SUCCESS
Validation      : PASSED
Overall         : SUCCESS
======================================
```

### 12.3 JSON 报告字段

```json
{
  "collectionName": "my_collection",
  "pgTable": "my_collection",
  "startTime": "...",
  "endTime": "...",
  "durationSeconds": 932,
  "totalRowsMigrated": 1000000,
  "schemaMigrationResult": "SUCCESS",
  "dataMigrationResult": "SUCCESS",
  "validationPassed": true,
  "success": true
}
```

---

## 13. 性能调优

### 13.1 实测性能基准

> 测试环境：Milvus 2.6.9 standalone (localhost:19530), PostgreSQL 14 + pgvector (localhost:5432), JDK 17

| 数据规模 | 同步模式 | 耗时 | 吞吐量 | 关键配置 |
|----------|---------|------|--------|---------|
| 500 行 | 全量快照 | ~5s | ~100 rows/s | 默认配置 |
| 50,000 行 | 全量快照 | ~12s | **~4,200 rows/s** | batch_size=10000, JDBC batch_size=2000 |
| 1,000 行 upsert | 增量 WAL | ~2s | — | 实时捕获 Delete+Insert 对 |
| 4,000 行 insert | 增量 WAL | ~3s | — | 实时写入 |
| 5,000 行 delete | 增量 WAL | ~5s | — | 每 1000 PK 一批 |

### 13.2 BATCH 模式关键参数

| 参数 | 建议 | 说明 |
| --- | --- | --- |
| `--batch-size` | 1000~10000 | 过小则 RPC 开销大；过大则内存占用高 |
| `--parallelism` | 2~8 | 受 SeaTunnel slot 数限制；向量库 IO 密集型，并非越高越好 |
| `--rate-limit` | 按源库承载能力设置 | 保护生产 Milvus，如 5000 行/秒 |

### 13.3 STREAMING 模式关键参数

| 参数 | 默认值 | 推荐值 | 影响 |
|------|--------|--------|------|
| `batch_size` (source) | 1000 | **5000-10000** | 全量快照读取速度，大值减少 RPC 次数 |
| `incremental_batch_size` | 500 | **1000-2000** | 增量每批最大事件数 |
| JDBC `batch_size` (sink) | 100 | **1000-2000** | 数据库写入批量，大值减少 SQL 执行次数 |
| JDBC `reWriteBatchedInserts` | false | **true** | PostgreSQL INSERT 批量重写，2-3x 写入提升 |
| `poll_interval_ms` | 1000 | **100** | 增量事件响应延迟，低值更快感知变更 |
| Snapshot `assignPendingSplits` | — | — | **代码层面**：分配后立即清除，防止重复全量扫描 |

### 13.4 SeaTunnel 引擎侧

- 调整 `config/jvm_options`、`jvm_worker_options` 的堆内存（向量数据内存占用大）。
- 集群模式下适当增加 worker 数量。
- 大表迁移建议分批跑（按 Collection 拆分任务）。

### 13.5 pgvector 侧

- 大表先建表灌数据，**数据导入完成后再建索引**（用 `--skip-index-migration` 跑数据，随后单独建索引）。
- 临时调大 `maintenance_work_mem` 加速 HNSW 索引构建：
  ```sql
  SET maintenance_work_mem = '2GB';
  ```
- 关闭目标表 autovacuum 直至导入完成。

---

## 14. 错误码

| 错误码 | 含义 | 常见原因 |
| --- | --- | --- |
| `MIGRATE-01` | Schema 内省失败 | Milvus 不可达 / Collection 不存在 / token 错误 |
| `MIGRATE-02` | 不支持的 Milvus 类型 | 遇到未映射的 DataType |
| `MIGRATE-03` | 不支持的 Milvus 索引类型 | 索引类型无 pgvector 等效（仅告警，非致命） |
| `MIGRATE-04` | DDL 执行失败 | pgvector 扩展未安装 / 权限不足 / 表已存在且未 drop |
| `MIGRATE-05` | 数据校验失败 | 行数不等 / 相似度低于阈值 / 字段值不一致 |
| `MIGRATE-06` | 校验查询失败 | 连接断开 / SQL 异常 |
| `MIGRATE-07` | 精度损失转换被禁止 | BFloat16 → halfvec 且 `--no-precision-loss` 开启 |
| `MIGRATE-08` | 限流等待超时 | `--rate-limit` 设置过低，acquire 超时（默认 60s） |
| `MIGRATE-09` | 配置无效 | 缺失必填参数 / 互斥参数冲突 |
| `MIGRATE-10` | 编排步骤失败 | SeaTunnel 进程启动失败 / 任务异常退出 |

---

## 15. 常见问题

### Q1：Data 阶段报 "SeaTunnel job exited with code 1"

- 检查 `migration-logs/{collection}_*.log` 中的 `[seatunnel]` 前缀输出。
- 确认 `SEATUNNEL_HOME` 或 `--seatunnel-home` 指向正确的安装目录。
- 确认 `connectors/` 下存在 `connector-milvus-*.jar`、`connector-jdbc-*.jar`，`lib/` 下存在 `postgresql-*.jar`。
- 确认 SeaTunnel 集群已启动（集群模式）或 `seatunnel` 可在 PATH 中找到（本地模式）。

### Q2：Schema 阶段报 "CREATE EXTENSION vector" 失败

pgvector 扩展二进制未安装到 PostgreSQL 服务端。请在 PG 服务端安装 pgvector 后重试。仅安装客户端 `vector` 类型不够。

### Q3：校验阶段相似度不达标

- FloatVector 迁移应为 1.0；若低于 1.0 检查 Transform 是否启用了精度损失转换。
- halfvec / BFloat16 场景需适当下调 `--similarity-threshold`（如 0.99）。
- 采样命中率低时可增大 `--sample-size`。

### Q4：BFloat16 Collection 迁移报 MIGRATE-07

BFloat16 → halfvec 会损失精度。若可接受，去掉 `--no-precision-loss`；若不可接受，需在 Milvus 侧先转换为 FloatVector。

### Q5：如何只重新跑数据阶段

```bash
java -cp ... MigrationCli --config ... --data-only --drop-existing-table
```

### Q6：磁盘占用大

`--log-dir` 下会累积每次运行的日志和渲染后的 conf 文件，可定期清理历史文件（保留最新的 `_progress.json` 和 `_report.json`）。

---

## 16. 测试

### 16.1 单元测试

```bash
./mvnw test -pl seatunnel-connectors-v2/connector-milvus-pgvector-migration \
  -Dmaven.test.skip=false -DskipUT=false
```

覆盖：类型映射、Schema 生成、索引转换、Transform 转换、令牌桶限流。

### 16.2 本地 E2E 测试（连接已部署的 Milvus + PostgreSQL）

前提：本地已运行 Milvus（`localhost:19530`）与 PostgreSQL（`localhost:5432`，用户 `zhangqiang`，trust 认证），并已安装 pgvector 扩展。

```bash
./mvnw test -pl seatunnel-connectors-v2/connector-milvus-pgvector-migration \
  -Dtest=MilvusToPgVectorLocalE2E \
  -Dmaven.test.skip=false -DskipUT=false -DfailIfNoTests=false
```

测试会自动创建临时数据库 `vts_migration_test`，写入 500 行 128 维向量，执行 Schema 迁移 + 数据校验，结束后清理。

### 16.3 Testcontainers E2E 测试（需 Docker）

```bash
./mvnw test -pl seatunnel-connectors-v2/connector-milvus-pgvector-migration \
  -Dtest=MilvusToPgVectorE2E,MilvusToPgVectorResumeE2E \
  -Dmaven.test.skip=false -DskipUT=false \
  -Dmigration.e2e.enabled=true -DfailIfNoTests=false
```

自动拉起 Milvus（`milvusdb/milvus:v2.6-latest`）与 pgvector（`pgvector/pgvector:pg16`）容器，覆盖完整迁移流程与断点续传场景。

---

## 附录：模块结构

```
connector-milvus-pgvector-migration/
├── src/main/java/.../milvus2pgvector/
│   ├── cli/                  # CLI 入口与编排
│   │   ├── MigrationCli.java
│   │   ├── MigrationCliArgs.java
│   │   ├── MigrationOrchestrator.java
│   │   └── SeaTunnelJobSubmitter.java
│   ├── config/               # MigrationConfig
│   ├── schema/               # Schema 内省、DDL 生成、索引转换、类型映射
│   ├── transform/            # SeaTunnel Transform（MilvusToPgVector）
│   ├── validation/           # 数据校验器
│   ├── audit/                # 日志、进度跟踪、报告
│   ├── internal/             # 令牌桶限流器
│   └── exception/            # 错误码与异常
├── src/main/resources/templates/
│   ├── migration-template.yaml           # 配置模板（YAML 可读版）
│   ├── milvus_to_pgvector.conf           # SeaTunnel 数据迁移任务模板
│   └── milvus_to_pgvector_validate.conf  # SeaTunnel 校验任务模板
└── src/test/java/.../milvus2pgvector/
    ├── MilvusPgVectorTestBase.java       # Testcontainers 测试基类
    ├── MilvusToPgVectorE2E.java          # 完整 E2E（Docker）
    ├── MilvusToPgVectorResumeE2E.java    # 断点续传 E2E（Docker）
    └── MilvusToPgVectorLocalE2E.java     # 本地 E2E（连接已部署服务）
```
