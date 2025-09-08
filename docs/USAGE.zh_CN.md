# RDBInsight 使用手册（中文）

中文 | [English](USAGE.md)

RDBInsight 是一个将 Redis RDB 快照解析为结构化数据的工具，并支持将结果导入 ClickHouse 或输出为 Parquet，以便进行灵活的 OLAP 分析与离线可视化报告。

所有命令的用法与输出格式请参见 `rdbinsight --help` 或 `rdbinsight <子命令> --help`。

---

## 核心概念

- 集群（cluster）：用于区分不同业务集群的数据，通常由用户自定义；在 Codis 模式下可从 Dashboard 的 product name 自动获取，其它模式需手动指定。
- 批次（batch）：一次导入的时间戳标签（RFC3339），用于区分不同时间点的数据；缺省为当前时间。
- 实例（instance）：来源节点标识（如 `IP:PORT`）。
- 输入（source）：数据的来源形式，如单机 Redis、Redis Cluster、RDB 文件、Codis 集群。
- 输出（output）：数据的输出形式，如 ClickHouse、Parquet。

---

## 快速开始

最小示例（导入 ClickHouse）：

```bash
rdbinsight dump from-cluster \
  --nodes 127.0.0.1:7000,127.0.0.1:7001 \
  --cluster your_cluster \
  into-clickhouse \
  --url http://username:password@127.0.0.1:8123?database=rdbinsight \
  --auto-create-tables
```

生成 HTML 报告（基于 ClickHouse 中最新批次）：

```bash
rdbinsight report from-clickhouse \
  --cluster your_cluster \
  --url http://username:password@127.0.0.1:8123?database=rdbinsight
```

---

## 子命令概览

顶层结构：

- `dump`：解析 RDB 并输出到指定存储
- `report`：生成交互式 HTML 报告
- `misc`：实用工具（打印建表 SQL、生成 shell 补全）

通用开关：

- `-v/--verbose`：启用更详细日志

### dump：导出数据

通用参数（对 `dump` 生效）：

- `--prometheus <url>`：可选，Prometheus 指标端点（仅支持 `http://host:port`），示例：`http://0.0.0.0:9901`
- `--concurrency <N>`：并发度（默认：CPU 核心数的一半，范围 1-8）。

`dump` 下级来源（四选一）：

#### from-standalone

从单机 Redis 导出：

```bash
rdbinsight dump from-standalone \
  --addr 127.0.0.1:6379 \
  --cluster your_cluster \
  [--username <user>] \
  [--password <pass>] \
  [--batch-timestamp <RFC3339>] \
  into-clickhouse --url <CH_URL> [--auto-create-tables] [--proxy-url <HTTP_PROXY>]
```

参数：

- `--addr`：Redis 地址（必填）
- `--cluster`：集群名（必填，用于维度字段）
- `--username`：用户名（可选，默认空）
- `--password`：密码（可选）
- `--batch-timestamp`：RFC3339 时间戳，默认当前时间

#### from-cluster

从 Redis Cluster 导出：

```bash
rdbinsight dump from-cluster \
  --nodes 10.0.0.1:7000,10.0.0.2:7001 \
  --cluster your_cluster \
  [--username <user>] \
  [--password <pass>] \
  [--require-slave] \
  [--batch-timestamp <RFC3339>] \
  into-clickhouse --url <CH_URL> [--auto-create-tables] [--proxy-url <HTTP_PROXY>]
```

参数：

- `--nodes`：逗号分隔的节点 `host:port` 列表（必填）
- `--cluster`：集群名（必填）
- `--username`：用户名（可选，默认空）
- `--password`：密码（可选）
- `--require-slave`：默认优先从从节点读取；若某个分片无从节点，则回退到其主节点。若显式指定该参数，则强制仅从从节点读取；若任一分片无从节点则直接报错。
- `--batch-timestamp`：RFC3339 时间戳，默认当前时间

#### from-file

从 RDB 文件导出：

```bash
rdbinsight dump from-file \
  --path /data/dump.rdb \
  --cluster your_cluster \
  --instance 192.168.1.10:6379 \
  [--batch-timestamp <RFC3339>] \
  into-parquet --dir /tmp/rdb_parquet
```

参数：

- `--path`：RDB 文件路径（必填）
- `--cluster`：集群名（必填）
- `--instance`：实例标识（必填，例如 `IP:PORT`）
- `--batch-timestamp`：RFC3339 时间戳，默认当前时间

说明：文件模式的并发度固定为 1。

#### from-codis

从 Codis 集群导出：

```bash
rdbinsight dump from-codis \
  --dashboard http://127.0.0.1:11080 \
  [--cluster your_cluster] \
  [--password <pass>] \
  [--require-slave] \
  [--batch-timestamp <RFC3339>] \
  into-clickhouse --url <CH_URL> [--auto-create-tables] [--proxy-url <HTTP_PROXY>]
```

参数：

- `--dashboard`：Codis Dashboard 地址（必填）
- `--cluster`：集群名（可选，未提供时将尝试从 Dashboard 获取）
- `--password`：密码（可选）
- `--require-slave`：默认优先从从节点读取；若某个分片无从节点，则回退到其主节点。若显式指定该参数，则强制仅从从节点读取；若任一分片无从节点则直接报错。
- `--batch-timestamp`：RFC3339 时间戳，默认当前时间

### 输出（output）

在 `dump` 下选择一种输出（两选一）：

#### into-clickhouse

```bash
... into-clickhouse \
  --url http://user:pass@127.0.0.1:8123?database=rdbinsight \
  [--auto-create-tables] \
  [--proxy-url http://127.0.0.1:7890]
```

参数：

- `--url`：ClickHouse 服务 URL（必须包含端口与 `?database=<db>`）。示例：`http[s]://[user[:pass]@]host:8123?database=<db>`
- `--auto-create-tables`：若不存在则自动建表
- `--proxy-url`：HTTP 代理 URL（可选）

辅助命令：`rdbinsight misc clickhouse-schema` 可打印推荐的建表 SQL（`sql/01_*`、`02_*`、`03_*`）。

#### into-parquet

```bash
... into-parquet \
  --dir /path/to/output \
  [--compression zstd|snappy|gzip] \
  [--max_run_rows 131072]
```

参数：

- `--dir`：Parquet 输出目录（必填）
- `--compression`：合并后最终文件压缩算法，默认 `zstd`
- `--max_run_rows`：单段运行的最大行数，默认 `131072`

---

### report：生成 HTML 报告

#### from-clickhouse

```bash
rdbinsight report from-clickhouse \
  --cluster your_cluster \
  [--batch 2024-01-01T00:00:00Z] \
  [--output ./report.html] \
  --url http://user:pass@127.0.0.1:8123?database=rdbinsight \
  [--proxy-url http://127.0.0.1:7890]
```

参数：

- `--cluster`：集群名（必填）
- `--batch`：RFC3339 批次时间（可选，缺省为从 ClickHouse 查询该集群的最新批次）
- `--output`：HTML 输出路径（可选，未提供时默认生成 `rdb_report_<cluster>_<batch>.html`）
- `--url`：ClickHouse URL（必填，与上节相同约束）
- `--proxy-url`：HTTP 代理（可选）

#### from-parquet

```bash
rdbinsight report from-parquet \
  --dir /path/to/parquet \
  --cluster your_cluster \
  [--batch 2024-01-01T00:00:00Z] \
  [--output ./report.html]
```

参数：

- `--dir`：Parquet 基础目录（必填），内部结构包含 `cluster=<name>/batch=<slug>`
- `--cluster`：集群名（必填）
- `--batch`：批次 slug（可选，未提供时按目录字典序选择最新）
- `--output`：HTML 输出路径（可选）

---

### misc：实用工具

#### clickhouse-schema

打印推荐的 ClickHouse DDL（用于 `redis_records_raw`、`import_batches_completed` 与 `redis_records_view`）：

```bash
rdbinsight misc clickhouse-schema
```

#### completion

生成 shell 补全脚本（自动探测或指定 shell）：

```bash
rdbinsight misc completion [--shell bash|zsh|fish|powershell|elvish]
```

---

## 环境变量

可选的环境变量（与 CLI 等效）：

- `RDBINSIGHT_PROMETHEUS`：同 `dump --prometheus`
- `RDBINSIGHT_CONCURRENCY`：同 `dump --concurrency`
- `RDBINSIGHT_BATCH`：同各处 `--batch`/`--batch-timestamp`
- `RDBINSIGHT_CLICKHOUSE_URL`：同 `--url`
- `RDBINSIGHT_CLICKHOUSE_AUTO_CREATE_TABLES`：同 `--auto-create-tables`
- `RDBINSIGHT_CLICKHOUSE_PROXY_URL`：同 `--proxy-url`
- `RDBINSIGHT_CLUSTER`：同 `--cluster`
- `RDBINSIGHT_REPORT_OUTPUT`：同 `--output`
- `RDBINSIGHT_SHELL`：同 `misc completion --shell`

---

## 常见问题（FAQ）

- 批次时间格式要求？
  - 使用 RFC3339，例如 `2024-01-01T00:00:00Z`。不传时默认使用当前 UTC 时间（dump）或自动查询最新批次（report from-clickhouse）。
- ClickHouse URL 不能省略端口或 database 吗？
  - 必须包含端口与 `?database=<db>` 查询参数，否则会报错。
- Parquet 报告如何选择最新批次？
  - 在 `--dir/cluster=<name>/` 下按目录名字典序选取最后一个 `batch=<slug>`。
- 进度与监控？
  - `--verbose` 可获得更详细日志；`--prometheus` 可暴露 `/metrics` 指标（仅 http 协议）。
- 性能与并发？
  - `--concurrency` 默认 `CPU/2`（范围 1-8）。文件模式固定为 1。
