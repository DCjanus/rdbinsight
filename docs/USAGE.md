# RDBInsight Usage Guide (English)

English | [中文](USAGE.zh_CN.md)

RDBInsight converts Redis RDB snapshots into structured data for OLAP analysis, supporting output to ClickHouse or Parquet and generating offline HTML reports.

For CLI syntax and output formats, run `rdbinsight --help` or `rdbinsight <subcommand> --help`.

---

## Core Concepts

- Cluster (cluster): Distinguishes different business clusters; typically user-defined. In Codis mode, the cluster name can be fetched automatically from Dashboard product name; all other modes require manual specification.
- Batch (batch): RFC3339 timestamp label representing one import; defaults to current time.
- Instance (instance): Source node identifier (e.g., `IP:PORT`).
- Source (source): Input mode determining where data comes from.
- Output (output): Output mode determining where data is written.

---

## Quick Start

Minimal example (ClickHouse import):

```bash
rdbinsight dump from-cluster \
  --nodes 127.0.0.1:7000,127.0.0.1:7001 \
  --cluster your_cluster \
  into-clickhouse \
  --url http://username:password@127.0.0.1:8123?database=rdbinsight \
  --auto-create-tables
```

Generate HTML report (from latest batch in ClickHouse):

```bash
rdbinsight report from-clickhouse \
  --cluster your_cluster \
  --url http://username:password@127.0.0.1:8123?database=rdbinsight
```

---

## Command Overview

Top-level commands:

- `dump`: Parse RDB and write to storage
- `report`: Generate interactive HTML report
- `misc`: Utilities (print ClickHouse DDL, generate shell completion)

Global flag:

- `-v/--verbose`: Verbose logging

### dump: Export data

Common flags (effective under `dump`):

- `--prometheus <url>`: Optional Prometheus endpoint (only `http://host:port`). Example: `http://0.0.0.0:9901`
- `--concurrency <N>`: Parallelism (default: CPU/2, range 1–8)

Pick one source under `dump`:

#### from-standalone

Export from a standalone Redis:

```bash
rdbinsight dump from-standalone \
  --addr 127.0.0.1:6379 \
  --cluster your_cluster \
  [--username <user>] \
  [--password <pass>] \
  [--batch-timestamp <RFC3339>] \
  into-clickhouse --url <CH_URL> [--auto-create-tables] [--proxy-url <HTTP_PROXY>]
```

Flags:

- `--addr`: Redis address (required)
- `--cluster`: Cluster name (required)
- `--username`: Username (optional, default empty)
- `--password`: Password (optional)
- `--batch-timestamp`: RFC3339, defaults to now

#### from-cluster

Export from a Redis Cluster:

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

Flags:

- `--nodes`: Comma-separated `host:port` list (required)
- `--cluster`: Cluster name (required)
- `--username`: Username (optional, default empty)
- `--password`: Password (optional)
- `--require-slave`: Read from slave nodes only (optional)
- `--batch-timestamp`: RFC3339, defaults to now

#### from-file

Export from an RDB file:

```bash
rdbinsight dump from-file \
  --path /data/dump.rdb \
  --cluster your_cluster \
  --instance 192.168.1.10:6379 \
  [--batch-timestamp <RFC3339>] \
  into-parquet --dir /tmp/rdb_parquet
```

Flags:

- `--path`: RDB file path (required)
- `--cluster`: Cluster name (required)
- `--instance`: Instance identifier (required, e.g., `IP:PORT`)
- `--batch-timestamp`: RFC3339, defaults to now

Note: File mode uses fixed concurrency of 1.

#### from-codis

Export from a Codis cluster:

```bash
rdbinsight dump from-codis \
  --dashboard http://127.0.0.1:11080 \
  [--cluster your_cluster] \
  [--password <pass>] \
  [--require-slave] \
  [--batch-timestamp <RFC3339>] \
  into-clickhouse --url <CH_URL> [--auto-create-tables] [--proxy-url <HTTP_PROXY>]
```

Flags:

- `--dashboard`: Codis Dashboard address (required)
- `--cluster`: Cluster name (optional; will be fetched from Dashboard product name if omitted)
- `--password`: Password (optional)
- `--require-slave`: Read from slave nodes only (optional)
- `--batch-timestamp`: RFC3339, defaults to now

### output

Pick one output under `dump` (choose one):

#### into-clickhouse

```bash
... into-clickhouse \
  --url http://user:pass@127.0.0.1:8123?database=rdbinsight \
  [--auto-create-tables] \
  [--proxy-url http://127.0.0.1:7890]
```

Flags:

- `--url`: ClickHouse URL (must include explicit port and `?database=<db>`), e.g., `http[s]://[user[:pass]@]host:8123?database=<db>`
- `--auto-create-tables`: Auto-create tables if missing
- `--proxy-url`: HTTP proxy (optional)

Tip: `rdbinsight misc clickhouse-schema` prints recommended DDLs (`sql/01_*`, `02_*`, `03_*`).

#### into-parquet

```bash
... into-parquet \
  --dir /path/to/output \
  [--compression zstd|snappy|gzip] \
  [--max_run_rows 131072]
```

Flags:

- `--dir`: Output directory (required)
- `--compression`: Final merged file compression (default `zstd`)
- `--max_run_rows`: Max rows per run segment (default `131072`)

---

### report: Generate HTML report

#### from-clickhouse

```bash
rdbinsight report from-clickhouse \
  --cluster your_cluster \
  [--batch 2024-01-01T00:00:00Z] \
  [--output ./report.html] \
  --url http://user:pass@127.0.0.1:8123?database=rdbinsight \
  [--proxy-url http://127.0.0.1:7890]
```

Flags:

- `--cluster`: Cluster name (required)
- `--batch`: RFC3339 batch timestamp (optional; defaults to the latest batch in ClickHouse)
- `--output`: HTML output path (optional; default `rdb_report_<cluster>_<batch>.html`)
- `--url`: ClickHouse URL (required)
- `--proxy-url`: HTTP proxy (optional)

#### from-parquet

```bash
rdbinsight report from-parquet \
  --dir /path/to/parquet \
  --cluster your_cluster \
  [--batch 2024-01-01T00:00:00Z] \
  [--output ./report.html]
```

Flags:

- `--dir`: Base directory with structure `cluster=<name>/batch=<slug>` (required)
- `--cluster`: Cluster name (required)
- `--batch`: Batch slug (optional; if omitted, picks the latest by lexicographical order)
- `--output`: HTML output path (optional)

---

### misc: Utilities

#### clickhouse-schema

Print recommended ClickHouse DDLs for `redis_records_raw`, `import_batches_completed`, and `redis_records_view`:

```bash
rdbinsight misc clickhouse-schema
```

#### completion

Generate shell completion script (auto-detect or specify shell):

```bash
rdbinsight misc completion [--shell bash|zsh|fish|powershell|elvish]
```

---

## Environment Variables

Optional environment variables (equivalent to CLI flags):

- `RDBINSIGHT_PROMETHEUS`: same as `dump --prometheus`
- `RDBINSIGHT_CONCURRENCY`: same as `dump --concurrency`
- `RDBINSIGHT_BATCH`: same as `--batch`/`--batch-timestamp`
- `RDBINSIGHT_CLICKHOUSE_URL`: same as `--url`
- `RDBINSIGHT_CLICKHOUSE_AUTO_CREATE_TABLES`: same as `--auto-create-tables`
- `RDBINSIGHT_CLICKHOUSE_PROXY_URL`: same as `--proxy-url`
- `RDBINSIGHT_CLUSTER`: same as `--cluster`
- `RDBINSIGHT_REPORT_OUTPUT`: same as `--output`
- `RDBINSIGHT_SHELL`: same as `misc completion --shell`

---

## FAQ

- Batch timestamp format?
  - RFC3339, e.g., `2024-01-01T00:00:00Z`. Defaults to current UTC (dump) or latest batch auto-detection (report from-clickhouse).
- Must the ClickHouse URL include port and database?
  - Yes. It must include the explicit port and `?database=<db>` query parameter.
- How is the latest Parquet batch chosen?
  - Under `--dir/cluster=<name>/`, the latest directory by lexicographical order of `batch=<slug>` is picked.
- Progress and monitoring?
  - `--verbose` for more logs; `--prometheus` exposes `/metrics` (http only).
- Performance and concurrency?
  - `--concurrency` defaults to CPU/2 (1–8). File mode is fixed at 1.
