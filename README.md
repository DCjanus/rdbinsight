# RDBInsight

[![CI](https://github.com/DCjanus/rdbinsight/actions/workflows/ci.yml/badge.svg)](https://github.com/DCjanus/rdbinsight/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/DCjanus/rdbinsight/graph/badge.svg?token=KXVIGig53g)](https://codecov.io/gh/DCjanus/rdbinsight)
[![dependency status](https://deps.rs/repo/github/dcjanus/rdbinsight/status.svg)](https://deps.rs/repo/github/dcjanus/rdbinsight)
[![Docker Image Version](https://ghcr-badge.egpl.dev/dcjanus/rdbinsight/latest_tag?color=%2344cc11&ignore=latest&label=docker+image&trim=)](https://github.com/DCjanus/rdbinsight/pkgs/container/rdbinsight)

RDBInsight is a Redis RDB analysis tool. It reads Redis snapshots from live Redis instances, Redis Cluster, Codis, or local RDB files, then writes compact key metadata to ClickHouse or Parquet for repeatable memory and data-distribution investigations.

English | [中文](README.zh_CN.md)

## Important Notice

This project is under active development. Backward compatibility is not guaranteed between versions, and interfaces, CLI flags, and output formats may change without notice. Do not use this software in production environments.

## Usage

See the full usage guide: [docs/USAGE.md](docs/USAGE.md)

Official Docker images are published to [ghcr.io](https://github.com/DCjanus/rdbinsight/pkgs/container/rdbinsight):

```bash
docker run --rm ghcr.io/dcjanus/rdbinsight:v0.2.0 --help
```

Example: parse a local RDB file into Parquet:

```bash
docker run --rm \
  -v "$PWD:/work" \
  ghcr.io/dcjanus/rdbinsight:v0.2.0 \
  dump from-file \
  --path /work/dump.rdb \
  --cluster your_cluster \
  --instance 127.0.0.1:6379 \
  into-parquet \
  --dir /work/rdb_parquet
```

Precompiled host binaries are not published at the moment. If you need to run without Docker, build from source.

## Why RDBInsight?

For common Redis issues such as large keys, mature tools already exist. In production environments, however, more intricate and less clearly defined situations still arise, for example:

- A data-cleanup script terminates unexpectedly, resulting in uneven memory usage across nodes.
- Improper hash-tag usage leads to imbalanced data distribution that regular monitoring does not immediately reveal.
- Minor defects in business logic accumulate over time, producing large volumes of obsolete data.

Addressing these atypical problems often involves writing one-off scripts for scanning and analysis. That is repetitive, slow on large clusters, and hard to reuse.

## Design Philosophy: Enabling, Not Prescribing

RDBInsight focuses on empowering users with flexible diagnostic capabilities rather than enforcing a fixed checklist.

By parsing RDB data into a stable analytical schema, users can query the same dataset repeatedly instead of rescanning Redis:

- Load metadata into ClickHouse for ad-hoc SQL analysis.
- Store metadata as Parquet for offline processing and report generation.
- Generate self-contained HTML reports with prefix flame graphs.
- Turn ad-hoc troubleshooting steps into repeatable analysis workflows.

## Features

- **Incremental parser** – Parses RDB files with a near-constant memory footprint and can handle datasets containing very large keys.
- **Multiple sources** – Reads RDB data from standalone Redis, Redis Cluster, Codis, and local RDB files.
- **Modern Redis compatibility** – Supports recent Redis RDB encodings including Redis 8.6 streams and Redis 8.8 array/stream records.
- **Flexible outputs** – Writes structured metadata to ClickHouse or Parquet.
- **Self-contained HTML report** – Generates a single-file HTML report (including prefix flame graphs) that can be viewed offline. [Sample report](https://dcjanus.github.io/rdbinsight/)

## Data Model

Only the fields essential for analysis are extracted:

- `cluster`: Source cluster name
- `batch`: Import batch timestamp (nanosecond precision)
- `instance`: Instance name (IP:PORT)
- `db`: Database number
- `key`: Key name
- `type`: Data type
- `member_count`: Number of elements in collection types
- `rdb_size`: Bytes occupied in RDB
- `encoding`: Encoding method
- `expire_at`: Expiration time
- `idle_seconds`: LRU idle time (seconds)
- `freq`: LFU access frequency
- `codis_slot`: Codis Slot ID (0-1023), only for Codis clusters
- `redis_slot`: Redis Cluster Slot ID (0-16383), only for Redis clusters

## Example: Validating a Prefix Whitelist in a 1-TB Cluster

Assume a production Redis cluster consumes **1 TB** of memory and the application team provides five prefixes as a whitelist. They want to:

1. Confirm that only these five prefixes exist and list keys with any other prefix.
2. Count the keys for each whitelisted prefix to estimate potential cleanup gains or capacity requirements.

Traditional approaches involve custom scripts that execute `SCAN` on every instance—a slow process that offers little reusability.

With RDBInsight:

1. Parse the RDB data and load the metadata into ClickHouse.
2. Use standard SQL to obtain the results:

```sql
-- Whitelist defined as a temporary table or array constant
WITH ['bizA:', 'bizB:', 'bizC:', 'bizD:', 'bizE:'] AS whitelist

-- 1. Prefixes not in the whitelist
SELECT
    substring(key, 1, position(key, ':')) AS prefix,
    key
FROM redis_records_view
WHERE prefix NOT IN whitelist
LIMIT 100;

-- 2. Key count per whitelisted prefix
WITH
    '^(bizA:|bizB:|bizC:|bizD:|bizE:)' AS re
SELECT
    extract(key, re) AS prefix,
    COUNT()          AS key_cnt
FROM redis_records_view
WHERE cluster = 'your_cluster'
  AND batch = parseDateTime64BestEffort('2024-01-01T00:00:00Z', 9, 'UTC')
  AND extract(key, re) != ''
GROUP BY prefix
ORDER BY key_cnt DESC;
```

The query-based workflow removes the need for Redis-side commands, and similar tasks can be handled by adjusting the SQL.

## TODO

- [ ] Add end-to-end Redis Cluster source integration tests that exercise `dump from-cluster` shard discovery, replica selection, and record output.
- [ ] Add Codis cluster integration tests with a real Codis dashboard and Redis backend to verify dashboard discovery and Codis slot metadata.
- [ ] Implement a DuckDB-based fallback for Parquet reports when RDBInsight summary metadata is missing or incompatible.
