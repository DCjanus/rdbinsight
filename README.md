# RDBInsight

[![codecov](https://codecov.io/gh/DCjanus/rdbinsight/graph/badge.svg?token=KXVIGig53g)](https://codecov.io/gh/DCjanus/rdbinsight)
[![dependency status](https://deps.rs/repo/github/dcjanus/rdbinsight/status.svg)](https://deps.rs/repo/github/dcjanus/rdbinsight)

[![CI](https://github.com/DCjanus/rdbinsight/actions/workflows/ci.yml/badge.svg)](https://github.com/DCjanus/rdbinsight/actions/workflows/ci.yml)

RDBInsight is a Redis analysis and diagnostic tool that converts RDB snapshots into structured data suitable for OLAP workloads, helping engineers investigate memory and performance issues.

English | [中文](README.zh_CN.md)

## Why RDBInsight?

For common Redis issues such as large keys, mature tools already exist. In production environments, however, more intricate and less clearly defined situations still arise, for example:

- A data-cleanup script terminates unexpectedly, resulting in uneven memory usage across nodes.
- Improper hash-tag usage leads to imbalanced data distribution that regular monitoring does not immediately reveal.
- Minor defects in business logic accumulate over time, producing large volumes of obsolete data.

Addressing these atypical problems often involves writing one-off scripts for scanning and analysis—a repetitive, time-consuming process that is hard to reuse.

## Design Philosophy: Enabling, Not Prescribing

RDBInsight focuses on empowering users with flexible diagnostic capabilities rather than enforcing a fixed checklist.

By parsing RDB files offline, extracting lightweight key metadata, and loading the results into ClickHouse, users can rely on standard SQL to investigate the data from any dimension:

- Generate memory flame graphs to visualise memory distribution by key prefix.
- Combine multiple dimensions of information to validate hypotheses about complex issues.
- Turn ad-hoc troubleshooting steps into reusable analysis procedures.

## Features

- **Incremental parser** – Parses RDB files with a near-constant memory footprint and can handle datasets containing very large keys.
- **Flexible SQL analysis** – Structured data is loaded into ClickHouse, enabling ad-hoc queries across arbitrary dimensions.
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

1. Parse the RDB file offline and load the data into ClickHouse (or another OLAP database).
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

## Development Plan

### Testing

- [ ] Use testcontainers to build Redis Cluster integration tests and verify parsing accuracy in cluster scenarios.
- [ ] Use testcontainers to create a Codis cluster test environment (based on the `pikadb/codis:v3.5.5` image) to ensure compatibility.
- [ ] Optimize codecov test coverage thresholds by implementing reasonable module-based coverage targets for different components (parser, output, source, etc.).

### Performance

- [ ] Separate RDB parsing and ClickHouse insertion into independent tasks connected by bounded channels to improve throughput.
- [ ] Evaluate the performance gains of replacing the default allocator with mimalloc, especially for large files.
