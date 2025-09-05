# Changelog

## [Unreleased]

### Features

- Report: add periodic progress logs when scanning Parquet files' keys to avoid long silence during report generation.
- Report: log newly discovered significant prefixes during top-prefix discovery to surface important findings early.

### Performance

- Report (Parquet): switch top-prefix discovery to a single-pass LCP stack algorithm, significantly reducing CPU and speeding up report generation.

## [0.1.0-rc.5]

### Fixed

- Parser: support LZF-encoded RDB strings and fix incorrect handling of LZF-compressed IntSet strings in RDB files (now correctly unboxes compressed strings and reads intset header).

## [0.1.0-rc.4]

### Breaking

- Parquet adopts Hadoop-style layout with batch-level atomic finalize: `_tmp_batch=<batch>` → `batch=<batch>`.

### Features

- Parquet: Per-instance external merge sort; one final file per instance sorted by `(db, key)`.
- Parquet: Configurable compression for runs (LZ4) and final files (ZSTD).
- Use `mimalloc` as the global allocator to improve memory management and performance
- Report: Support generating reports from Parquet files via `report from-parquet`.

## [0.1.0-rc.3]

### Breaking

- Require `http[s]://host:port?database=<db>` for ClickHouse to align with the [official HTTP interface](https://clickhouse.com/docs/interfaces/http) and avoid issues in complex deployments (e.g., multi-layer reverse proxies); path-based database selection is removed

### Features

- Add sanitized info logs for ClickHouse/Proxy URLs

## [0.1.0-rc.2]

### Changed

- CLI: Unified argument style across subcommands. `report` now uses `from-clickhouse` instead of direct ClickHouse flags. Example: `rdbinsight report from-clickhouse --url <URL> [--proxy-url <PROXY>]`.

## [0.1.0-rc.1]

### Features

- **Multiple Data Sources Support**: Support for dumping data from Redis standalone instances, Redis clusters, Codis clusters, and local RDB files
- **Multiple Output Targets**: Support for outputting parsed data to ClickHouse database and local Parquet files
- **HTML Report Generation**: Capability to generate self-contained HTML reports from ClickHouse tables

---

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
