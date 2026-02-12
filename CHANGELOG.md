# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Dependencies: migrate run-file serialization from `bincode` to `wincode` because `bincode` is no longer maintained.
- Dependencies: upgrade Arrow/Parquet to 57.0.0 and update Parquet sorting metadata construction to use `parquet::file::metadata::SortingColumn`.
- Dependencies: bump `lz4_flex` to 0.12.0.
- Dependencies: bump `bytes` from 1.10.1 to 1.11.1.
- Dependencies: upgrade `rand` from 0.9.2 to 0.10.0.

## [0.1.0]

### Added

- Report (HTML): flame graph card now offers a CSV export of top prefixes with decoded and base64 columns.

### Changed

- Report (HTML): top key/top prefix CSV exports use snake_case headers, include both human-readable and base64 values, and share the same count formatting helper.
- Report (Parquet): progress logs now include average rows-per-second throughput when scanning top prefixes.
- Dependencies: bump `typed-builder` to 0.22.0.
- Dependencies: migrate retry logic from unmaintained `backoff` crate to `backon`.
- Dependencies: use crates.io `clickhouse` 0.14.0 release and adopt its async insert API.

## [0.1.0-rc.8]

### Changed

- Display-only: human-friendly byte-size formatting for reports and progress logs.
- ClickHouse Output: saturate overflow `expire_at_ms` to a maximum datetime instead of panicking during conversion.

## [0.1.0-rc.7]

### Changed

- Enable `time` crate `large-dates` feature to support very large TTL values.

## [0.1.0-rc.6]

### Added

- Report: add periodic progress logs when scanning Parquet files' keys to avoid long silence during report generation.
- Report: log newly discovered significant prefixes during top-prefix discovery to surface important findings early.

### Changed

- Report (Parquet): switch top-prefix discovery to a single-pass LCP stack algorithm, significantly reducing CPU and speeding up report generation.

## [0.1.0-rc.5]

### Fixed

- Parser: support LZF-encoded RDB strings and fix incorrect handling of LZF-compressed IntSet strings in RDB files (now correctly unboxes compressed strings and reads intset header).

## [0.1.0-rc.4]

### Changed

- BREAKING: Parquet adopts Hadoop-style layout with batch-level atomic finalize: `_tmp_batch=<batch>` → `batch=<batch>`.

### Added

- Parquet: Per-instance external merge sort; one final file per instance sorted by `(db, key)`.
- Parquet: Configurable compression for runs (LZ4) and final files (ZSTD).
- Use `mimalloc` as the global allocator to improve memory management and performance
- Report: Support generating reports from Parquet files via `report from-parquet`.

## [0.1.0-rc.3]

### Changed

- BREAKING: Require `http[s]://host:port?database=<db>` for ClickHouse to align with the [official HTTP interface](https://clickhouse.com/docs/interfaces/http) and avoid issues in complex deployments (e.g., multi-layer reverse proxies); path-based database selection is removed

### Added

- Add sanitized info logs for ClickHouse/Proxy URLs

## [0.1.0-rc.2]

### Changed

- CLI: Unified argument style across subcommands. `report` now uses `from-clickhouse` instead of direct ClickHouse flags. Example: `rdbinsight report from-clickhouse --url <URL> [--proxy-url <PROXY>]`.

## [0.1.0-rc.1]

### Added

- Multiple Data Sources Support: Support for dumping data from Redis standalone instances, Redis clusters, Codis clusters, and local RDB files
- Multiple Output Targets: Support for outputting parsed data to ClickHouse database and local Parquet files
- HTML Report Generation: Capability to generate self-contained HTML reports from ClickHouse tables
