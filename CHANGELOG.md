# Changelog

## [Unreleased]

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
