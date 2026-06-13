# RDBInsight

[![CI](https://github.com/DCjanus/rdbinsight/actions/workflows/ci.yml/badge.svg)](https://github.com/DCjanus/rdbinsight/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/DCjanus/rdbinsight/graph/badge.svg?token=KXVIGig53g)](https://codecov.io/gh/DCjanus/rdbinsight)
[![dependency status](https://deps.rs/repo/github/dcjanus/rdbinsight/status.svg)](https://deps.rs/repo/github/dcjanus/rdbinsight)
[![Docker Image Version](https://ghcr-badge.egpl.dev/dcjanus/rdbinsight/latest_tag?color=%2344cc11&ignore=latest&label=docker+image&trim=)](https://github.com/DCjanus/rdbinsight/pkgs/container/rdbinsight)

RDBInsight 是面向 Redis RDB 的分析工具。它可以从在线 Redis、Redis Cluster、Codis 或本地 RDB 文件读取快照数据，并将紧凑的 key 元数据写入 ClickHouse 或 Parquet，用于可重复的内存与数据分布诊断。

[English](README.md) | 中文

## 重要说明

本项目仍在积极开发中，版本可能不保证向后兼容，命令行参数、输出格式或接口可能发生变更。请谨慎在生产环境中使用。

## 使用示例（Usage）

完整使用手册：[docs/USAGE.zh_CN.md](docs/USAGE.zh_CN.md)

官方 Docker 镜像发布在 [ghcr.io](https://github.com/DCjanus/rdbinsight/pkgs/container/rdbinsight)：

```bash
docker run --rm ghcr.io/dcjanus/rdbinsight:v0.2.0 --help
```

示例：将本地 RDB 文件解析为 Parquet：

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

本仓库目前暂不提供宿主机预编译二进制。需要在不使用 Docker 的环境运行时，请自行从源码构建。

## 为什么选择 RDBInsight

对于常见的 Redis 问题（例如“大 Key”），已有许多成熟工具。但在生产环境中仍会遇到更复杂、难以用常规检查覆盖的场景，例如：

- 数据清理脚本异常中断，导致节点间出现无法解释的内存不均衡；
- 错误使用 hash tag 导致数据分布严重倾斜，监控难以及时反映；
- 业务边缘缺陷长期累积产生大量无用数据。

这类问题通常需要运维编写一次性扫描脚本，耗时、影响线上实例且难以复用。RDBInsight 通过解析 RDB 并将关键元数据加载到分析存储，使排查过程可复用、可审计。

## 设计理念：从“定位问题”到“赋能分析”

RDBInsight 更关注提供通用的诊断能力而非固定的检查项。通过将 RDB 数据解析成稳定的分析 schema，用户可以反复查询同一份数据，而不必重复扫描 Redis：

- 将元数据加载到 ClickHouse 进行即席 SQL 分析；
- 将元数据保存为 Parquet，便于离线处理与报告生成；
- 生成包含前缀火焰图的自包含 HTML 报告；
- 将一次性排查沉淀为可重复的分析流程。

## 主要特性

- **增量解析器**：在接近恒定内存开销下解析 RDB，能处理包含大型键值的数据集；
- **多种数据来源**：支持从单机 Redis、Redis Cluster、Codis 与本地 RDB 文件读取数据；
- **现代 Redis 兼容性**：支持 Redis 8.6 stream 与 Redis 8.8 array/stream 等较新的 RDB 编码；
- **灵活输出**：可将结构化元数据写入 ClickHouse 或 Parquet；
- **自包含 HTML 报告**：生成单文件 HTML 报表，离线查看集群信息和分析结果，并包含前缀火焰图（[示例报表](https://dcjanus.github.io/rdbinsight/)）。

## 数据模型

我们仅提取用于分析的关键元数据，以降低存储与查询成本：

- `cluster`: 来源集群名
- `batch`: 导入批次时间戳（纳秒精度）
- `instance`: 实例（IP:PORT）
- `db`: 所属数据库
- `key`: 键名
- `type`: 数据类型
- `member_count`: 集合类元素数量
- `rdb_size`: 在 RDB 中占用的字节数
- `encoding`: 编码方式
- `expire_at`: 过期时间
- `idle_seconds`: LRU 空闲时间（秒）
- `freq`: LFU 访问频率
- `codis_slot`: Codis Slot ID（0-1023，仅用于 Codis）
- `redis_slot`: Redis Cluster Slot ID（0-16383，仅用于 Redis Cluster）

## 实例：对 1 TB 集群进行前缀校验

假设某生产集群总内存约 **1 TB**，业务方提供 5 个“白名单”前缀，期望确认：

1. 是否仅包含这 5 个前缀；若存在其它前缀，需要列出示例 Key；
2. 统计每个合法前缀的 Key 数量，以评估清理收益或容量规划。

传统做法需要在每个实例上运行 `SCAN`，耗时且难以复用。使用 RDBInsight 可按如下步骤完成：

1. 解析 RDB 数据并将元数据写入 ClickHouse；
2. 使用标准 SQL 完成诊断：

```sql
-- 白名单，可写成临时表或使用数组常量
WITH ['bizA:', 'bizB:', 'bizC:', 'bizD:', 'bizE:'] AS whitelist

-- 1. 找出不在白名单中的前缀
SELECT
    substring(key, 1, position(key, ':')) AS prefix,
    key
FROM redis_records_view
WHERE prefix NOT IN whitelist
LIMIT 100;

-- 2. 统计每个合法前缀的 Key 数量
WITH
    -- 正则仅保留白名单前缀（示例共 5 个）
    '^(bizA:|bizB:|bizC:|bizD:|bizE:)' AS re
SELECT
    extract(key, re) AS prefix,  -- 提取前缀
    COUNT()          AS key_cnt  -- 统计数量
FROM redis_records_view
WHERE cluster = 'your_cluster'  -- 可替换为具体过滤条件
  AND batch = parseDateTime64BestEffort('2024-01-01T00:00:00Z', 9, 'UTC') -- 指定批次
  AND extract(key, re) != ''
GROUP BY prefix
ORDER BY key_cnt DESC;
```

该流程无需在线连接 Redis，后续只需调整 SQL 即可复用。

## TODO

- [ ] 补充 Redis Cluster source 端到端集成测试，覆盖 `dump from-cluster` 的分片发现、副本选择与记录输出。
- [ ] 补充真实 Codis Dashboard 与 Redis backend 的 Codis 集群集成测试，验证 Dashboard 发现流程与 Codis slot 元数据。
- [ ] 为 Parquet 报告实现 DuckDB 降级路径，用于处理 RDBInsight 汇总元数据缺失或不兼容的 Parquet 文件。
- [ ] 将集成测试迁移到 `rdbinsight` crate 内部，使内部模块尽可能收敛为 `pub(crate)`。
