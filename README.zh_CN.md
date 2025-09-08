# RDBInsight

[![CI](https://github.com/DCjanus/rdbinsight/actions/workflows/ci.yml/badge.svg)](https://github.com/DCjanus/rdbinsight/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/DCjanus/rdbinsight/graph/badge.svg?token=KXVIGig53g)](https://codecov.io/gh/DCjanus/rdbinsight)
[![dependency status](https://deps.rs/repo/github/dcjanus/rdbinsight/status.svg)](https://deps.rs/repo/github/dcjanus/rdbinsight)
[![Lines of Code](https://tokei.rs/b1/github/DCjanus/rdbinsight?)](https://github.com/DCjanus/rdbinsight)

RDBInsight 是面向 Redis 的分析与诊断工具。它将 RDB 快照解析为便于 OLAP 分析的结构化元数据，帮助快速定位内存与性能相关的问题。

[English](README.md) | 中文

## 重要说明

本项目仍在积极开发中，版本可能不保证向后兼容，命令行参数、输出格式或接口可能发生变更。请谨慎在生产环境中使用。

## 使用示例（Usage）

完整使用手册：[docs/USAGE.zh_CN.md](docs/USAGE.zh_CN.md)

## 为什么选择 RDBInsight

对于常见的 Redis 问题（例如“大 Key”），已有许多成熟工具。但在生产环境中仍会遇到更复杂、难以用常规检查覆盖的场景，例如：

- 数据清理脚本异常中断，导致节点间出现无法解释的内存不均衡；
- 错误使用 hash tag 导致数据分布严重倾斜，监控难以及时反映；
- 业务边缘缺陷长期累积产生大量无用数据。

这类问题通常需要运维编写一次性扫描脚本，耗时且难以复用。RDBInsight 通过离线解析 RDB 并将关键元数据加载到 OLAP 系统，使分析过程可复用、可审计。

## 设计理念：从“定位问题”到“赋能分析”

RDBInsight 更关注提供通用的诊断能力而非固定的检查项。通过提取轻量级键元数据并加载到 ClickHouse，用户可以用标准 SQL 从任意维度进行分析，以实现：

- 生成按前缀聚合的内存火焰图；
- 结合多维信息验证复杂假设；
- 将一次性排查沉淀为可复用的分析流程。

## 主要特性

- **增量解析器**：在接近恒定内存开销下解析 RDB，能处理包含大型键值的数据集；
- **灵活的 SQL 分析**：将结构化元数据写入 ClickHouse 后，可用 SQL 做任意即席查询；
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

1. 离线解析 RDB 并写入 ClickHouse 或其他 OLAP 存储；
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

- [ ] 使用 testcontainers 构建 Redis Cluster 集成测试，验证集群场景下的数据解析准确性；
- [ ] 使用 testcontainers 创建 Codis 集群测试环境（基于 `pikadb/codis:v3.5.5` 镜像），确保 Codis 兼容性；
- [ ] 使用 DuckDB 实现当 Parquet 元数据缺失或版本不兼容时的降级报告生成逻辑（作为后备方案）。
- [ ] 将现有的集成测试迁移到 `rdbinsight` crate 的模块中，以便大多数类型/函数可以从 `pub` 改为 `pub(crate)`，更容易发现 dead code。
