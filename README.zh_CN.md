# RDBInsight

[![codecov](https://codecov.io/gh/DCjanus/rdbinsight/graph/badge.svg?token=KXVIGig53g)](https://codecov.io/gh/DCjanus/rdbinsight)
[![dependency status](https://deps.rs/repo/github/dcjanus/rdbinsight/status.svg)](https://deps.rs/repo/github/dcjanus/rdbinsight)
[![CI](https://github.com/DCjanus/rdbinsight/actions/workflows/ci.yml/badge.svg)](https://github.com/DCjanus/rdbinsight/actions/workflows/ci.yml)

RDBInsight 是一款 Redis 分析与诊断工具，通过将 RDB 快照转换为便于 OLAP 处理的结构化数据，帮助定位内存及性能相关问题。

[English](README.md) | 中文

## 重要说明

本项目处于积极开发阶段，版本之间不保证向下兼容，接口、命令行参数及输出格式可能在未通知的情况下发生变化。请勿在生产环境使用本软件。

## 使用示例（Usage）

最小示例：

```bash
rdbinsight dump from-cluster \
  --nodes 127.0.0.1:7000,127.0.0.1:7001 \
  --cluster your_cluster \
  into-clickhouse \
  --url http://username:password@127.0.0.1:8124/rdbinsight \
  --auto-create-tables
```

查看更详细的用法，请执行 `rdbinsight --help` 或 `rdbinsight <子命令> --help`。

## 为何使用 RDBInsight？

对于常见的 Redis 问题（如大 Key），已有不少成熟工具可供选择。然而在生产环境中，仍会出现更为复杂且难以归类的场景，例如：

- 某数据清理脚本异常中断，导致节点间出现难以解释的内存倾斜
- 由于 hash tag 使用不当，数据分布严重不均，但监控侧难以及时反映
- 业务逻辑的边缘缺陷在长期运行后累积出大量无用数据

处理此类问题通常需要运维人员编写一次性脚本进行扫描与分析，过程重复且效率有限，也缺乏可复用的方法论。

## 设计理念：从“定位问题”到“提供能力”

RDBInsight 的目标是提供灵活的诊断能力，而非限定固定的检查项。

通过离线解析 RDB 文件、提取轻量级键元数据并加载至 ClickHouse，用户可以使用标准 SQL 从任意维度对数据进行查询与分析，以便：

- 生成内存火焰图，按不同前缀查看内存分布
- 结合多维信息验证对复杂问题的假设
- 将一次性的排查步骤沉淀为可复用的分析流程

## 特性

- **增量解析器**：在接近恒定的内存开销下解析 RDB，可处理包含大型键值的数据集
- **灵活的 SQL 分析**：结构化数据加载到 ClickHouse 后，可用 SQL 进行即席查询
- **自包含 HTML 报告**：生成单文件 HTML，离线即可查看集群信息和分析结果，并附带前缀火焰图（[示例报表](https://dcjanus.github.io/rdbinsight/)）

## 数据模型

我们仅提取关键元数据，以实现高效分析与存储：

- `cluster`: 来源集群名
- `batch`: 导入批次时间戳（纳秒精度）
- `instance`: 实例名(IP:PORT)
- `db`: 所属 DB
- `key`: 键名
- `type`: 数据类型
- `member_count`: 集合类元素数量
- `rdb_size`: RDB 中的占用字节
- `encoding`: 编码方式
- `expire_at`: 过期时间
- `idle_seconds`: LRU 空闲时间（秒）
- `freq`: LFU 访问频率
- `codis_slot`: Codis Slot ID (0-1023)，仅用于 Codis 集群
- `redis_slot`: Redis Cluster Slot ID (0-16383)，仅用于 Redis 集群

## 实际案例：1 TB 集群的前缀校验

假设某生产集群的 Redis 总内存已达到 **1 TB**，业务方提供了 5 个前缀作为“白名单”，希望确认：

1. 是否仅包含这 5 个前缀；若存在其它前缀，需要列出具体 Key 以便排查
2. 统计每个合法前缀的 Key 数量，用于评估清理收益或容量规划

传统方案通常需要编写脚本，在所有实例上执行 `SCAN`，过程耗时且难以复用。使用 RDBInsight，可按以下步骤完成：

1. 离线解析 RDB 并写入 ClickHouse 或其他 OLAP 数据库。
2. 通过标准 SQL 完成诊断：

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
WHERE cluster = 'your_cluster'  -- 可替换过滤条件
  AND batch = parseDateTime64BestEffort('2024-01-01T00:00:00Z', 9, 'UTC') -- 指定批次
  AND extract(key, re) != ''
GROUP BY prefix
ORDER BY key_cnt DESC;
```

整个过程不依赖 Redis 在线命令，后续类似需求仅需调整 SQL。

## 开发计划

### 测试完善

- [ ] 使用 testcontainers 构建 Redis Cluster 集成测试，验证集群场景下的数据解析准确性
- [ ] 使用 testcontainers 创建 Codis 集群测试环境（基于 `pikadb/codis:v3.5.5` 镜像），确保 Codis 兼容性

### 性能优化

- [ ] 将 RDB 解析和 ClickHouse 写入拆分为独立协程，通过有界 Channel 连接，提升整体吞吐量
- [ ] 评估 mimalloc 内存分配器替代默认分配器的性能收益，优化大文件解析场景
