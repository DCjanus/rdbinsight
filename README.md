# RDBInsight

RDBInsight 是一款 Redis 深度诊断工具，它通过将 RDB 快照转换为适合 OLAP 分析的结构化数据，帮助您排查复杂的内存和性能问题。

## 为什么需要 RDBInsight？

对于 Redis 的一些常见问题，如大 Key，市面上已有不少成熟的工具。然而，在真实的生产环境中，我们经常会遇到更棘手的"疑难杂症"：

- 某个数据清理脚本异常中断，导致节点间出现难以解释的内存倾斜。
- 由于 hash tag 使用不当，数据分布严重不均，但从监控上无法一目了然。
- 业务逻辑的微小缺陷，日积月累导致了大量难以归类的无用数据。

排查这些非标问题，往往需要运维人员编写一次性的脚本去扫描和分析，这不仅是大量的重复劳动，而且效率低下，缺乏系统性的方法。

## 核心理念：从"找问题"到"给能力"

RDBInsight 的核心理念不是预设要寻找的问题，而是赋予用户自由探索和诊断的能力。

它通过离线解析 RDB 文件，提取出轻量级的键元数据，并将其加载到 ClickHouse 中。一旦数据被结构化，您就可以利用 SQL 的全部威力，进行任意维度的、灵活的、深度的即席查询，从而能够：

- 生成内存火焰图，直观地发现任何粒度的键前缀的内存分布。
- 关联分析不同维度的数据，以验证您对复杂问题的任何猜想。
- 将重复的排查工作，沉淀为标准的、可复用的分析流程。

## 核心亮点

- **流式解析架构**：轻松应对 GB 级大 Key，内存占用几乎恒定，不会因数据规模而崩溃
- **灵活的 SQL 分析**：将 RDB 数据结构化后加载到 ClickHouse，支持任意维度的即席查询
- **自包含 HTML 报告**：生成单文件、自包含的 HTML，包含直观的前缀火焰图，可离线查看集群完整信息和分析结果（[查看示例报表](https://dcjanus.github.io/rdbinsight/)）

## 数据模型

我们只提取核心元数据，以实现高效的分析和存储：

- `cluster`: 来源集群名
- `batch`: 导入批次时间戳（纳秒精度）
- `instance`: 实例名(IP:PORT)
- `db`: 所属 DB
- `key`: 键名
- `rdb_size`: RDB 中的占用字节
- `type`: 数据类型
- `encoding`: 编码方式
- `expire_at`: 过期时间
- `member_count`: 集合类元素数量
- `idle_seconds`: LRU 空闲时间（秒）
- `freq`: LFU 访问频率

## 实际案例：1 TB 集群的前缀白名单校验

假设某生产集群的 Redis 总内存已达到 **1 TB**，业务方提供了 5 个前缀作为"白名单"。他们希望快速确认：

1. **是否有且只有这 5 个前缀**。若存在其它前缀，需要列出具体的 Key 以便后续排查。
2. **统计每个合法前缀的 Key 数量**，用于评估预估清理收益或容量规划。

传统做法往往要编写脚本，对集群所有实例执行 `SCAN`，不仅耗时且难以复用。  
借助 **RDBInsight**，只需：

1. 离线解析 RDB 并写入 ClickHouse 或其他 OLAP 数据库。
2. 运行几条标准 SQL 即可完成诊断：

```sql
-- 白名单，直接写成临时表或使用数组常量
WITH ['bizA:', 'bizB:', 'bizC:', 'bizD:', 'bizE:'] AS whitelist

-- 1. 找出不在白名单中的前缀
SELECT
    substring(key, 1, position(key, ':'))         AS prefix,
    key
FROM redis_records_view
WHERE prefix NOT IN whitelist
LIMIT 100;

-- 2. 统计每个合法前缀的 Key 数量
WITH
    -- 正则仅保留白名单前缀（示例共 5 个）
    '^(bizA:|bizB:|bizC:|bizD:|bizE:)' AS re

SELECT
    extract(key, re)         AS prefix,   -- 提取前缀
    COUNT()                  AS key_cnt   -- 统计数量
FROM redis_records_view
WHERE cluster = 'your_cluster'        -- 可按需替换过滤条件
  AND batch = parseDateTime64BestEffort('2024-01-01T00:00:00Z', 9, 'UTC')  -- 指定批次
  AND extract(key, re) != ''               -- 仅保留匹配到前缀的行
GROUP BY prefix
ORDER BY key_cnt DESC;
```

整个过程不依赖 Redis 在线命令，后续类似需求只需改改 SQL，**零开发成本**。

## TODO

- 集成测试将使用 testcontainers 验证 Redis cluster 场景
- 集成测试将使用 testcontainers 创建 Codis 集群，使用 `pikadb/codis:v3.5.5` 镜像
- 将 RDB 解析和 ClickHouse 写入拆分为两个独立协程，通过有界 Channel 连接以提升吞吐
