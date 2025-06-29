# 设计方案：ClickHouse 数据输出

## 1. 背景与思路

本方案旨在为 `rdbinsight` 工具增加一个 ClickHouse 数据输出端。核心目标是将从 RDB 文件中解析出的 `Record` 数据流，高效且可靠地写入 ClickHouse 数据库，并确保数据快照的查询原子性。

为解决数据导入过程中可能中断导致的数据不一致问题，我们采用"**数据与元数据分离**"的架构：

1.  **底层数据表 (`redis_records_raw`)**：用于存储所有批次导入的原始 Redis 记录。
2.  **完成标记表 (`import_batches_completed`)**：当一个批次的数据完全写入底层表后，在此表中插入一条完成标记。
3.  **查询视图 (`redis_records_view`)**：最终用户通过此视图查询数据。该视图只暴露那些已在标记表中存在的、完整的批次。

这种设计通过将数据写入和"发布"解耦，保证了用户查询的原子性和一致性。

## 2. 关键设计

### 2.1 ClickHouse Schema 定义

`rdbinsight` **不负责**自动在 ClickHouse 中创建数据库对象。作为替代，程序将提供一个专门的命令行参数（例如 `--print-clickhouse-schema`），用于输出推荐的、可直接执行的 DDL（Data Definition Language）建表语句。数据库管理员（DBA）或用户可以自行执行这些语句来准备目标环境。

以下是推荐的 Schema 定义：

#### a. 底层数据表 (`redis_records_raw`)

此表是所有 Redis 记录的中央存储库。

```sql
-- redis_records_raw: 存储从RDB解析出的原始Redis键值记录
CREATE TABLE IF NOT EXISTS redis_records_raw
(
    `cluster`      String COMMENT 'Redis 集群的逻辑名称',
    `batch`        DateTime64(3, 'UTC') COMMENT '数据导入批次的唯一时间戳标识',
    `instance`     String COMMENT 'Redis 实例的物理地址，如 IP:Port',
    `db`           UInt64 COMMENT 'Redis 数据库编号 (e.g., 0-15)',
    `key`          String COMMENT 'Redis 键名',
    `type`         LowCardinality(String) COMMENT 'Redis 数据结构类型 (e.g., string, hash, list)',
    `member_count` UInt64 COMMENT '数据结构中的元素数量',
    `rdb_size`     UInt64 COMMENT '该记录在RDB文件中占用的字节数',
    `encoding`     LowCardinality(String) COMMENT 'RDB内部编码类型',
    `expire_at`    Nullable(DateTime64(3, 'UTC')) COMMENT '键的绝对过期时间戳 (UTC)',
    `idle_seconds` Nullable(UInt64) COMMENT '键的LRU空闲时间（秒）',
    `freq`         Nullable(UInt8) COMMENT '键的LFU访问频率'
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(batch)
ORDER BY (cluster, batch, instance, db, key)
TTL batch + INTERVAL 3 DAY
COMMENT '存储所有从RDB解析的Redis记录的原始数据表';
```

- **`ReplacingMergeTree`**: 用于处理可能重复的数据写入。
- **`ORDER BY`**: `(cluster, batch, instance, db, key)` 是主键，用于唯一标识一条记录，是 `ReplacingMergeTree` 合并去重的依据。
- **`PARTITION BY`**: 按天分区，便于管理和删除过期数据。
- **`TTL`**: 数据保留3天后自动删除。

#### b. 导入完成标记表 (`import_batches_completed`)

此表记录了哪些批次的数据已经完整地写入 `redis_records_raw`。

```sql
-- import_batches_completed: 记录已成功完成导入的批次信息，用于实现快照的原子性读
CREATE TABLE IF NOT EXISTS import_batches_completed
(
    `cluster` String COMMENT 'Redis 集群的逻辑名称',
    `batch`   DateTime64(3, 'UTC') COMMENT '数据导入批次的唯一时间戳标识'
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(batch)
ORDER BY (cluster, batch)
TTL batch + INTERVAL 3 DAY
COMMENT '已完成数据导入的批次标记表';
```

#### c. 查询视图 (`redis_records_view`)

这是提供给最终用户的主要查询入口，它确保了查询结果的原子性。

```sql
-- redis_records_view: 安全的查询视图，仅暴露已确认完成导入的数据
CREATE OR REPLACE VIEW redis_records_view AS
SELECT
    *
FROM
    redis_records_raw
WHERE
    (cluster, batch) IN (
        SELECT
            cluster,
            batch
        FROM
            import_batches_completed
    );
```

### 2.2 核心组件与流程

#### a. `ClickHouseOutput` 模块

我们将创建一个新的 Rust 模块 `src/output/clickhouse.rs`。

- **`ClickHouseOutputConfig`**: 结构体，承载从 TOML 文件解析的配置信息。
- **`ClickHouseOutput`**: 核心结构体，负责：
  - 维护一个 ClickHouse 客户端连接池。
  - **在初始化时检查所需的表/视图是否存在**，如果不存在则返回错误，提示用户使用 `misc print-clickhouse-schema` 子命令进行创建。
  - 提供 `write(&[Record])` 方法，用于批量写入 `redis_records_raw`。
  - 提供 `commit_batch(batch, cluster)` 方法，用于在 `import_batches_completed` 中写入完成标记。

#### b. 数据写入流程

`rdbinsight` 命令的执行逻辑将由子命令驱动：

1.  **`misc print-clickhouse-schema` 子命令**:

    - 当用户执行此命令时，程序将打印出 2.1 节中定义的三个 DDL 语句，然后正常退出。

2.  **`dump` 子命令**:
    - **配置加载**: 根据 `dump` 命令提供的配置文件路径，加载源（Redis）和目标（ClickHouse）的配置。
    - **初始化**: 程序启动时，生成一个全局唯一的 `batch` 时间戳。
    - **连接与验证**: 初始化 `ClickHouseOutput` 实例，它会连接到 ClickHouse 并验证所需表/视图均已存在。
    - **数据处理与写入**: 创建 `RecordStream` 并持续产出 `Record` 对象，收集到缓冲区中，达到 `1,000,000` 条记录后批量写入 `redis_records_raw`。此过程包含带线性退避的重试逻辑。
    - **提交快照**: 所有数据成功写入后，调用 `ClickHouseOutput::commit_batch()` 向 `import_batches_completed` 表插入一条完成记录。

## 3. 实施建议

1.  **添加依赖**: 在 `Cargo.toml` 中添加 ClickHouse 客户端库（如 `clickhouse-rs`）。
2.  **配置模块**: 扩展 `src/config.rs`，添加 `ClickHouseConfig`。
3.  **命令行结构**: 使用 `clap` 的 `subcommand` 功能重构 `src/bin/rdbinsight.rs` 中的 `Cli` 结构体，以支持 `dump` 和 `misc print-clickhouse-schema` 命令。
4.  **实现 `ClickHouseOutput` 模块**: 在 `src/output/clickhouse.rs` 中实现。
    - 重点在于连接、**表结构验证**、数据转换和批量写入逻辑。
5.  **集成到 `main`**: 修改 `src/bin/rdbinsight.rs` 的 `main` 函数，根据不同的子命令（`dump` 或 `misc`）执行相应的工作流。

## 4. 建议标题

```
feat(output): implement ClickHouse as a data destination
```

```

```
