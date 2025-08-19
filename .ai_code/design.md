# 背景与思路

- 现状：当前数据通路是“多解析协程 → 单输出消费者”的生产者-消费者模型。各解析协程将 `Chunk` 通过 channel 发送到单一消费者，消费者串行执行 `Output::write_chunk`。在 ClickHouse 场景下，串行写入成为吞吐瓶颈。
- 目标：改为“每个协程内既有源端也有目标端”的并行架构。每个解析协程在本协程内完成解析与聚合成 `Chunk` 后，直接调用自身持有的输出端执行写入。通过并行化输出，消除单消费者串行瓶颈。
- 抽象升级：删除现有 `Output` 枚举，改为 trait object 抽象，两层接口：
  - 批次级：`Output` 负责批次准备（`prepare_batch`）、按实例派生 `ChunkWriter`、批次收尾（`finalize_batch`，如 ClickHouse 批次完成标记、Parquet 目录重命名）。
  - 协程/实例级：`ChunkWriter` 负责实际写入与实例级收尾（如 Parquet 文件关闭）。
- 进度统计：使用共享互斥锁保护的计数器聚合各协程成功提交后的累计记录数与完成实例数；每次成功提交 `Chunk` 或实例完成后输出累计进度日志（含 RPS），避免集中式瓶颈的同时保留全局可观测性。


# 关键设计

## 1. 新输出抽象（Trait Object）

```rust
use async_trait::async_trait;
use crate::helper::AnyResult;
use crate::output::types::Chunk;

#[async_trait]
pub trait Output: Send + Sync {
    /// 批次级准备（如 ClickHouse 校验/建表、Parquet 批次目录创建），要求幂等。
    async fn prepare_batch(&self) -> AnyResult<()>;

    /// 创建一个与给定 `instance` 绑定的写入器。
    async fn create_writer(&self, instance: &str) -> AnyResult<Box<dyn ChunkWriter + Send>>;

    /// 批次级收尾（如 ClickHouse 批次完成行写入、Parquet 批次目录重命名）。
    /// 需保证仅调用一次；建议在所有解析任务完成后再调用。
    async fn finalize_batch(self: Box<Self>) -> AnyResult<()>;
}

#[async_trait]
pub trait ChunkWriter: Send {
    /// 写入一个数据块。要求幂等或由调用方包裹重试策略。
    async fn write_chunk(&mut self, chunk: Chunk) -> AnyResult<()>;

    /// 实例级收尾（ClickHouse 可为 no-op；Parquet 需关闭文件并固化）。
    async fn finalize_instance(&mut self) -> AnyResult<()>;
}
```

- 说明：
  - 使用 trait object（`Box<dyn ...>`）替代旧 `Output` 枚举，便于扩展、解耦与并行。
  - `Output` 要求 `Send + Sync`，以便在多协程中安全共享创建写入器的能力；`ChunkWriter` 要求 `Send`，用于在单协程内执行 I/O。
  - `prepare_batch()` 必须具备幂等性，以便在出现重试或并发调用时不会造成副作用；建议内部实现包含必要的存在性检查或原子操作。

## 2. ClickHouse 实现

- `ClickHouseOutput`：
  - 责任：
    - 在 `prepare_batch()` 执行一次性初始化（表存在性校验/必要时建表），确保多 `ChunkWriter` 可安全复用同一批次上下文（`cluster`、`batch_ts`）。
  - 接口实现：
    - `prepare_batch()`：校验/建表，并记录批次上下文。
    - `create_writer(instance)`：返回一个 `ClickHouseChunkWriter`，内部持有独立的 `clickhouse::Client`。
    - `finalize_batch()`：向 `import_batches_completed` 插入一行，表示批次完成。
- `ClickHouseChunkWriter`：
  - 责任：
    - 基于自身持有的 Client，将 `Chunk` 逐条写入 `redis_records_raw`（沿用当前行构造逻辑）；`finalize_instance()` 为 no-op。
  - 并行性：
    - 每个协程一个 writer，天然并行；ClickHouse 服务端负责背压与吞吐调度。

## 3. Parquet 实现

- `ParquetOutput`：
  - 责任：
    - 在 `prepare_batch()` 中创建批次临时目录；记录 `final_batch_dir`。
    - `create_writer(instance)`：根据实例生成单独的 parquet 临时文件（`<instance>.parquet.tmp`），并返回 `ParquetChunkWriter`；确保同一实例仅被一个解析协程使用（与当前“每个 stream 对应一个实例”的并发模型一致）。
    - `finalize_batch()`：在确保所有实例 writer 均关闭后，将批次目录从临时名原子重命名为最终名。
- `ParquetChunkWriter`：
  - 责任：
    - 持有该实例对应的 `AsyncArrowWriter<File>`；`write_chunk()` 将 `Chunk` 转为 `RecordBatch` 并持续写入；`finalize_instance()` 关闭 writer 并将文件从 `.tmp` 重命名为最终文件。
  - 并发性：
    - 按实例独立文件，互不竞争；批次级目录重命名在所有实例完成后一次进行。

## 4. 进度统计（共享状态）

- 目标：在“每个协程独立写入”的模型下，仍能输出全局进度。
- 设计：
  - 共享结构 `ProgressState`：
    - `processed_records: u64`
    - `completed_instances: usize`
    - `start_time: Instant`
  - 共享访问方式：`Arc<tokio::sync::Mutex<ProgressState>>`。
  - 调用点：
    - 成功 `write_chunk()` 后，进入临界区累加 `processed_records += chunk.records.len()`，并输出日志；
    - 成功 `finalize_instance()` 后，进入临界区累加 `completed_instances += 1`，并输出日志。
  - 日志格式：遵循规范，`operation` 字段置首，例如：
    - `info!(operation = "progress_update", processed_records = processed, completed_instances = completed, elapsed_seconds = elapsed, total_instances = total, rps = rps, "Progress update")`
  - 节流：可选地增加最小时间间隔（如 200ms）以避免日志过于密集；必要时在实现阶段细化。
  - 一致性与原子性（重要）：默认采用“临界区快照”策略，确保计数快照与日志输出具备原子可观察性：
    - 使用 `tokio::sync::Mutex`（或 `parking_lot::Mutex`）保护极小临界区，仅包含：更新计数、计算 `elapsed`/`rps`、输出一次日志。
    - 计数无需原子类型；所有读写均在互斥保护下完成。
    - 不在临界区执行可能阻塞的其他操作（除日志输出本身），如网络 I/O、创建 writer 等。
    - 可与“节流+幂等”配合降低日志频率（如≥200ms），在高并发下进一步减少交错。

## 5. 错误处理与重试

- 将现有的指数退避重试策略抽象为通用工具方法，供各协程在调用 `write_chunk()` 与 `finalize_instance()` 时复用：
  - `write_chunk_with_retry(writer, chunk, backoff)`
  - `finalize_instance_with_retry(writer, backoff)`
- ClickHouse：重试 `insert.end()` 失败的场景；Parquet：写盘失败时直接失败并中止该实例（避免生成不完整文件）。

## 6. 执行模型与生命周期

- 主流程：
  1. 构造 `Output`（由 `OutputConfig::create_output(...)` 返回 trait object）。
  2. 调用一次 `output.prepare_batch()` 完成批次初始化（CH 校验/建表或 Parquet 目录创建等）。
  3. 为每个 RDB 流（实例）启动一个解析协程：
     - 协程开始时调用 `output.create_writer(instance)` 获取独占的 `ChunkWriter`。
     - 解析并组装 `Chunk`（批大小保持现有默认，例如 1_000_000）。
     - 对每个 `Chunk` 调用 `write_chunk_with_retry()`，成功后更新共享进度计数并日志。
     - 完成后调用 `finalize_instance_with_retry()`，更新计数并日志。
  4. 等待所有协程完成后，调用一次 `output.finalize_batch()` 完成批次收尾。
- 兼容性：
  - 移除集中式消费者与 channel；保留现有 `RecordStream` 与批量聚合逻辑。

## 7. 配置与构造

- `OutputConfig::create_output(cluster, batch_ts)` 返回 `Box<dyn Output + Send + Sync>`：
  - ClickHouse：构造 `ClickHouseOutput`（注入 `ClickHouseConfig`、`cluster`、`batch_ts`）。
  - Parquet：构造 `ParquetOutput`（注入目标目录、压缩、`cluster`、`batch_ts`）。
- 实例 Writer 的创建由协程按需调用，避免预先为所有实例分配资源。

## 8. 并发与安全性

- `Output` 仅承担轻量的工厂与批次状态职责，需要线程安全（`Sync`）。
- `ChunkWriter` 面向单协程，不要求 `Sync`，减少锁竞争；内部状态（如 parquet writer、CH client）不需跨线程共享。
- `prepare_batch()` 需具备幂等性与并发安全，避免重复创建表或重复创建目录；必要时采用排他锁或存在性检查。
- Parquet 批次目录重命名在 `finalize_batch()` 单点执行，避免竞争。


# 实施建议

- 高层步骤（不写具体代码）：
  1. 新增 `Output` 与 `ChunkWriter` trait，定义在 `output/sink.rs` 或新模块 `output/abstractions.rs`。
  2. 将 `OutputConfig::create_output(...)` 改为返回 trait object；删除旧 `Output` 枚举及其方法。
  3. 实现 ClickHouse 适配：
     - 将表校验/建表与批次上下文初始化迁移至 `ClickHouseOutput::prepare_batch()`；
     - 将分块写入逻辑迁移至 `ClickHouseChunkWriter`；
     - 在 `finalize_batch()` 中插入批次完成记录。
  4. 实现 Parquet 适配：
     - 将批次临时目录创建迁移至 `ParquetOutput::prepare_batch()`；
     - 将单文件 writer 逻辑迁移至 `ParquetChunkWriter`；
     - 在 `finalize_batch()` 中进行批次目录原子重命名。
  5. 重构主执行路径：
     - 在启动并发解析前：创建 `output` 并调用 `output.prepare_batch()`；
     - 为每个流创建一个 `ChunkWriter` 并在协程内持有；移除集中式消费者与 channel；
     - 引入共享进度计数器，并在 `write_chunk`/`finalize_instance` 成功后输出累计进度日志；
     - 所有协程完成后调用一次 `output.finalize_batch()`。
  6. 复用与抽取重试工具；保证日志字段顺序与项目规范一致。
  7. 回归现有测试（含 parquet 行为）；必要时新增并发写入与进度日志覆盖的测试用例（重点验证：`prepare_batch()` 幂等、批次完成一次、parquet 目录重命名、CH 插入行计数）。


# 建议标题

```text
feat(output): introduce Output/ChunkWriter with prepare_batch/finalize_batch for parallel writes and shared progress
```
