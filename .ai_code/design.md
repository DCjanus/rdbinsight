## 背景与思路

当前 `ChunkWriter` 以批为单位写入。上游在 `handle_stream_with_prepared_writer` 聚合到大批（通常百万级）再调用 `write_chunk`，导致：
- 上游必须持有大批 `Vec<Record>` 内存；
- ClickHouse SDK 在 `Insert`/`Inserter` 内部也持有 row-binary 缓冲，峰值内存双份驻留；
- 批大小与后端耦合，难以针对不同后端优化。

目标：
- 将写入抽象改为“逐条（或内部小批）流式写入”，把批策略内聚到各后端；
- ClickHouse 每个实例使用长生命周期 inserter，底层以固定 buffer（~256KB）连续刷写；实例结束时统一 `end()`；
- Parquet 在 writer 内部按固定 10,000 行攒批写入；
- 取消 writer 层的重试；
- 进度上游按节流聚合（例如每实例每 1,000,000 行）更新，接受非精确。

## 关键设计

- 接口变更（破坏性）
  - `ChunkWriter`：由 `write_chunk(Chunk)` 改为“逐条写入”接口 `write_record(Record, ctx)`（命名以实现为准），保留 `prepare_instance()` 与 `finalize_instance()`。
    - “逐条写入”仅表示调用方逐条驱动；后端可在内部继续以小批/缓冲实现合适的写入策略。
    - 不再提供/依赖 chunk 级别的重试辅助方法；错误直接向上冒泡。
  - `Output` 接口保持：`prepare_batch()`、`create_writer(instance)`、`finalize_batch()` 不变。

- ClickHouse 实现（`ClickHouseChunkWriter`）
  - 结构：
    - 持有 `Client`；
    - 持有长生命周期 `Inserter<RedisRecordRow>`（“多次 INSERT 的句柄”）；
    - Inserter 配置：`with_max_rows = 10_000_000`（每达到该行数自动分段发送；由 SDK 负责按 256KB 分片刷写）；
    - 需启用 crates feature：`clickhouse` crate 的 `inserter` feature。
  - 生命周期：
    - `prepare_instance()`：通过 `Client::inserter("redis_records_raw").with_max_rows(10_000_000)` 创建 `Inserter`；
    - `write_record(record)`：将 `Record` 转为 `RedisRecordRow`，调用 `inserter.write(&row).await`；
      - SDK 内部以约 256KB 分片刷写到 socket，调用方无需另做 flush；
    - `finalize_instance()`：调用 `inserter.end().await` 结束本实例的写入；
  - 重试：无。写入错误向上冒泡，实例任务失败；上游不缓存未确认数据，也不做重复提交控制；
  - 顺序：无强约束，同实例内/同批次内乱序不影响语义（仍按单实例一个 writer 顺序调用）。

- Parquet 实现（`ParquetChunkWriter`）
  - 结构：
    - 持有 `AsyncArrowWriter<File>`；
    - 内部“列式构建缓冲”（如列向量/`MutableArray` 之类的 builder）与计数器；
    - 固定小批阈值：每 10,000 行触发一次 flush，构建并写入一个 Arrow `RecordBatch`；
  - 生命周期：
    - `prepare_instance()`：打开 `AsyncArrowWriter`；
    - `write_record(record)`：将单条 `Record` 追加进内部列式缓冲；当累计 10,000 行时，构建 `RecordBatch` 并 `writer.write(batch).await`；
    - `finalize_instance()`：flush 剩余不足 10,000 行的缓冲并 `writer.close().await`；执行文件 `rename` 至最终路径；
  - 重试：无。

- 进度汇报（节流且近似）
  - 定义 `REPORT_INTERVAL = 1_000_000`（行）；
  - 每个实例处理协程内维护 `processed_since_report` 与 `total_processed`：
    - 每处理完一行（即成功提交给 writer 的 `write_record` 调用返回）就递增计数；
    - 当 `processed_since_report >= REPORT_INTERVAL` 时，统一调用一次进度更新（按本实例新增数），随后将 `processed_since_report` 归零；
    - 实例结束时，若仍有剩余（< REPORT_INTERVAL），再做一次进度补记；
  - 准确性：接受不精确（在崩溃时可能少记最后不足阈值的部分，或多实例并发导致总体显示滞后）；
  - 性能：避免每条记录都进度更新带来的锁竞争与日志开销。

- 失败与恢复
  - Writer 层无重试；任何 `write_record`/`finalize_instance` 错误直接返回，实例任务失败；
  - ClickHouse 的 inserter 在实例结束时落盘；
  - 整体恢复策略依赖于上层“重跑批次”能力（与现有行为一致或更简化）。

- 并发与可观测性
  - 保持“每实例一个 writer，一个异步任务”的并发模型；
  - 事件日志：
    - ClickHouse：inserter 打开/结束事件；
    - Parquet：每次 10k flush 事件（含行数与占用大小）；
    - 进度：仍按现有节流打印，增加“实例累计行数”。

## 实施建议

- 接口与调用链
  1) 修改 `ChunkWriter`：移除/弃用 `write_chunk(Chunk)`，新增 `write_record(record: Record, ctx: {cluster, batch_ts, instance})`；
  2) 调整 `ChunkWriterEnum` 的分发实现；
  3) 修改 `handle_stream_with_prepared_writer`：
     - 移除上游大 `Vec<Record>` 缓冲；
     - 逐条从 `RecordStream` 读取后直接调用 `writer.write_record(...)`；
     - 引入每实例节流计数（`REPORT_INTERVAL`），按阈值调用一次进度更新；实例收尾时做一次补记；
     - 取消 `write_chunk_with_retry`、`finalize_instance_with_retry` 调用链与相关退避逻辑；

- ClickHouse 实现
  4) `ClickHouseChunkWriter`：
     - 在 `prepare_instance()` 通过 `Client::inserter("redis_records_raw").with_max_rows(10_000_000)` 创建 `Inserter<RedisRecordRow>`（启用 crate feature `inserter`）；
     - `write_record(...)` 里转换并 `inserter.write(&row).await`；
     - `finalize_instance()` 调用 `inserter.end().await`；

- Parquet 实现
  5) `ParquetChunkWriter`：
     - 在 `prepare_instance()` 打开 `AsyncArrowWriter`；
     - `write_record(...)` 采用列式 builder 聚合；累计 10,000 行时构建 `RecordBatch` 并写入；
     - `finalize_instance()` flush 余量并关闭 + rename；10,000 行阈值作为固定常量；

- 其他
  6) 依赖与特性：在 `Cargo.toml` 中为 `clickhouse` crate 启用 `inserter` feature；
  7) 更新单元/集成测试：
     - ClickHouse：验证使用 `Inserter`（配置 `with_max_rows`）持续写入、在 `end()` 后数据完整且可查询（测试中将 `with_max_rows` 设为较小值以便更快触发分段并验证行为）；
     - Parquet：验证固定 10,000 行 flush 的行为与最终文件可读性、行数一致性；
     - 进度：验证节流逻辑在不同记录数下的累计效果；
  8) 文档与示例：更新 README/命令帮助，说明内存与吞吐的变化；
  9) 日志：增加 inserter 打开/结束、Parquet 10k flush 的 `info/debug` 日志；

## 建议标题

```text
feat(output): switch to per-record ChunkWriter; long-lived ClickHouse Inserter with max_rows=10M; Parquet 10k micro-batches; throttled progress updates
```
