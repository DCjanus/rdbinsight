# 开发计划

本计划旨在指导 AI 与人类协作完成开发任务。请遵循以下协作流程：

*   **AI**：负责执行【实现步骤】，并在完成后勾选对应的复选框 (`- [x]`)。AI **绝不能**勾选【验证步骤】。
*   **人类**：负责审核【实现步骤】的产出，并执行【验证步骤】以确保质量。

---

## 阶段一：引入逐条写入 API（保持向后兼容）

目标：在不破坏现有编译与行为的前提下，为 `ChunkWriter` 增加 `write_record` 能力，并保留现有 `write_chunk`（作为临时桥接）。

### 实现步骤
- [ ] 在 `src/output/mod.rs` 的 `ChunkWriter` trait 中新增 `write_record(record: Record, ctx: {cluster, batch_ts, instance})` 方法（带默认实现：构造单条 `Chunk` 调用现有 `write_chunk`）。
- [ ] 在 `ChunkWriterEnum` 的实现中转发新的 `write_record` 方法到各具体 writer。
- [ ] 保持所有现有实现（ClickHouse、Parquet）无需改动即可编译（因为有默认实现）。
- [ ] 运行 `cargo check` 和 `cargo clippy`，确保无编译错误并处理新 API 的基础告警（仅必要的最小改动）。

### 验证步骤
- [ ] 人类：确认新增方法签名与 `.ai_code/design.md` 一致，参数命名与类型符合项目风格。
- [ ] 人类：确认引入 `write_record` 后，现有功能和测试仍可通过 `cargo test`（不修改任何业务逻辑的前提下）。

---

## 阶段二：ClickHouse 改造为 Inserter + per-record 写入

目标：`ClickHouseChunkWriter` 使用长生命周期 `Inserter`（`with_max_rows(10_000_000)`），实现 `write_record`；`finalize_instance` 调用 `end()`；不做重试。

### 实现步骤
- [ ] 在 `Cargo.toml` 启用 `clickhouse` crate 的 `inserter` feature。
- [ ] 在 `src/output/clickhouse.rs` 中：
  - [ ] 为 `ClickHouseChunkWriter` 增加 `Inserter<RedisRecordRow>` 成员（必要时使用 `Option` 管理生命周期）。
  - [ ] 在 `prepare_instance()` 中通过 `client.inserter("redis_records_raw").with_max_rows(10_000_000)` 创建 `Inserter`。
  - [ ] 实现 `write_record(record, ctx)`：将 `Record` 映射为 `RedisRecordRow` 并 `inserter.write(&row).await`。
  - [ ] 在 `finalize_instance()` 调用 `inserter.end().await` 并清理资源。
  - [ ] 保留现有 `write_chunk`（如果 trait 仍要求），利用默认实现桥接到 `write_record` 或标注弃用。
- [ ] 运行 `cargo check` 和必要的最小修复。

### 验证步骤
- [ ] 人类：检查 `Client::inserter` + `with_max_rows(10_000_000)` 的使用是否正确；参考文档确认 API 行为一致（`Inserter` 自动分片到 socket，`end()` 统一完成）
  - 参考：`[Client::inserter](https://docs.rs/clickhouse/latest/clickhouse/struct.Client.html#method.inserter)`、`[Inserter](https://docs.rs/clickhouse/latest/clickhouse/inserter/struct.Inserter.html)`。
- [ ] 人类：本阶段先不改调用方，运行 `cargo test -q` 确认编译与现有测试通过。

---

## 阶段三：Parquet 改造为内部 10k 小批聚合 + per-record 写入

目标：`ParquetChunkWriter` 实现 `write_record`，内部以固定 10,000 行聚合构建 `RecordBatch` 写入；收尾 flush；无重试。

### 实现步骤
- [ ] 在 `src/output/parquet/output.rs` 中：
  - [ ] 为 `ParquetChunkWriter` 增加内部行缓冲（例如 `Vec<Record>` 或列式 builder 结构）与计数器；常量 `MICRO_BATCH_ROWS = 10_000`。
  - [ ] 在 `write_record(record, ctx)` 中：追加记录，达到 10,000 行时构建 `RecordBatch` 并 `writer.write(batch).await`；清空缓冲。
  - [ ] 在 `finalize_instance()` 中，若缓冲非空则构建一次 `RecordBatch` 写入；随后 `close()` 并 `rename`。
  - [ ] 保持现有 schema、文件命名、目录逻辑不变。
- [ ] 运行 `cargo check` 修复编译问题。

### 验证步骤
- [ ] 人类：使用少量数据跑一次 Parquet 输出路径，确认生成文件可被 Arrow/Parquet 读取，行数与输入一致。
- [ ] 人类：检查日志中出现 10k flush 相关的 info/debug（或通过断点/打印确认触发）。

---

## 阶段四：上游改造为逐条驱动 + 节流进度

目标：修改 `handle_stream_with_prepared_writer` 去除大缓冲，逐条调用 `write_record`；按实例每 1,000,000 行节流更新进度；取消 writer 层重试调用链。

### 实现步骤
- [ ] 在 `src/bin/rdbinsight.rs`：
  - [ ] 移除以 `batch_size` 为阈值的 `Vec<Record>` 缓冲逻辑，改为每条从 `RecordStream` 读取后立即调用 `writer.write_record(record, ctx)`；
  - [ ] 引入 `REPORT_INTERVAL = 1_000_000` 常量；维护本实例的 `processed_since_report` 计数；达到阈值时调用一次 `update_progress_after_write`，并清零计数；
  - [ ] 末尾补记剩余进度（不足阈值的部分）；
  - [ ] 删除 `write_chunk_with_retry`、`finalize_instance_with_retry` 的调用，改为直接调用 `writer.write_record` 与 `writer.finalize_instance`；
  - [ ] 清理未使用的 `batch_size` 参数与相关日志描述（如仍保留 CLI 参数，暂时忽略但不使用，或在后续阶段处理）。
- [ ] 运行 `cargo check` 并修复编译问题。

### 验证步骤
- [ ] 人类：运行一次小规模任务，观察进度打印是否按约每 1,000,000 行更新；
- [ ] 人类：确认内存峰值显著降低（无大 `Vec<Record>` 堆积），ClickHouse SDK 未出现双份峰值；
- [ ] 人类：确认失败时错误直接冒泡，任务终止，无重试逻辑。

---

## 阶段五：移除旧批量 API 与重试逻辑（清理）

目标：删除 `write_chunk` 相关 API 与调用，彻底迁移到 per-record；清理重试与无用代码。

### 实现步骤
- [ ] 在 `src/output/mod.rs` 的 `ChunkWriter` trait 中移除 `write_chunk` 方法与相关注释；确保 `ChunkWriterEnum` 不再分发该方法。
- [ ] 删除 `write_chunk_with_retry`、`finalize_instance_with_retry` 及其依赖（若有专用 backoff 工具仅用于此路径，也一并移除或保留给其他用途）。
- [ ] 全局搜索 `write_chunk(` 引用并逐一删除或迁移。
- [ ] 若 `output::types::Chunk` 已仅被 Parquet 内部使用，保留其定义但降低可见性；否则按需保留。
- [ ] 运行 `cargo clippy` 清理告警（只处理与本次变更直接相关的告警）。

### 验证步骤
- [ ] 人类：`cargo test` 全量通过（本地可跳过依赖外部服务的测试，或使用容器）。
- [ ] 人类：确认代码中不再存在对旧批量 API 的引用。

---

## 阶段六：测试增强与示例

目标：补充/调整测试以覆盖新行为；确保 ClickHouse 与 Parquet 路径均可验证。

### 实现步骤
- [ ] ClickHouse 测试：在 `tests/report_test.rs` 等位置，确保报告生成路径仍可用；验证 `Inserter` 写入后数据可查询。必要时在测试中降低 `with_max_rows`（通过测试专用配置）以加快分段触发与验证；若容器环境可用，则运行集成测试。
- [ ] Parquet 测试：构造 >10k 行的输入，验证生成多个 `RecordBatch` 且最终行数一致。
- [ ] 进度测试：模拟/注入计数，验证节流更新逻辑的边界条件（恰好 1,000,000，多于/少于阈值）。

### 验证步骤
- [ ] 人类：`cargo test -q` 通过；若环境支持，运行集成测试（ClickHouse 容器）。
- [ ] 人类：对比变更前后内存占用（可用简单基准或观察进程 RSS）。

---

## 阶段七：文档与发布

目标：同步文档、命令帮助，说明行为变化与配置项。

### 实现步骤
- [ ] 更新 README/命令帮助：说明 per-record 写入、ClickHouse Inserter（`with_max_rows=10M`）、Parquet 10k 小批，以及取消 writer 层重试与进度节流策略。
- [ ] 更新变更记录/提交信息，准备 PR。

### 验证步骤
- [ ] 人类：审阅文档，确保外部用户理解升级影响与好处。

---
