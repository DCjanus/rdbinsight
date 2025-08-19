# 开发计划

本计划旨在指导 AI 与人类协作完成开发任务。请遵循以下协作流程：

*   **AI**：负责执行【实现步骤】，并在完成后勾选对应的复选框 (`- [x]`)。AI **绝不能**勾选【验证步骤】。
*   **人类**：负责审核【实现步骤】的产出，并执行【验证步骤】以确保质量。

---

## 阶段一：引入新抽象（Output/ChunkWriter）与准备接口

### 实现步骤
- [x] 在 `output/` 新增模块（建议：`output/abstractions.rs`），定义 trait：
  - `trait Output { async fn prepare_batch(&self) -> AnyResult<()>; async fn create_writer(&self, instance: &str) -> AnyResult<Box<dyn ChunkWriter + Send>>; async fn finalize_batch(self: Box<Self>) -> AnyResult<()>; }`
  - `trait ChunkWriter { async fn write_chunk(&mut self, chunk: Chunk) -> AnyResult<()>; async fn finalize_instance(&mut self) -> AnyResult<()>; }`
- [x] 暂不接入现有调用链，确保新增模块可编译（添加必要的 `use`）。
- [x] 保持现有 `output::sink::Output` 枚举与旧调用路径不变，确保编译通过。

### 验证步骤
- [ ] 运行 `just test`，确认无编译错误、现有测试均通过。
- [ ] 检查新文件导出是否符合现有项目模块结构（通过 `cargo check`）。

---

## 阶段二：ClickHouse 新实现（并行友好）

### 实现步骤
- [x] 在 `output/clickhouse.rs` 内新增基于新接口的实现（避免命名冲突，暂用 `ClickHouseOutputV2` 与 `ClickHouseChunkWriter`）。
  - [x] `ClickHouseOutputV2`：持有 `Client` 构造器上下文、`cluster`、`batch_ts`、`ClickHouseConfig`；实现 `Output`：
    - `prepare_batch()`：校验/建表（幂等）。
    - `create_writer(instance)`：创建独立 `Client` 的 `ClickHouseChunkWriter`。
    - `finalize_batch()`：写入 `import_batches_completed` 完成标记。
  - [x] `ClickHouseChunkWriter`：实现 `ChunkWriter`：
    - `write_chunk()`：按现有行转换逻辑写入 `redis_records_raw`（沿用 `record_to_row_from_chunk`）。
    - `finalize_instance()`：no-op。
- [x] 不改动旧 `output::sink::Output` 调用路径。

### 验证步骤
- [x] 运行 `just test`，确认编译通过、测试不回退。
- [x] 代码检查：日志调用字段顺序符合规范（`operation` 首位）。

---

## 阶段三：Parquet 新实现（实例级文件写入）

### 实现步骤
- [ ] 在 `output/parquet/` 基于现有实现新增 `ParquetOutputV2` 与 `ParquetChunkWriter`：
  - [ ] `ParquetOutputV2` 实现 `Output`：
    - `prepare_batch()`：创建批次临时目录并记录 `final_batch_dir`。
    - `create_writer(instance)`：创建该实例专属 `.parquet.tmp` 的 writer（`AsyncArrowWriter<File>`）。
    - `finalize_batch()`：确保所有 writer 已关闭后，将批次临时目录原子重命名为最终目录。
  - [ ] `ParquetChunkWriter` 实现 `ChunkWriter`：
    - `write_chunk()`：将 `Chunk` 转 `RecordBatch` 并写入 writer。
    - `finalize_instance()`：关闭 writer 并将临时文件重命名为最终文件。
- [ ] 保持旧路径不变。

### 验证步骤
- [ ] 运行 `just test`，确保现有 parquet 测试仍通过（新类型未接入，不应影响）。
- [ ] 代码检查：目录与文件命名策略与现有实现一致（避免行为变化）。

---

## 阶段四：配置层对接（返回 trait object）

### 实现步骤
- [ ] 在 `config.rs` 中为 `OutputConfig` 新增方法 `create_output_v2(cluster, batch_ts) -> AnyResult<Box<dyn Output + Send + Sync>>`：
  - [ ] ClickHouse 分支：返回 `ClickHouseOutputV2`。
  - [ ] Parquet 分支：返回 `ParquetOutputV2`。
- [ ] 暂不替换旧的 `create_output`，避免影响现有流程。

### 验证步骤
- [ ] 运行 `cargo check` 确保可编译；`just test` 确认测试不受影响。

---

## 阶段五：新增并发写入流程（不替换旧流程）

### 实现步骤
- [ ] 在 `src/bin/rdbinsight.rs` 中新增独立函数（建议：`process_streams_with_direct_writers`）：
  - [ ] 基于 `OutputConfig::create_output_v2` 获得 `output`。
  - [ ] 调用 `output.prepare_batch()`。
  - [ ] 为每个 `RDBStream` 启动解析协程：
    - [ ] 协程内 `output.create_writer(instance)` 获取 `ChunkWriter`。
    - [ ] 按现有逻辑解析、分批组装 `Chunk` 并直接 `write_chunk_with_retry`。
    - [ ] 完成后 `finalize_instance_with_retry`。
  - [ ] 全部完成后，调用 `output.finalize_batch()`。
- [ ] 引入进度统计（互斥锁保护）：
  - [ ] 定义 `ProgressState { processed_records: u64, completed_instances: usize, start_time: Instant }`，用 `Arc<tokio::sync::Mutex<_>>` 共享。
  - [ ] 在 `write_chunk()` 成功后、`finalize_instance()` 成功后进入锁临界区更新计数并输出日志（临界区快照策略，尽量缩小范围；日志遵循规范）。
  - [ ] 可选节流：在临界区内按时间阈值（≥200ms）控制输出频率。
- [ ] 复用现有 `write_chunk_with_retry`、`finalize_instance_with_retry`（如需，抽象为通用工具）。

### 验证步骤
- [ ] 使用小规模输入（本地 RDB 或 fixtures）进行试跑，观察日志是否单调、无交错困惑。
- [ ] 运行 `just test`，确保新增函数未破坏既有测试。

---

## 阶段六：切换主流程到新架构并移除旧实现

### 实现步骤
- [ ] 在 `dump_records()` 中将调用从 `process_streams_with_output` 切换为 `process_streams_with_direct_writers`。
- [ ] 移除旧的生产者-消费者通道与 `run_consumer/run_producers` 等关联代码。
- [ ] 删除旧的 `output::sink::Output` 枚举及其方法，移除不再使用的路径。
- [ ] 清理 ClickHouse/Parquet 旧实现中仅供旧路径使用的 API，保留/重命名 `V2` 类型为最终名称（`ClickHouseOutput`、`ParquetOutput`）。
- [ ] 统一命名与导出路径，确保公开 API 与设计文档一致。

### 验证步骤
- [ ] 运行 `just test` 全量测试。
- [ ] 对 Parquet 路径执行现有测试（目录与文件重命名语义应保持一致）。
- [ ] 如有 ClickHouse 集成测试环境，执行相关测试（或以 mock 方式验证 `finalize_batch` 被调用一次）。

---

## 阶段七：并发与稳健性测试

### 实现步骤
- [ ] 新增或扩充测试：
  - [ ] 单实例、多实例并发写入（在 Parquet 路径更易验证）：
    - [ ] 验证每实例生成一个最终文件；
    - [ ] 验证批次目录重命名只发生一次；
    - [ ] 验证 `finalize_instance()` 必须先于批次 `finalize_batch()`。
  - [ ] 进度日志：
    - [ ] 在模拟高并发/小批量条件下，验证日志数与趋势合理（可统计日志条数与最大/最小值单调性）。
  - [ ] 失败与重试（可注入错误/使用测试替身）：
    - [ ] `write_chunk_with_retry` 在前几次失败后成功的路径；
    - [ ] `finalize_instance_with_retry` 的重试路径。

### 验证步骤
- [ ] 运行新增测试：`cargo nextest run -p <crate> <filter>` 或 `just test`。
- [ ] 检查日志字段顺序（`operation` 首位）与信息完备性。

---

## 阶段八：清理与文档

### 实现步骤
- [ ] 全局查找未使用代码与导入，移除冗余。
- [ ] 更新 README 与 `README.zh_CN.md`（如涉及使用方式/性能描述变更），确保两者同步更新（内容与结构一致）。
- [ ] 在 `templates/design.md`/报告模板无需改动；如修改 `templates/report.html`，需同步更新 `templates/design.md`（当前无此需求）。

### 验证步骤
- [ ] 人工审阅 README 中的新架构说明是否清晰，且中英文版本一致。
- [ ] 运行 `just test` 确认最终状态稳定。

---

## 备注与约束

- 日志规范：所有日志调用遵循项目规范，`operation` 字段首位，context 紧随，描述字符串最后。
- 幂等性：`prepare_batch()` 与 `finalize_batch()` 均需保证在失败重试或异常情况下的幂等与一致性（CH/Parquet 分别实现）。
- 依赖管理：如需新增依赖（当前方案使用 `tokio::sync::Mutex` 无需新增依赖），务必使用 `cargo add`/`cargo remove` 管理，保持 `Cargo.lock` 同步。
- 性能：临界区尽量小，仅包含计数更新与日志；必要时开启日志节流。
