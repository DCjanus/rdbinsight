# 开发计划

本计划旨在指导 AI 与人类协作完成开发任务。请遵循以下协作流程：

*   **AI**：负责执行【实现步骤】，并在完成后勾选对应的复选框 (`- [x]`)。AI **绝不能**勾选【验证步骤】。
*   **人类**：负责审核【实现步骤】的产出，并执行【验证步骤】以确保质量。

---

## 阶段一：输出抽象与类型脚手架

### 实现步骤
- [x] 新增 `output::types`（或 `output::mod` 顶层）定义 `Chunk`：`{ cluster: String, batch_ts: OffsetDateTime, instance: String, records: Vec<Record> }`。
- [x] 新增 `output::sink`（或 `output::mod` 顶层）定义 `enum Output { ClickHouse(ClickHouseOutput), Parquet(ParquetOutput) }`，并为其添加方法签名：
  - `async fn write_chunk(&mut self, chunk: Chunk) -> AnyResult<()>`
  - `async fn finalize_instance(&mut self, instance: &str) -> AnyResult<()>`
  - `async fn finalize_batch(self) -> AnyResult<()>`
- [x] 在 `config::OutputConfig` 增加工厂方法：`create_output(&self, cluster: &str, batch_ts: OffsetDateTime) -> AnyResult<Output>`（仅声明与空实现/`todo!`，暂不影响现有逻辑）。

### 验证步骤
- [x] 运行构建：`cargo build` 应通过。
- [x] Lint（如有）：`cargo clippy --all-targets --all-features -D warnings` 应无新告警。

---

## 阶段二：实现 Output 枚举分支（不改动现有调用路径）

### 实现步骤
- [ ] 在 `ClickHouseOutput` 中实现 `Output::write_chunk` 与 `Output::finalize_batch`/`finalize_instance` 分支逻辑：
  - 复用当前 `write()` 的批量插入逻辑，将 `Chunk` 字段映射到行模型。
  - `finalize_instance` 为 no-op。
  - `finalize_batch(self)` 使用 `cluster + batch_ts` 执行批次提交。
- [ ] 在 `ParquetOutput` 中实现 `Output::write_chunk` 与 `finalize_instance`/`finalize_batch` 分支逻辑：
  - 复用当前按实例 `WriterHandle` 写入与 `finalize_instance`/`finalize_batch` 语义。
- [ ] 完成 `OutputConfig::create_output` 的返回枚举（仍不在主流程启用）。

### 验证步骤
- [ ] 运行构建：`cargo build` 应通过。
- [ ] 运行单测：`just test` 应全部通过（不修改现有测试）。

---

## 阶段三：统一生产者-消费者流水线（新增，不移除旧逻辑）

### 实现步骤
- [ ] 在 `src/bin/rdbinsight.rs` 中新增统一函数：
  - `process_streams_with_output(streams, output: Output, cluster: String, batch_ts: OffsetDateTime, concurrency: usize, channel_capacity: usize, batch_size: usize)`：
    - 定义 `enum OutputMsg { Chunk(Chunk), InstanceDone { instance: String } }`；
    - 生产者：对每个 `RDBStream` 并发解析，按 `batch_size` 封装 `Chunk` 发送，结束发送 `InstanceDone`；
    - 消费者：持有 `Output`，循环 `recv`；收到 `Chunk` 调用 `write_chunk`，收到 `InstanceDone` 调用 `finalize_instance`；drain 完后 `finalize_batch(self)`；
    - 对 `write_chunk`/`finalize_instance`/`finalize_batch` 使用统一 `backoff` 重试策略；
    - 输出进度日志（累计 records、实例完成数/总数）。
- [ ] 在 `dump_records` 中接入新流水线（保留旧的 `process_streams_to_clickhouse/parquet` 代码未删，作为回退）。
  - ClickHouse/Parquet 分支均通过工厂创建 `Output` 并调用统一函数。
  - 选择合适的默认 `channel_capacity`/`batch_size`（延续现有 `BATCH_SIZE=1_000_000`；`capacity = concurrency * 4`）。

### 验证步骤
- [ ] 运行构建：`cargo build` 应通过。
- [ ] 运行单测：`just test` 应全部通过。
- [ ] 手动冒烟（可选）：使用 `FromFile → Parquet` 路径在小样本上运行，确认能生成 parquet 文件且目录 `tmp_...` → 最终目录 rename 生效。

---

## 阶段四：移除 BatchInfo 并完成内部引用替换

### 实现步骤
- [ ] 从 `src/output/clickhouse.rs` 移除 `BatchInfo` 定义与所有引用，改为内部持有 `cluster: String` 与 `batch_ts: OffsetDateTime` 字段；相应更新 `record_to_row` 参数与调用点。
- [ ] 从 `src/output/parquet/*` 移除对 `clickhouse::BatchInfo` 的依赖；`record_to_columns`、`ParquetOutput::new` 等改用 `cluster + batch_ts`。
- [ ] 更新相关测试：
  - `src/output/parquet/mapper.rs` 测试：删除 `create_test_batch_info()`，改用 `cluster + OffsetDateTime`；
  - `src/output/parquet/mod.rs` 测试：`ParquetOutput::new(dir, compression, cluster, &batch_ts)` → 去掉 `&`，直接传 `OffsetDateTime`；
  - `src/output/clickhouse.rs` 测试：`record_to_row` 入参改为 `cluster + batch_ts`。

### 验证步骤
- [ ] 运行构建：`cargo build` 应通过。
- [ ] 运行相关单测：`cargo nextest run -p rdbinsight -- tests::parquet_output_*` 与 `cargo nextest run -p rdbinsight -- output::clickhouse::tests::test_record_to_row_conversion` 应通过。
- [ ] 全量测试：`just test` 应全部通过。

---

## 阶段五：删除旧分支流水线与 Parquet 旧并发路径

### 实现步骤
- [ ] 删除 `process_streams_to_clickhouse` 与 `process_streams_to_parquet` 旧函数及其调用；确保 `dump_records` 仅使用统一流水线。
- [ ] 移除 Parquet 路径中 `Arc<Mutex<ParquetOutput>>` 的旧并发写入逻辑与相关辅助函数；统一改为单消费者持有可变引用。
- [ ] 清理不再使用的 `use` 与死代码。

### 验证步骤
- [ ] 运行构建：`cargo build` 应通过（确认无遗留引用）。
- [ ] 全量测试：`just test` 应全部通过。

---

## 阶段六：进度输出与日志一致性收敛

### 实现步骤
- [ ] 在统一消费者中标准化进度日志：
  - 累计 `processed_records`、`completed_instances/total_instances`、`rps`；
  - 遵循日志规范：`operation` 字段置前，结尾为描述字符串；实例以 `IP:Port` 标识。
- [ ] 将重试告警统一为 `warn!(operation = "*_retry", ...)`；失败链打印 `error_chain`。

### 验证步骤
- [ ] 运行一次 `FromFile → Parquet` 冒烟，检查日志字段与格式是否与规范一致。

---

## 阶段七：端到端集成测试（新增）

### 实现步骤
- [ ] 新增集成测试：多实例（或模拟两条 `RDBStream`）→ 统一流水线 → Parquet 输出：
  - 断言：每实例输出文件存在；临时目录已重命名；记录数量与输入一致。
- [ ]（可选）ClickHouse 端到端：若测试环境具备 CH，执行最小写入并校验 `import_batches_completed` 是否存在对应 `cluster + batch_ts` 行。

### 验证步骤
- [ ] 运行新增集成测试：`cargo nextest run -p rdbinsight -- tests::pipeline_*`（具体名称以实现为准）。
- [ ] 全量测试：`just test` 应全部通过。

---

## 注意事项
- 每个阶段结束必须保持构建与测试通过，避免在一个阶段引入下一阶段才会使用的接口或依赖。
- 如需修改 README/README.zh_CN，务必双向同步内容保持一致（若涉及到使用说明变更）。
- 性能调优（如 `channel_capacity` 与 `batch_size`）在功能正确后再行评估与调整。
