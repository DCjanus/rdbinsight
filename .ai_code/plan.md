# 开发计划

本计划旨在指导 AI 与人类协作完成开发任务。请遵循以下协作流程：

*   **AI**：负责执行【实现步骤】，并在完成后勾选对应的复选框 (`- [x]`)。AI **绝不能**勾选【验证步骤】。
*   **人类**：负责审核【实现步骤】的产出，并执行【验证步骤】以确保质量。

---

## 阶段一：路径与命名更新（.run 扩展名，6 位零填充）

将 Run 分段与滚动产物统一命名为 `<instance>.<idx:06>.run`（内容为 Parquet），最终文件为 `<instance>.parquet`。

### 实现步骤
- [x] 更新 `src/output/parquet/path.rs`：
  - [x] 新增/调整生成 run 文件名的函数：`run_filename(instance_sanitized: &str, idx: u64) -> PathBuf`，返回 `<instance>.<idx:06>.run`；
  - [x] 保留/更新最终文件名函数，保证 `<instance>.parquet` 不变；
  - [x] 路径相关日志使用统一的 helper 输出。
- [x] 更新 `src/output/parquet/output.rs`：
  - [x] `ParquetChunkWriter::flush_run_segment` 改为使用新的 `run_filename`，写出 `.run`（内容为 Parquet），并设置完整 `sorting_columns`；
  - [x] 移除对旧的 `*.000N.parquet` 命名的依赖。

### 验证步骤
- [x] 编译与 Lint：`cargo clippy --all-targets --all-features -- -D warnings` 通过；`cargo build` 通过。
- [x] 人工运行一次最小化 dump（或单元测试中的模拟）后检查临时目录，确认生成的文件名符合 `<instance>.<idx:06>.run` 约定，最终文件仍为 `<instance>.parquet`。

---

## 阶段二：MergeContext 重构为“一次合并”的执行器

`MergeContext` 接受一组输入与一个输出，完成一次 k（≤F）路归并；合并成功后删除本次输入。

### 实现步骤
- [x] 重构 `src/output/parquet/merge.rs`：
  - [x] 定义新的 `MergeContext { inputs: Vec<PathBuf>, output: PathBuf, compression: ParquetCompression, ... }` 构造方式；
  - [x] 提供 `merge_once_delete_inputs_on_success(self) -> AnyResult<()>`：
    - [x] 使用最小堆进行 k 路归并，读取 `inputs`，写入 `output`；
    - [x] `ArrowWriter::WriterProperties` 设置完整 `sorting_columns = schema::create_db_key_sorting_columns`；
    - [x] 合并完成并成功 close 后，删除 `inputs`；失败则不删除；
    - [x] 保持在 `spawn_blocking` 中执行合并，避免阻塞 async runtime；
  - [x] 保留/复用现有 `RunCursor`、`OutputBuilders` 与堆逻辑；必要时抽取共用方法；
  - [x] 移除/废弃旧的“全量一次性合并”入口（基于 run_count 的路径）。

### 验证步骤
- [x] 为 `merge.rs` 增补单元测试（可使用小型内存构造或测试夹具）：
  - [x] 构造 2~3 个小型 `.run` 输入，调用 `merge_once_delete_inputs_on_success` 输出到新的 `.run` 或 `.parquet`，验证：
    - [x] 输出存在且 `(cluster,batch,instance,db,key)` 全局有序；
    - [x] 输出元数据包含完整 `sorting_columns`；
    - [x] 输入文件在成功后被删除；
  - [x] 失败路径（模拟写入失败）时，输入不会被删除（可通过注入错误或临时路径权限控制实现）。

---

## 阶段三：编排器（ParquetChunkWriter）引入“发号器 + 滚动 F 路合并”

`ParquetChunkWriter` 负责：产生 `.run` 文件、维护候选集合、在 `finalize_instance` 阶段按 fan-in 进行滚动合并，直到仅剩一个输出（最终为 `<instance>.parquet`）。

### 实现步骤
- [ ] 在 `ParquetChunkWriter` 中新增：
  - [ ] `issuer: u64` 发号器，从 0 开始单调递增；
  - [ ] `candidates: Vec<PathBuf>` 用于记录本实例产生的所有 `.run` 路径；
- [ ] `flush_run_segment`：
  - [ ] 使用 `issuer` 生成 `out = <instance>.<idx:06>.run`，写出（内容 Parquet）后将 `out` 推入 `candidates`；
  - [ ] 仍使用 `intermediate_compression` 与完整 `sorting_columns`；
- [ ] `finalize_instance`：
  - [ ] 计算 `fan_in = merge_fan_in`（从配置/CLI 注入）；
  - [ ] 若 `candidates.len() <= fan_in`：调用 `MergeContext` 将全部候选合并输出为 `<instance>.parquet`；
  - [ ] 否则循环：
    - [ ] 取按 idx 升序的最小 `fan_in` 个候选为集合 S；
    - [ ] 生成 `out = <instance>.<idx:06>.run`（由 `issuer` 发号）；
    - [ ] 调用 `MergeContext` 将 S 合并到 `out`（使用 `intermediate_compression`），成功后将 `out` 加回 `candidates` 并移除 S；
    - [ ] 重复直到候选数 ≤ fan_in；最后一次合并输出 `<instance>.parquet`（使用最终 `compression`）。
  - [ ] 日志：按规范输出 `operation`、`fan_in`、选取的最小/最大 idx、`next_idx`、耗时与速率。

### 验证步骤
- [ ] 增补集成测试：设置较小的 `run_rows` 强制生成多个 `.run`，设置 `merge_fan_in=3`，验证：
  - [ ] 滚动合并过程结束后，实例目录仅剩 `<instance>.parquet`（无 `.run` 残留）；
  - [ ] 最终文件 `(cluster,batch,instance,db,key)` 有序且 `sorting_columns` 完整；
  - [ ] 运行日志包含 fan-in 与 idx 信息（人工检查或基于测试日志捕获）。

---

## 阶段四：配置与 CLI 参数接入（merge_fan_in）

加入 `merge_fan_in` 参数（默认 64），用于控制滚动合并的最大 fan-in。

### 实现步骤
- [ ] 扩展配置结构体与 CLI：
  - [ ] 在 `config.rs` 与 `src/bin/rdbinsight.rs` 的 into-parquet 分支接入 `merge_fan_in`；
  - [ ] 将该参数传递至 `ParquetOutput` 与 `ParquetChunkWriter`；
- [ ] 更新帮助信息与默认值展示；
- [ ] 日志打印 `merge_fan_in` 生效值。

### 验证步骤
- [ ] `cargo run --bin rdbinsight -- dump ... into-parquet --help` 能看到 `merge_fan_in`；
- [ ] 指定不同 `merge_fan_in` 值时，日志中展示的选取规模与行为符合预期。

---

## 阶段五：可观测性与清理保证

完善关键日志与错误处理，确保合并失败不会误删输入。

### 实现步骤
- [ ] 按日志规范输出：`operation` 字段放首位，随后为上下文字段，描述字符串结尾（遵循项目日志规范记忆）。
- [ ] `MergeContext`：仅在成功 close 输出后删除输入；错误路径保留输入并透出明确错误上下文（实例、输入数量、输出路径、压缩参数）。
- [ ] `ParquetChunkWriter`：在 finalize 编排期间对每轮合并输出耗时、速率、候选数量变化进行记录。

### 验证步骤
- [ ] 人工注入失败（例如输出路径不可写或磁盘满）：确认 `MergeContext` 未删除输入，并输出可定位的错误日志；
- [ ] 正常路径结束后，目录下无 `.run` 残留且有 `<instance>.parquet`。

---

## 阶段六：测试完善与文档同步

### 实现步骤
- [ ] 更新/新增测试：
  - [ ] 路径与命名测试：校验 `.run` 的 6 位零填充与最终文件名；
  - [ ] 滚动合并正确性测试：多 `.run`、`merge_fan_in` 不同取值；
  - [ ] Parquet 元数据测试：`sorting_columns` 为完整列集；
- [ ] 更新 README 与 README.zh_CN 对 Parquet 目录布局与 `merge_fan_in` 参数的简要说明（与英文保持同步）。

### 验证步骤
- [ ] 运行全部测试：`just test`（或 `cargo test --all`）通过；
- [ ] 文档术语与 CLI 帮助一致，示例路径命名与实现一致。

---

## 里程碑与范围说明
- 仅实现：`.run` 命名的滚动 F 路合并、`merge_fan_in` 参数接入、`MergeContext` 作为“一次合并”的执行器并负责成功后的输入删除。
- 不包含：断点续传、目录扫描恢复、`.tmp` 中间名、P0 读取批次/输出批次调优（可后续评估）。
