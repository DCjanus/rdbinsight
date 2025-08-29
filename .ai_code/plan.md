# 开发计划

本计划旨在指导 AI 与人类协作完成开发任务。请遵循以下协作流程：

*   **AI**：负责执行【实现步骤】，并在完成后勾选对应的复选框 (`- [x]`)。AI **绝不能**勾选【验证步骤】。
*   **人类**：负责审核【实现步骤】的产出，并执行【验证步骤】以确保质量。

---

## 阶段一：抽象统一接口与数据模型上移

### 实现步骤
- [ ] 新建 `src/report/model.rs`，将 `ReportData` 及相关结构（`DbAggregate`、`TypeAggregate`、`InstanceAggregate`、`TopKeyRecord`、`PrefixAggregate`、`ClusterIssues`、`BigKey`）迁移至该模块（保持序列化字段与现有模板一致）。
- [ ] 新建 trait `ReportDataProvider`（单一方法 `async fn generate_report_data(&self) -> AnyResult<ReportData>`）。
- [ ] 迁移 `report::querier` 中对 `ReportData` 的引用到 `report::model`。
- [ ] 修改 `ReportGenerator` 使其依赖 `ReportDataProvider`，保留模板注入与输出逻辑不变。

### 验证步骤
- [ ] 编译通过，无 clippy/lint 新增告警。
- [ ] 现有 ClickHouse 报告路径的单元/集成测试仍然通过（如有）。

---

## 阶段二：Parquet 写入端增强（版本化元数据 + 行组对齐）

### 实现步骤
- [ ] 保持写入排序不变：`(cluster, batch, instance, db, key)`；`sorting_columns` 无需改动。
- [ ] 在 `output/parquet/merge.rs` 的合并流程中：
  - [ ] 统计并生成元数据摘要（MessagePack 结构）：`cluster`、`batch_unix_nanos`、`instance`、`total_key_count`、`total_size_bytes`、`per_db[]`、`per_type[]`、`top_keys_full[<=100]`（完整 `Record`）、可选 `slots`（`codis_slots[]`、`redis_slots[]`）。
  - [ ] 增加“按 db 的行组对齐”策略：构造 Arrow RecordBatch 时，检测 `db` 变化即结束当前批次，确保一个 Row Group 内不跨越多个 `db`。Row Group 大小上限继续遵循既有阈值（如 8192），但以 `db` 边界优先。
  - [ ] 生成 per-db 行组索引并写入元数据：`per_db_row_groups: Map<db, Vec<row_group_idx>>`，用于读取阶段快速建立“每个 db 的子流游标”。
  - [ ] 将摘要使用 MessagePack 编码并 Base64；通过 WriterProperties 的 key-value metadata 写入：`rdbinsight.meta.version = "1"`、`rdbinsight.meta.encoding = "msgpack"`、`rdbinsight.meta.summary.b64 = <base64>`。
- [ ] 补充/更新必要的结构体与（反）序列化实现，确保 `Record` 可无损编码，`per_db_row_groups` 序列化正确。

### 验证步骤
- [ ] 端到端导出 Parquet 后，使用工具读取 footer 验证 KV 元数据存在且可解析（含 `per_db_row_groups`）。
- [ ] 抽样读取多个 Row Group，确认单个 Row Group 内 `db` 唯一（可临时投影 `db` 列进行校验）。
- [ ] 评估元数据体积（目标：每文件几十 KB 量级）。

---

## 阶段三：Parquet 报告 Provider（读取元数据 + 前缀扫描）

### 实现步骤
- [ ] 新建 `src/report/parquet.rs` 实现 `ParquetReportProvider { base_dir, cluster, batch_slug }`：
  - [ ] 目录发现：`cluster=<cluster>/batch=<slug>` 下枚举 `*.parquet`。
  - [ ] 读取并校验元数据：`version == "1"`、`encoding == "msgpack"`、存在 `summary.b64`；反序列化 MessagePack。
  - [ ] 基于元数据合成：`db_aggregates`、`type_aggregates`、`instance_aggregates`、`total_size`、`top_keys`（完整 `Record`）与槽倾斜判定。
  - [ ] 构建“主扫描”（仅用于前缀）：
    - [ ] Arrow/Parquet Reader 开启列投影 `{ key, rdb_size }`。
    - [ ] 多游标子流初始化：依据 `per_db_row_groups`，为每个文件的每个 `db` 建立一个子流游标，游标按其行组顺序读取（每个 Row Group 内按 `key` 递增）。
    - [ ] 全局 K 路归并（按 key 的小根堆）：堆元素为 `(key_bytes, stream_id, rdb_size)`；每轮抽干相同 key 的分组，得到 `(key_min, sum_rdb_size, dup_count)`。
    - [ ] 预先阈值：`threshold = max(1, total_size/100)`。
    - [ ] 活跃前缀链 + 固定阈值过滤：按 LCP 算法封闭/扩展、累计体积与数量；封闭时若 `>= threshold` 则收集；结束后对剩余活跃前缀做相同判断；字典序输出。
  - [ ] 汇总为 `ReportData` 并返回。

### 验证步骤
- [ ] 使用小样本构造多实例、多 db、多 type、多槽数据，验证输出 JSON 与 ClickHouse 结果在口径上对齐（允许格式化细差）。
- [ ] 验证仅投影 `{ key, rdb_size }` 即可完成前缀扫描，且性能优于全列读取。
- [ ] 针对 `per_db_row_groups` 的子流读取正确性测试：确保所有行被覆盖且不重不漏。

---

## 阶段四：CLI 扩展（from-parquet）

### 实现步骤
- [ ] 在 CLI 增加 `report from-parquet` 子命令，参数：`--dir`、`--cluster`、`--batch?`、`--output?`。
- [ ] 批次解析：缺省从 `cluster=<cluster>` 下最新 `batch=<slug>` 目录选择（按 slug 字典序或 mtime 兜底）。
- [ ] 调用 `ParquetReportProvider` 生成 `ReportData`，复用现有模板输出 HTML。

### 验证步骤
- [ ] 运行 `report from-parquet` 生成报告文件，校验输出命名与默认路径。
- [ ] 人工打开 HTML 确认前端渲染正确、交互正常。

---

## 阶段五：测试完善与文档

### 实现步骤
- [ ] 单元测试：
  - [ ] 元数据（写入/读取）版本与编码校验。
  - [ ] LCP 前缀扫描的关键路径（跨 db、多样 key），验证固定阈值过滤。
  - [ ] K 路归并重复 key 分组逻辑（相同 key 被连续处理；sum 与 count 正确）。
  - [ ] Row Group 对齐测试：保证 Row Group 内 `db` 唯一；`per_db_row_groups` 与实际数据一致。
- [ ] 端到端测试：
  - [ ] 从 Redis 样本输出到 Parquet -> `report from-parquet` -> HTML，核对聚合、Top100、槽倾斜与前缀结果。
- [ ] 文档：README 增加 from-parquet 用法与限制（需本工具生成的特定元数据版本）。

### 验证步骤
- [ ] CI 通过：编译、lint、测试全绿。
- [ ] 文档渲染检查，无死链与格式问题。
