# 开发计划

本计划旨在指导 AI 与人类协作完成开发任务。请遵循以下协作流程：

*   **AI**：负责执行【实现步骤】，并在完成后勾选对应的复选框 (`- [x]`)。AI **绝不能**勾选【验证步骤】。
*   **人类**：负责审核【实现步骤】的产出，并执行【验证步骤】以确保质量。

---

## 阶段一：新增 Parquet 输出的 CLI 接口与配置骨架

目标：在不影响现有 ClickHouse 流程的前提下，新增 Parquet 输出选项及最小配置，并保证可编译。

### 实现步骤
- [ ] 在 `src/bin/rdbinsight.rs` 中为 `OutputCommand` 增加 `IntoParquet(ParquetOutputArgs)`；新增 `ParquetOutputArgs`，参数：
  - `--dir <path>`（必填）
  - `--compression <zstd|snappy|none>`（可选，默认 `zstd`）
- [ ] 在 `src/config.rs` 中增加 Parquet 输出配置结构（如 `ParquetConfig`）与 `OutputConfig::Parquet(ParquetConfig)`；实现基础 `validate()`。
- [ ] 在主流程中解析 CLI 后，依据 `OutputConfig` 分支初始化对应输出（暂时仅创建 `ParquetOutput` 占位结构，不落地写文件）。

### 验证步骤
- [ ] 运行 `cargo build`，确认可编译。
- [ ] 执行 `rdbinsight dump ... into-parquet --dir /tmp/out`（或等价 CLI），确认 CLI 解析无报错（功能待后续阶段实现）。

---

## 阶段二：路径与命名工具（批目录与实例文件）

目标：实现安全可读的批目录名与实例文件名转义，以及目录创建逻辑。

### 实现步骤
- [ ] 新增一个内部工具模块（如 `src/output/path_utils.rs` 或放在 `output/mod.rs` 下）：
  - [ ] `format_batch_dir(OffsetDateTime utc) -> String`：返回 `YYYY-MM-DD_HH-mm-ss.SSSZ`。
  - [ ] `make_tmp_batch_dir(name: &str) -> String`：返回 `tmp_<name>`。
  - [ ] `sanitize_instance_filename(instance: &str) -> String`：将 `:` 替换为 `-`。
  - [ ] `ensure_dir(path: &Path)`：逐层创建目录。
- [ ] 为上述函数添加单元测试，覆盖：
  - 日期格式正确且为 UTC；
  - `127.0.0.1:6379` → `127.0.0.1-6379`；
  - 临时批目录名前缀 `tmp_`；
  - `ensure_dir` 在已存在目录时幂等。

### 验证步骤
- [ ] 运行 `just test`（或 `cargo test`），确认工具函数测试全部通过。

---

## 阶段三：Parquet Schema 与字段映射

目标：依据 `.ai_code/design.md` 定义 Arrow/Parquet Schema，并实现从 `Record` 到 Arrow 列的映射（仅内存对象，尚不写盘）。

### 实现步骤
- [ ] 新建 `src/output/parquet/schema.rs`：
  - [ ] 定义 Arrow `Schema`，字段与类型严格按设计文档：
    - `cluster: Utf8`
    - `batch: Timestamp(Nanosecond, UTC)`
    - `instance: Utf8`
    - `db: Int64`
    - `key: Binary`
    - `type: Utf8`
    - `member_count: Int64`
    - `rdb_size: Int64`
    - `encoding: Utf8`
    - `expire_at: Timestamp(Millisecond, UTC) nullable`
    - `idle_seconds: Int64 nullable`
    - `freq: Int32 nullable`
    - `codis_slot: Int32 nullable`
    - `redis_slot: Int32 nullable`
- [ ] 新建 `src/output/parquet/mapper.rs`：提供 `record_to_columns(records: &[Record], batch_info: &BatchInfo, instance: &str) -> RecordBatch`。
- [ ] 为映射逻辑添加单元测试：
  - `key` 为二进制；
  - `batch` 纳秒精度、UTC；
  - `expire_at` 毫秒精度；
  - 可空字段为 `None` 时正确为空值。

### 验证步骤
- [ ] 运行 `just test`，确认 schema 与映射测试通过。

---

## 阶段四：每实例一个 Parquet Writer（.parquet.tmp 写入与完成重命名）

目标：实现 per-instance 的 Parquet writer 生命周期与压缩配置，实例完成后将 `*.parquet.tmp` 原子重命名为 `*.parquet`。

### 实现步骤
- [ ] 新建 `src/output/parquet/mod.rs` 并导出 `ParquetOutput`：
  - [ ] 管理 `instance -> writer` 的映射（如 `HashMap<String, WriterHandle>`）。
  - [ ] `WriterHandle` 持有目标临时文件路径、Arrow writer 及累计状态。
  - [ ] 压缩：解析 `--compression` 为 Arrow/Parquet 对应选项（默认 ZSTD）。
  - [ ] `write(records, batch_info, instance)`：
    - 若该实例 writer 不存在：
      - 构造批目录路径：`/<dir>/<cluster>/tmp_<batch_dir>/`；`ensure_dir`；
      - 生成实例文件名（`:`→`-`），创建 `*.parquet.tmp` 并初始化 writer；
    - 将第 3 阶段的 `RecordBatch` 追加到 writer。
  - [ ] `finalize_instance(instance)`：关闭 writer 并 `rename(<file>.parquet.tmp -> <file>.parquet)`。
- [ ] 单元测试（使用 `tempfile`）：
  - 写入一个实例的两批数据，关闭后存在 `.parquet` 且无 `.tmp`；
  - 内容行数与写入条数一致。

### 验证步骤
- [ ] 运行 `just test`，确认 Writer 生命周期测试通过。

---

## 阶段五：与主处理流水线集成（按实例并发）

目标：在不影响 ClickHouse 分支的前提下，接入 Parquet 分支，保证每实例独立写入并在实例完成时完成临时文件重命名；全部实例完成后重命名批目录。

### 实现步骤
- [ ] 在 `src/bin/rdbinsight.rs` 中：
  - [ ] 依据 `OutputConfig::Parquet` 分支初始化 `ParquetOutput`，计算 `batch_dir` 名称，并提前创建 `tmp_<batch_dir>` 目录。
  - [ ] 新增 `process_records_to_parquet`（与现有 `process_records_to_clickhouse` 类似的循环）：
    - 复用现有批量缓冲阈值；
    - 每批调用 `ParquetOutput.write(...)`；
    - 流结束时调用 `finalize_instance(instance)`。
  - [ ] 当所有实例完成后，执行批目录重命名：`tmp_<batch_dir> -> <batch_dir>`。
- [ ] 确保日志字段顺序符合规范（`operation` 优先）。

### 验证步骤
- [ ] 运行 `cargo build`；
- [ ] 使用小数据源运行一次 dump（任一来源），确认产物目录：
  - `/<dir>/<cluster>/tmp_<batch_dir>/instance.parquet.tmp`（运行中）
  - 全部结束后为：`/<dir>/<cluster>/<batch_dir>/instance.parquet`
- [ ] 使用 Parquet 工具（如 `parquet-tools` 或 Arrow 读取）检查字段与行数。

---

## 阶段六：基础错误处理与日志完善

目标：提升诊断可用性，确保失败时保留现场并输出高质量日志。

### 实现步骤
- [ ] 在 Writer 初始化、批量写入、实例 finalize、批目录重命名处增加 `info!/warn!/error!` 日志，字段顺序遵循项目规范。
- [ ] 对关键 I/O 操作加上 `anyhow::Context`，包含文件路径与实例信息。
- [ ] 明确在失败时不清理 `.tmp` 与 `tmp_` 目录，仅报错返回。

### 验证步骤
- [ ] 人为制造错误（如只读目录）验证报错信息是否包含上下文；
- [ ] 观察日志是否包含 operation、路径、实例等关键维度。

---

## 阶段七：端到端最小验证

目标：确保该阶段“仅引入 Parquet 输出能力”的目标达成，且不影响 ClickHouse 流程。

### 实现步骤
- [ ] 在 `tests/` 中新增一个集成测试（或临时二进制/脚本）驱动小样本输入到 Parquet 分支（如 `FromFile`）；验证生成文件存在与可读取；
- [ ] 确认对现有 ClickHouse 流程无编译/运行时影响。

### 验证步骤
- [ ] 运行 `just test`；
- [ ] 手动调用两条命令分别走 ClickHouse 与 Parquet 分支，确认二者均可运行且互不影响；
- [ ] 检查 Parquet 产出字段、记录数与输入一致。

---

备注：以上各阶段严格限定在“引入 Parquet 输出能力”的范围内；外部排序、run 归并、断点续写/覆盖策略、高级写出参数等均不在本阶段内。
