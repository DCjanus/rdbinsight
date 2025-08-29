# 开发计划

本计划旨在指导 AI 与人类协作完成开发任务。请遵循以下协作流程：

*   **AI**：负责执行【实现步骤】，并在完成后勾选对应的复选框 (`- [x]`)。AI **绝不能**勾选【验证步骤】。
*   **人类**：负责审核【实现步骤】的产出，并执行【验证步骤】以确保质量。

---

## 阶段一：依赖与数据模型（serde 序列化）

为 LZ4 流式 run 与 bincode 序列化准备依赖，并为数据结构添加序列化派生。

### 实现步骤
- [ ] 使用命令添加依赖（避免手工改 `Cargo.toml`）：
  - [ ] `cargo add serde --features derive`
  - [ ] `cargo add bincode`
  - [ ] `cargo add lz4_flex`
  - [ ] `cargo add crc32fast`
- [ ] 为 `src/record.rs` 中的 `Record`、相关枚举与 `src/parser/core/raw.rs` 中的 `RDBStr` 添加 `Serialize`/`Deserialize` 派生。
- [ ] 确认不引入不必要的 `Clone`/`Copy`；遵循现有命名与风格（日志/注释用英文）。

### 验证步骤
- [ ] `cargo clippy --all-targets --all-features -- -D warnings` 通过；`cargo build` 通过。
- [ ] 简单创建/序列化/反序列化一个 `Record`（单元测试），验证 serde 派生生效。

---

## 阶段二：路径与配置更新（.run.lz4 与默认 run_rows=64K）

规范中间 run 扩展名并更新默认阈值。

### 实现步骤
- [ ] 更新 `src/output/parquet/path.rs`：
  - [ ] 新增/调整 `run_filename(instance_sanitized: &str, idx: u64) -> PathBuf`，返回 `<instance>.<idx:06>.run.lz4`。
  - [ ] 保留最终文件名 `<instance>.parquet` 逻辑不变。
- [ ] 更新 `src/config.rs` 与 CLI（`src/bin/rdbinsight.rs`）默认值与帮助：
  - [ ] 将 `run_rows` 默认值改为 `65536`。
  - [ ] 保留 `merge_fan_in` 参数作为内部 FD 保护的上限（可设置较大默认，如 1024），帮助文案说明“内部可能分批归并”。

### 验证步骤
- [ ] `cargo build` 通过。
- [ ] `--help` 展示的默认值与描述符合预期；run 文件名生成函数返回 `.run.lz4`。

---

## 阶段三：RunWriter（LZ4 Frame + bincode + CRC32）

实现同步 I/O 的流式 run 写入器，在 `spawn_blocking` 中运行。

### 实现步骤
- [ ] 新增模块（建议）：`src/output/parquet/run_lz4.rs`
  - [ ] `RunWriter`：基于 `std::fs::File` + `std::io::BufWriter` + `lz4_flex::frame::FrameEncoder`。
  - [ ] 暴露 `write_record(&mut self, record: &Record) -> Result<()>`，内部：
    - [ ] `bincode` 序列化为 `payload`；
    - [ ] 计算 `crc32fast::hash(&payload)`；
    - [ ] 写入 `len_be_u32 | payload | crc32_be_u32`。
  - [ ] `finish(self) -> Result<()>`：关闭 `FrameEncoder` 并 flush 底层 writer。
- [ ] 日志：按规范输出 `operation` 首位，关闭时记录写入的条目数与耗时。
- [ ] 在 `tokio::task::spawn_blocking` 中封装对 `RunWriter` 的使用入口，避免阻塞 async runtime。

### 验证步骤
- [ ] 单元测试：写入少量 `Record` 到临时文件（非异步），仅验证文件可创建与 `finish` 正常返回。
- [ ] `cargo clippy` 与 `cargo test` 通过。

---

## 阶段四：RunReader（BufReader + LZ4 Frame + bincode + CRC 校验）

实现同步 I/O 的 run 读取器，逐条返回 `Record`。

### 实现步骤
- [ ] 在 `run_lz4.rs` 增加：
  - [ ] `RunReader`：基于 `std::fs::File` + `std::io::BufReader`（建议 128–256 KiB）+ `lz4_flex::frame::FrameDecoder`。
  - [ ] `read_next(&mut self) -> Result<Option<Record>>`：
    - [ ] 读取大端 u32 长度、读取 payload、读取大端 CRC32；
    - [ ] 计算 CRC32 校验并对比；
    - [ ] 使用 `bincode` 反序列化为 `Record`。
- [ ] 错误处理：CRC 不匹配/EOF/反序列化错误分别返回明确错误信息（包含文件路径与偏移）。

### 验证步骤
- [ ] 单元测试：
  - [ ] Writer 与 Reader 的往返：多条记录 roundtrip 等价。
  - [ ] 破坏 CRC 一字节，Reader 报错且包含上下文。
- [ ] `cargo test` 通过。

---

## 阶段五：与 ParquetChunkWriter 集成（flush 与 finalize）

用 LZ4 run 取代 Parquet run；finalize 阶段进行流式 k 路归并并写最终 Parquet。

### 实现步骤
- [ ] 更新 `src/output/parquet/output.rs`：
  - [ ] 在 `ParquetChunkWriter` 中：
    - [ ] `flush_run_segment`：当 `run_rows` 达到阈值时，使用 `spawn_blocking` + `RunWriter` 将 `BTreeMap` 升序条目逐条写入 `<instance>.<idx:06>.run.lz4`，清空内存并登记到 `candidates`。
    - [ ] `finalize_instance`：
      - [ ] 在 `spawn_blocking` 中执行归并；
      - [ ] 为每个候选构建 `RunReader`，初始化堆（键为 `(db asc, key asc)`）；
      - [ ] 循环弹出最小项，填充 Arrow builders（`cluster`/`instance`/`batch_ts` 常量列），按 8K–16K 批量写 `ArrowWriter`；
      - [ ] `WriterProperties` 设置完整 `sorting_columns = schema::create_db_key_sorting_columns(...)`；
      - [ ] 关闭输出后删除 `.run.lz4`；
      - [ ] 记录耗时、rows/s、MB/s 等日志（`operation` 首位）。
  - [ ] 保留/复用现有 `SortKey` 与 `BTreeMap` 聚合逻辑与命名风格。
- [ ] 保留 `merge_fan_in` 作为 FD 保护上限：若候选数 > 上限，分批归并输出中间 `.run.lz4` 后再归并（内部实现，对外透明）。

### 验证步骤
- [ ] 集成测试：
  - [ ] 设置较小 `run_rows`（如 64/128）强制产生多个 `.run.lz4`，运行到 finalize；
  - [ ] 验证最终 `<instance>.parquet` 存在、`(db,key)` 全局有序，且 `sorting_columns` 完整；
  - [ ] 目录下不再残留 `.run.lz4`；
  - [ ] 日志包含归并输入规模与速率信息。
- [ ] `just test` 通过。

---

## 阶段六：FD 保护与极端规模回归

在大量 run 的情况下，验证不会因 FD 限制或内存问题失败。

### 实现步骤
- [ ] 基于 `merge_fan_in` 或动态探测 `ulimit -n`，限制同时打开的 run 个数；
- [ ] 超出上限时，将超量 run 分批归并为更少的中间 `.run.lz4`，直至可一次性归并为最终 Parquet。
- [ ] 为分批归并路径补充日志与错误上下文。

### 验证步骤
- [ ] 构造 200+ run（将 `run_rows` 设为极小），验证分批归并能够完成且不超出 FD 上限；
- [ ] 最终产物与排序/元数据一致，`.run.lz4` 无残留。

---

## 阶段七：文档与 CLI 帮助同步

### 实现步骤
- [ ] 更新 README.md 与 README.zh_CN：
  - [ ] 目录布局（`.run.lz4` 临时文件）与流程说明；
  - [ ] 参数默认值（`run_rows=64K`、`merge_fan_in` 含义）；
  - [ ] 链接 `lz4_flex` 文档参考：`https://docs.rs/lz4_flex/latest/lz4_flex/`。
- [ ] 确保英文与中文版本同步更新（术语一致）。

### 验证步骤
- [ ] 人工校对两份 README 叙述一致且与 CLI 帮助一致。

---

## 阶段八：日志与可观测性完善

### 实现步骤
- [ ] 确保所有日志调用遵循规范：`operation` 字段首位，描述字符串置尾；
- [ ] run 写入与归并记录关键维度：输入数量、行数、压缩后/前字节、耗时、rows/s、MB/s；
- [ ] 错误路径保留输入文件并输出明确上下文（实例、路径、偏移、原因）。

### 验证步骤
- [ ] 通过测试与手工运行检查日志格式与关键信息是否齐全。

---

## 里程碑与范围说明
- 本次改造将中间 run 替换为 `.run.lz4` 流式格式；finalize 采用流式 k 路归并写最终 Parquet；默认 `run_rows=64K`；内部在 `spawn_blocking` 中以同步 I/O 实现，读端使用 `BufReader`，写端使用 `BufWriter`。
- 不包含：断点续传、历史 run 兼容与恢复；如中断，请删除 `_tmp_batch=...` 目录并重跑。
