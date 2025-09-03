# 开发计划

本计划旨在指导 AI 与人类协作完成开发任务。请遵循以下协作流程：

- **AI**：负责执行【实现步骤】，并在完成后勾选对应的复选框 (`- [x]`)。AI **绝不能**勾选【验证步骤】。
- **人类**：负责审核【实现步骤】的产出，并执行【验证步骤】以确保质量。

---

## 阶段一：引入通用 k 路归并迭代器（基于 Vec<Iterator>，T: Ord）

### 实现步骤

- [x] 新增文件 `src/helper/sort_merge.rs`，实现：
  - [x] `SortMergeIterator<I, T>`：`I: Iterator<Item=T>`, `T: Ord`；内部使用最小堆（通过 `std::cmp::Reverse` 包装）维护 `{ item: T, iter_idx: usize }`，`Ord` 按 `item` 再 `iter_idx`。
  - [x] `SortMergeIteratorResult<I, T, E>`：`I: Iterator<Item=Result<T,E>>`, `T: Ord`；遇到任一来源 `Err(e)` 时，迭代器 `next()` 返回该错误并终止。
  - [x] 为两个结构补充基础单元测试：
    - [x] 多来源合并的顺序正确性（包含相等元素的稳定性）。
    - [x] Result 变体的错误传播行为。
- [x] 在 `src/helper/mod.rs`（或 crate 根模块）中导出公共 API（仅暴露必要类型）。

### 验证步骤

- [x] 运行 `cargo build`，确保编译通过。
- [x] 运行 `cargo test -p rdbinsight -- sort_merge`，测试通过。
- [x] 代码通过 `cargo clippy` 与 `cargo fmt`（若项目使用）。

---

## 阶段二：在报表侧替换 `GroupedMergeIterator` 内部堆为通用归并

### 实现步骤

- [x] 打开 `src/report/parquet.rs`，定位 `GroupedMergeIterator`：
  - [x] 保留对外 `GroupedMergeIterator` 名称与 `Iterator<Item=AnyResult<(Bytes,u64)>>` 行为不变。
  - [x] 将内部 `heap + iters` 实现替换为基于 `SortMergeIteratorResult<DBElementsIterator, (Bytes,u64), anyhow::Error>` 的组合：
    - [x] 将 `Vec<DBElementsIterator>` 传入 `SortMergeIteratorResult::new`，由其维护堆与拉取。
    - [x] `impl Iterator for GroupedMergeIterator` 中直接委托 `next()` 到内部的 `SortMergeIteratorResult`。
- [x] 复用现有单元测试；若有必要，微调以适配实现内细节变更（不改对外行为）。

### 验证步骤

- [x] 运行 `cargo build` 确认编译通过。
- [x] 运行报表相关测试（若存在），确保通过。
- [x] 使用一组小型 Parquet 样本手动运行报表生成，确认结果一致。

---

## 阶段三：在合并阶段引入 `SortableRecord` 与通用归并

### 实现步骤

- [x] 在 `src/output/parquet/merge.rs` 或临近模块新增 `SortableRecord`：
  - [x] 字段：`db: i64`, `key: Vec<u8>`, `record: crate::record::Record`；实现 `Ord/PartialOrd/Eq/PartialEq`，排序规则 `(db asc, key asc)`。
- [x] 为 `run_lz4::RunReader` 提供轻量迭代器适配器 `RunRecordIter`：
  - [x] `impl Iterator<Item=anyhow::Result<SortableRecord>>`，内部调用 `read_next()` 并在 EOF 时返回 `None`。
- [x] 在 `merge.rs` 的 `merge()` 中：
  - [x] 将现有 `BinaryHeap<Reverse<HeapItem>>` 逻辑替换为 `SortMergeIteratorResult<RunRecordIter, SortableRecord, anyhow::Error>`。
  - [x] 消费 `SortableRecord` 时取 `record` 字段，保持批写与统计逻辑不变。
- [x] 单元测试：保留并通过现有 `heap` 行为测试（必要时调整为覆盖 `SortableRecord` 的比较与合并行为）。

### 验证步骤

- [x] 运行 `cargo build` 确认编译通过。
- [x] 运行相关测试；用较小的旧式 run 文件样本进行一次合并，检验排序与输出一致。

---

## 阶段四：实现单文件多 chunk 的 run 写入（writer）

### 实现步骤

- [x] 在 `src/output/parquet/output.rs` 的 `ParquetChunkWriter` 中：
  - [x] 打开并持有单个 run 文件句柄（临时路径，位于 batch temp 目录）。
  - [x] 维护 `Vec<ChunkDesc { offset: u64, length: u64 }>` 的内存索引。
  - [x] 改造 `flush_run_segment()`：
    - [x] 取出排序缓冲的记录，按序编码为 `u32 (BE) + bincode(record)` 连续字节流（移除 CRC）。
    - [x] 将该字节流压缩为单个 LZ4 Frame，并写入文件；记录写入前的文件偏移为 `offset`，压缩后字节数为 `length`，追加到 `Vec<ChunkDesc>`。
  - [x] 在 `finalize_instance()`：
    - [x] 将 `RunIndex { chunks: Vec<ChunkDesc> }` 进行 bincode 编码；对编码结果做 LZ4 压缩；
    - [x] 先写入“压缩后的 index 二进制”，再写入 8 字节（BE）的 `index_len: u64`；
    - [x] 调用合并阶段时仅传入该单个 run 文件路径。
- [x] 保留配置 `max_run_rows` 作为 chunk 粒度控制（按记录条数）。

### 验证步骤

- [x] 使用临时目录运行一次小批量数据写入，检查生成的单一 run 文件大小与尾部 8 字节（BE）是否正确。
- [x] 解析并解压 index，验证 `chunks` 的 offset/length 合法且不重叠、覆盖所有 chunk。
- [x] 运行 `cargo build` 与基础测试。

---

## 阶段五：实现单文件多 chunk 的 run 读取（reader）并接入合并

### 实现步骤

- [x] 在 `src/output/parquet/run_lz4.rs`（或新建模块）实现 `ChunkedRunReader`：
  - [x] 打开文件，读取尾部 8 字节（BE）得到 `index_len`，回读 `index_len` 字节并 LZ4 解压 + bincode 反序列化 `RunIndex`。
  - [x] 为每个 `{offset, length}` 构造限长读取器 + LZ4 Frame 解码器，得到 `ChunkIterator<Item=anyhow::Result<Record>>`：
    - [x] 逐条读取 `u32 (BE)` 长度与 bincode payload；EOF 即结束该 chunk。
- [x] 在 `merge.rs` 中新增 `open_chunk_readers()`：
  - [x] 打开单个 run 文件，创建所有 `ChunkIterator`；
  - [x] 将其包装为 `Iterator<Item=anyhow::Result<SortableRecord>>`（注入 `(db,key)`），并交给 `SortMergeIteratorResult`。
- [x] 根据新流程删除（或保留兼容）旧的“多文件 run 输入”流。

### 验证步骤

- [x] 用阶段四生成的单 run 文件执行合并，确认排序与 Parquet 输出正确。
- [x] 使用包含多个 chunk 的输入验证 k 路归并稳定性与完整性。
- [x] 运行 `cargo build` 与全部测试。

---

## 阶段六：端到端联调与性能回归检查

### 实现步骤

- [ ] 通过 `ParquetOutput` 的全流程生成 run + 合并 Parquet + 生成报表，确保日志与目录结构符合预期。
- [ ] 观察 run 文件数量（应只有 1 个）与总大小；确认 `max_run_rows` 生效。
- [ ] 评估内存占用（一次性为所有 chunk 建立游标），确认在典型数据规模下可接受。

### 验证步骤

- [ ] 对比新旧实现生成的 Parquet 排序与统计结果一致（允许元数据不同）。
- [ ] 对大数据样本进行一次端到端跑通，记录时间与资源占用，与旧实现对比回归不劣化或可接受。

---

## 阶段七：清理与文档

### 实现步骤

- [ ] 移除未使用的旧式多 run 文件写入路径与工具函数（如不再需要）。
- [ ] 更新 README/内部文档：描述新 run 文件结构（多 chunk + 尾部 index），说明为临时产物、整数均为大端序，校验依赖 LZ4。
- [ ] 为关键模块补充注释，解释 `SortableRecord`、`SortMergeIterator*` 的稳定性与边界行为。

### 验证步骤

- [ ] 最终 `cargo build`、`cargo test` 全量通过。
- [ ] 代码扫描与静态检查通过；无未引用代码与死代码告警。
