## 背景与思路

- 现状：RDB 解析后以 `(db asc, key asc)` 排序，每达到阈值（`max_run_rows`）落盘为一个 LZ4 流式 run 文件，最终在 `merge.rs` 进行多路归并，写出单个 Parquet。大实例会产生大量 run 文件，增加元数据与 IO 压力。
- 目标：将“多 run 文件”优化为“单 run 文件多 chunk”。每个 chunk 内部记录已经排序；最终归并阶段在同一个 run 文件内对所有 chunk 建立游标并执行 k 路归并，生成 Parquet。
- 关键约束（已确认）：
  - **校验**：依赖 LZ4 自带校验，不再做每条记录 CRC。
  - **字节序**：所有整数均使用**大端序**。
  - **索引信息**：`RunIndex` 仅包含每个 chunk 的 `{offset: u64, length: u64}`；无需 first/last key。
  - **chunk 粒度**：沿用现有 `max_run_rows`（按记录条数）控制单个 chunk 的大小。
  - **归并游标**：一次性为所有 chunk 建立游标进行归并，典型场景内存可接受。
  - **兼容性**：run 文件为临时产物，无需 magic 与版本号。

## 关键设计

- **统一 run 文件整体结构**（单文件，多 chunk）：

  1. 文件主体：顺序排列的若干个 chunk。每个 chunk 为独立的 LZ4 压缩块（建议使用 LZ4 Frame），其解压数据为若干条连续记录；chunk 之间紧密相连。
  2. 文件尾部：`RunIndex` 压缩后二进制（先写入 LZ4 压缩的 bincode 产物），紧接着最后 8 字节为该“压缩后 index 的长度”（`u64`，大端）。

- **记录与 chunk 内部格式**：

  - 记录序列：每条记录 = `len: u32 (BE)` + `payload`，其中 `payload` 为使用 `bincode::serde` 编解码的 `Record` 二进制；无 CRC。
  - chunk：将上述记录序列整体进行 LZ4 压缩后写入。建议采用 LZ4 Frame（便于解码器与校验/边界控制）。

- **RunIndex（只存索引，不含键范围）**：

  - bincode 编码的结构形态（描述性，不强制具体代码）：
    - `chunks: Vec<{ offset: u64, length: u64 }>`
  - 写入顺序：`lz4_compressed(bincode(RunIndex))` + `index_len_be_u64`（大端）。

- **写入流程（统一 run writer）**：

  - 缓冲与排序：沿用 `BTreeMap<SortKey, Record>` 方式累积记录以保证 `(db, key)` 有序。
  - 触发 flush（当条数达到 `max_run_rows` 或 finalize）：
    1. 迭代已排序记录，按“`u32(BE)` 长度 + `bincode` 的 `Record`”拼接为未压缩字节流。
    2. 将该字节流压缩为一个独立的 LZ4 Frame，得到 `chunk_bytes`。
    3. 记录当前文件写指针为 `offset`，写入 `chunk_bytes`，并记录 `{offset, length: chunk_bytes.len()}` 到内存中的 `RunIndex` 构建向量。
    4. 清空缓冲。
  - 完成所有 chunk 写入后：
    1. 对 `RunIndex` 进行 bincode 编码；
    2. 对编码结果再做 LZ4 压缩；
    3. 依次写入“压缩后的 index 二进制”和“8 字节（BE）的 index 长度”。

- **读取与归并流程（统一 run reader + 合并）**：

  - 打开 run 文件：读取末尾 8 字节（BE）获得 `index_len`，`seek` 回到 `file_len - 8 - index_len` 读取压缩的 index 区；LZ4 解压、bincode 解析，得到 `chunks` 列表。
  - 为每个 chunk 创建一个游标：
    - 通过 `File::try_clone + seek(offset) + take(length)` 或等效的“限长读取器”包一层 `BufReader`，供 LZ4 Frame 解码器消费；
    - 游标提供 `read_next()`：读取 `u32(BE)` 长度、再读 payload、bincode 反序列化为 `Record`，直至该 chunk 结束（游标 EOF）。
  - k 路归并：
    - 为每个游标读取一条“当前记录”，以 `(db, key)` 构建堆项，使用二叉堆（最小堆）进行多路归并；
    - 弹出最小项 -> 写入 Parquet 批缓冲 -> 从相应游标补充下一条；
    - 批缓冲满时（沿用现有 `BATCH_CAPACITY`）调用 `records_to_columns` + `ArrowWriter::write`；尾部 flush；
    - 同步维护统计信息（沿用 `RecordsSummary`）。

- **与现有模块的集成与职责边界**：

  - `ParquetChunkWriter`：
    - 改为维护“单 run 文件句柄 + 运行中 index 向量 + 排序缓冲”。
    - `flush_run_segment()`：不再创建单独 run 文件，而是追加一个 chunk 到该 run 文件，并将 `{offset, length}` 追加到本次 writer 的 index 向量。
    - `finalize_instance()`：落盘 index 与长度，随后调用合并阶段（只传入这一个 run 文件路径）。
  - `merge.rs`：
    - `open_run_readers()` 替换为基于“统一 run reader”的 `open_chunk_readers()`：
      - 输入：单个 run 文件路径；
      - 输出：`Vec<ChunkReader>` 与 `Vec<Option<Record>>`（每个 chunk 的首记录）。
    - 主循环保持既有堆归并与批写入逻辑不变，只是维度从“多个文件”变为“单文件内多个 chunk”。
  - `run_lz4.rs`：
    - 新增“统一 run writer/reader”能力（可与旧实现并存，便于重构渐进），或将该模块演进为仅支持新格式。

- **错误处理与鲁棒性**：

  - 若在写入 index 前进程中断，文件尾部缺少合法 index 长度与 index 区，reader 应判定为不完整文件；由于 run 文件仅为临时产物，可直接丢弃。
  - 依赖 LZ4 frame 的内置校验确保 chunk 数据块损坏能被检测。
  - 所有偏移/长度均为 `u64`（BE），避免超大文件的整型溢出。

- **性能与资源**：
  - chunk 粒度由 `max_run_rows` 控制，保持与现状一致，避免行为突变；
  - 归并阶段一次性为所有 chunk 建立游标，内存开销线性于 chunk 数与解码器缓冲，满足“典型场景可接受”的约束；
  - LZ4 压缩在写端以 chunk 维度进行，减少小文件管理开销、提升顺序写效率。

## 关键设计（补充）：通用 k 路归并迭代器（基于 Vec<Iterator>）

- **目标**：提供通用的“多来源最小堆归并”，输入为 `Vec<Iterator<Item = T>>`，输出也为 `Iterator<Item = T>`，其中 `T: Ord`。不额外存储独立 key，不新增 Trait。

- **核心思路**：

  - 维护一个最小堆，堆元素包含：`{ item: T, iter_idx: usize }`；其 `Ord` 比较首先按 `item`，若相等再按 `iter_idx` 提供稳定次序。
  - 初始化：从每个 iterator 拉取一条（若有）并入堆。
  - 每次 `next()`：弹出最小项，产出其中的 `item`，随后从对应的 `iter_idx` 再拉取一条并入堆。

- **API（伪签名）**：

  - 纯值型：
    - `struct SortMergeIterator<I, T>` where `I: Iterator<Item = T>`, `T: Ord`。
    - `fn new(iters: Vec<I>) -> Self`。
    - `impl Iterator for SortMergeIterator { type Item = T; fn next(&mut self) -> Option<T>; }`。
  - 错误透传型（来源迭代器产出 `Result<T, E>`）：
    - `struct SortMergeIteratorResult<I, T, E>` where `I: Iterator<Item = Result<T, E>>`, `T: Ord`。
    - `fn new(iters: Vec<I>) -> Self`。
    - `impl Iterator for SortMergeIteratorResult { type Item = Result<T, E>; fn next(&mut self) -> Option<Result<T, E>>; }`。
    - 行为：当任一来源 `next()` 返回 `Err(e)` 时，立即返回该错误并终止归并（与现有错误传播保持一致）。

- **复杂度与资源**：

  - 与传统 k 路归并相同：总 `O(n log m)`；额外内存 `O(m)`。

- **与现有模块的对接**：

  - `output/parquet/merge.rs`：
    - 需要让来源迭代器的 `Item` 自身具备 `Ord`。建议引入轻量包装类型 `SortableRecord { db: i64, key: Vec<u8>, record: Record }`，其 `Ord` 按 `(db, key)` 升序；`Iterator` 产出 `Result<SortableRecord, anyhow::Error>`。
    - 将 `RunReader` 适配为 `Iterator<Item = AnyResult<SortableRecord>>`（内部拉取 `Record` 并转包为 `SortableRecord`）。
    - 使用 `SortMergeIteratorResult` 做归并；消费端取到 `SortableRecord` 后访问 `record` 字段即可复用批写与统计逻辑。
  - `report/parquet.rs::GroupedMergeIterator`：
    - 现有 `DBElementsIterator` 产出 `AnyResult<(Bytes, u64)>`，其中 `(Bytes, u64)` 自带字典序 `Ord`；
    - 可直接使用 `SortMergeIteratorResult<DBElementsIterator, (Bytes, u64), anyhow::Error>` 替换自建堆；对外行为保持不变。

- **确定性**：

  - 当 `item` 完全相等时，用 `iter_idx` 作为稳定次序，保证结果确定性。

- **边界条件**：
  - 空来源忽略；全部为空时直接 `None`；遇到错误即返回该错误并终止。

## 实施建议

- **步骤 1：定义统一 run 文件索引结构**

  - 在 `output/parquet/run_lz4.rs`（或新建 `run_file.rs`）定义 `RunIndex` 的 bincode 结构（向量形式的 `{offset: u64, length: u64}`）。
  - 明确末尾长度字段为 `u64 (BE)`，并约定 index 区域为 LZ4 压缩的 bincode 结果。

- **步骤 2：实现统一 run writer**

  - 在 `ParquetChunkWriter` 内部持有“单 run 文件”的写句柄与 `Vec<ChunkDesc>` 的索引向量；
  - 改造 `flush_run_segment()`：
    - 将当前排序缓冲转为记录流（`u32(BE)` + `bincode(Record)`），压缩为 LZ4 frame，写入文件；
    - 记录 `{offset, length}`；清空缓冲；
  - 在 `finalize_instance()`：写入 `RunIndex`（bincode -> LZ4），再写入 8 字节 BE 的长度；然后调用合并阶段。

- **步骤 3：实现统一 run reader 与 chunk 游标**

  - 读取尾部 8 字节获得 index 长度，回读并解压 index；
  - 基于 index 为每个 chunk 生成限长读取器 + LZ4 FrameDecoder 的 `ChunkReader`；
  - 提供 `read_next()` 返回 `Option<Record>`；

- **步骤 4：改造合并阶段**

  - 在 `merge.rs` 增加 `open_chunk_readers()`，返回 `Vec<ChunkReader>` 与首记录列表；
  - 维持现有二叉堆归并、批写 Parquet 与统计逻辑；

- **步骤 5：迁移与清理**
  - `ParquetChunkWriter` 不再产生多 run 文件，仅产生一个统一 run 文件；
  - `merge.rs` 的入口使用统一 run 文件路径；
  - 保留旧实现的最小兼容（可选），或直接切换到新格式（更简洁）。

## 实施建议（补充）

- 在 `helper` 模块下新增 `sort_merge.rs`，实现上述 `SortMergeIterator` 与 `SortMergeIteratorResult`。
- 在 `merge.rs` 中：
  - 增加 `SortableRecord` 包装（或同等效果的 `Ord` 封装）；为 `RunReader` 提供 `Iterator<Item = AnyResult<SortableRecord>>` 的适配层；
  - 用 `SortMergeIteratorResult` 替换现有堆与循环，保留批写入与统计不变。
- 在 `report/parquet.rs` 中：
  - 用 `SortMergeIteratorResult` 替换 `GroupedMergeIterator` 的内部堆逻辑，导出统一的合并实现。

## 建议标题（更新）

```text
feat(parquet): unify chunked LZ4 run file and generic k-way merge over Ord items
```
