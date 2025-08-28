# 背景与思路

当前报告的“前缀 ≥1%（用于火焰图）”依赖数据按键有序，以便在低内存下通过局部范围的最短公共前缀（LCP）快速收敛。ClickHouse 方案借助 MergeTree 的后台排序达成此目标；而 Parquet 输出缺乏排序能力，直接在未排序 Parquet 上计算显著前缀会导致高内存与高 CPU 成本。

为在仅有 Parquet 的环境下也能稳定生成报告，整体方案聚焦于“实例粒度（per-instance）的外部归并排序”，并集成到 dump 流程中：
- 对每个实例在 dump 过程中生成排序 Run 并立即落盘；
- 在实例级完成后进行实例内归并，得到“按 (db, key) 有序”的最终文件；
- 所有实例完成后，将批次级临时目录整体重命名为最终分区目录；
- 并发度直接复用 `DumpArgs.concurrency`，排序与 dump 共享协程，避免额外 CPU 竞争。

在实践中，当实例存在大量 Run（如 300+，每 run 10 万行），并发多实例同时归并时，内存会因“每个 run 同时常驻一个解码后的当前批次”而快速膨胀。为显著降低峰值内存，本次在既有方案上引入“分层归并（Hierarchical Merge，限制 fan-in）”，并配合两项 P0 内存优化（较小的读取批次、及时释放已完成 run 的当前批次）。

注：本次范围仍不包含“从 Parquet 生成报告”的具体实现，该部分将作为后续迭代处理。


# 关键设计

## 1. 总体流程与组件（集成到 dump）
- ParquetSorter（内嵌于 Parquet 输出路径）
  - 作用：在 dump 流水线内对每个实例执行外部归并排序，输出“实例内按 (db, key) 有序”的 Parquet。
  - 阶段：
    1) Run 生成（块级排序）：dump 产生的记录按实例聚合到内存块（按行数阈值），达到阈值后在内存中按 (db, key) 排序，写出临时有序分段文件（`<instance>.000N.parquet`）。
    2) 实例内分层归并（限制 fan-in，滚动合并）：当该实例的 dump 结束时，若 run 数量 `k > F`（最大 fan-in），采用“最小 idx 优先、滚动 F 路合并”的方式逐步归并，直到仅剩一个文件。
  - 并发：继续使用 `DumpArgs.concurrency` 控制实例级并发；同一实例内的合并过程串行执行，不额外引入新的并行度。

- 批次最终化（Batch Finalize）
  - 当且仅当本批次所有实例文件均已生成 `<instance>.parquet` 后，执行一次性目录改名，将临时批次目录重命名为最终分区目录，实现“全有或全无”的可见性。

## 2. 数据与文件布局（Hadoop 分区风格 + 批次级临时目录）
- 最终根目录：`<output_root>/cluster=<cluster>/batch=<batch>/`
- 临时根目录（本批次）：`<output_root>/cluster=<cluster>/_tmp_batch=<batch>/`
  - Run 分段文件（初始与滚动产物）：`<instance>.<idx:06>.run`（内容为 Parquet，`idx` 为零填充 6 位的单调递增整数，由发号器分配）。
  - 实例最终文件：`<instance>.parquet`（当仅剩 1 个候选文件时直接写入/保留为最终文件）。
- 压缩策略：
  - Run 分段与滚动合并产物（`.run`）使用 `intermediate-compression`（默认 lz4），追求低 CPU 与快速周转；
  - 最终 `<instance>.parquet` 使用 `compression`（默认 zstd）。
- 最终化（Finalize）：
  - `rename(<output_root>/cluster=<cluster>/_tmp_batch=<batch>/, <output_root>/cluster=<cluster>/batch=<batch>/)`；
  - 失败时保留 `_tmp_batch=...` 目录便于排查与整体重试。

## 3. 排序键与元数据（按 schema::create_db_key_sorting_columns）
- 排序键定义：遵循 `schema::create_db_key_sorting_columns` 的列顺序（例如 `cluster`、`batch`、`instance`、`db`、`key` 全部升序）。即便在单个实例文件内前三列完全一致，也仍将其包含在 `sorting_columns` 中以保持一致性与可读性。
- 元数据：所有 Run 分段与滚动产物（`.run`，Parquet 内容）以及最终文件（`.parquet`）的 Parquet `sorting_columns` 均设置为上述完整列集合。

### 职责与接口（Orchestration 与 MergeContext）
- ParquetChunkWriter（编排者）：
  - 维护“发号器（idx issuer）”，单调递增，从 0 开始；
  - dump 阶段：每次 flush Run 分段时消耗一个 idx，统一生成 `<instance>.<idx:06>.run`（Parquet 内容，方便 ls 区分）；
  - finalize 阶段（滚动合并编排）：反复从候选集合中取最小 F 个输入，向发号器申请 `next_idx`，计算输出路径 `<instance>.<next_idx:06>.run`（或最终 `<instance>.parquet`），并调用 MergeContext 执行一次 F 路归并；MergeContext 成功返回后，将新输出加入候选；直到仅剩 1 个文件时输出为 `<instance>.parquet` 并结束。
- MergeContext（执行者）：
  - 输入：`inputs: Vec<PathBuf>`（`.run` 文件，Parquet 内容）、`output: PathBuf`（`.run` 或 `.parquet`）、压缩/批量/排序元数据等不变配置；
  - 行为：以最小堆进行 k 路归并，将输入按 `(cluster,batch,instance,db,key)` 升序合并写入 `output`；
  - 清理：在合并并成功关闭 `output` 后，删除本次合并所消耗的所有 `inputs`；如合并失败不删除，便于排查（本方案不做断点续传）。

## 4. 关键算法、内存模型与 P0 优化
- 分层归并（限制 fan-in，滚动 F 路合并）
  - 候选文件发现：
    - 初始候选由当前运行过程中 `ParquetChunkWriter` 刷新 Run 时产生，统一命名为 `<instance>.<idx:06>.run`（`idx` 来自发号器，单调递增，内容为 Parquet）；
    - 不进行目录扫描；不考虑历史残留与断点续传场景。
  - 滚动选择与产出：
    - 记当前候选集合的最小 F 个文件为集合 S（按 idx 升序取前 F 个）。
    - 若候选数 `<= F`：这是最后一次合并，合并全部候选到 `<instance>.parquet`；
    - 否则（候选数 `> F`）：
      - 由发号器申请 `next_idx = issuer.next()`；
      - 使用 S 作为输入，执行一次 F 路归并，直接写入 `<instance>.<next_idx:06>.run`；
      - MergeContext 成功返回后，将新文件加入候选集合，继续循环。
  - 终止条件与合并次数：
    - 每次合并用 F 个文件生成 1 个新文件，候选数量减少 `F-1`；
    - 合并总次数约为 `ceil((k - 1) / (F - 1))`（k 为初始候选数）。
  - 内存上界：
    - 任一时刻仅有 F 个 `RunCursor` 活跃，常驻内存从 O(k) 降至 O(F)。
- P0 优化 1：较小的读取批次
  - 为每个 `RunCursor` 的 `ParquetRecordBatchReader` 设置较小的 `batch_rows`（建议默认 1024~2048），减少“当前 RecordBatch”的内存占用。
  - 若底层不支持精确控制，可在写 Run 分段时采用较小的 row group 或输出批次作为替代策略。
- P0 优化 2：及时释放已完成 run 的当前批次
  - 当某个 `RunCursor` 读到结尾，应立即清空其 `current RecordBatch` 并让 reader 尽快可被回收，避免完成的 runs 尾部缓冲常驻到全局结束。
- 输出端批构建器（OutputBuilders）
  - 可将单批输出容量从 64K 行下调至 8K~16K（可配），进一步回收常驻 builder 内存（次要）。

### 滚动合并的命名与清理
- 命名：
  - 统一采用 `<instance>.<idx:06>.run`（idx 来自发号器，零填充 6 位，Parquet 内容），便于 ls 区分与排序；
  - 最后仅保留 `<instance>.parquet` 作为实例最终文件。
- 清理策略：
  - 每次合并所消耗的输入文件删除由 MergeContext 在合并成功后执行；
  - 整个批次均在 `_tmp_batch=` 目录中；不支持断点续传，如中断请删除该目录后重跑。

### 伪代码
- 初始化：
  - `issuer = 0`
  - dump 阶段每次 flush：
    - `out = format("<inst>.{idx:06}.run", idx = issuer.next())`
    - `write_run(out)`；`candidates.add(out)`
- finalize 循环：
  - `if candidates.len() <= F: merge_to("<inst>.parquet", candidates) // MergeContext merges and deletes inputs; break`
  - `S = take_first_F(candidates)`
  - `out = format("<inst>.{idx:06}.run", idx = issuer.next())`
  - `merge_to(out, S) // MergeContext merges and deletes inputs`
  - `candidates = (candidates \ S) ∪ {out}`；继续循环

## 5. 运行规模、并发与复杂度
- 并发：
  - 实例级并发仍由 `DumpArgs.concurrency` 控制；同一实例内部的滚动合并过程串行，不新增并行度。
- 复杂度与 IO：
  - 合并次数 `~ ceil((k - 1) / (F - 1))`；总 IO 相比单轮合并略增，但换取稳定内存峰值与实现简洁。
- 内存上界（近似）：
  - 峰值 ≈ F × reader_batch_rows × 平均行宽 + 输出构建器开销；F、reader_batch_rows 可调，便于权衡。

## 6. 失败、清理与可观测性
- 不支持断点续传；如执行中断，请删除 `_tmp_batch=...` 目录并重新执行该批次。
- 清理：每次成功合并后立即删除输入段，保持磁盘占用受控；最终仅保留 `<instance>.parquet`。
- 指标与日志：
  - 指标：每次合并的输入数量、输出大小、耗时、累计合并次数、当前候选数；
  - 日志：显示 `fan_in=F`、本轮选择的最小/最大 idx、next_idx、rows/s、MB/s；最终化时长。

## 7. 兼容性
- 当 `k <= F` 时，行为与现有单轮 k 路归并一致；
- 所有产物保持 `(cluster,batch,instance,db,key)` 升序与 `sorting_columns` 元数据；
- 不改变最终可见目录与文件命名（仅中间产物在 `_tmp_batch=` 下可见，完成后均被清理）。

## 后续迭代（非本次范围）
- 报告生成：
  - 基于已排序的 Parquet，使用跨实例 k 路归并迭代器获得全局有序视图，执行显著前缀（≥1%）与其他聚合；
  - 提供 `report from-parquet` 命令；
  - 可能的优化：利用目录分区与列统计进行剪枝、预读与向量化、并发读写优化等。
- 更精细的 `run-bytes` 控制与内存探测（global allocator 或 Arrow/Parquet 层的精确内存统计）。


# 实施建议

- 阶段二：实例内分层归并（限制 fan-in，滚动 F 路合并）
  - 引入 `merge_fan_in=F`（默认 64），并按“最小 idx 优先、滚动合并”算法实现；
  - 所有中间产物与最终文件均设置完整的 `sorting_columns`；
  - 合并完成后清理输入段，最终仅保留 `<instance>.parquet`。

- 阶段三：参数与文档
  - 新增参数：
    - `merge_fan_in`：最大 fan-in，默认 64；
    - `reader_batch_rows`：单 run 读取批次行数上限，默认 1024 或 2048；
    - `merge_output_batch_rows`：合并输出批次行数上限，默认 16K（可选）。
  - CLI 与配置文档补充默认值、取值范围、内存/IO 的权衡说明；日志补充 idx 选择与合并迭代信息。


# 建议标题

```text
feat(parquet): adopt rolling F-way merge (min-index first) to cap memory and simplify implementation
```
