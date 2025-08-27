# 背景与思路

当前报告的“前缀 ≥1%（用于火焰图）”依赖数据按键有序，以便在低内存下通过局部范围的最短公共前缀（LCP）快速收敛。ClickHouse 方案借助 MergeTree 的后台排序达成此目标；而 Parquet 输出缺乏排序能力，直接在未排序 Parquet 上计算显著前缀会导致高内存与高 CPU 成本。

为在仅有 Parquet 的环境下也能稳定生成报告，本次开发仅聚焦于“实例粒度（per-instance）的外部归并排序”，并将其集成到 dump 流程中：
- 对每个实例在 dump 过程中生成排序 Run 并立即落盘；
- 在实例级完成后进行实例内 k 路归并，得到“按 (db, key) 有序”的最终文件；
- 所有实例完成后，将批次级临时目录整体重命名为最终分区目录；
- 并发度直接复用 `DumpArgs.concurrency`，排序与 dump 共享协程，避免额外 CPU 竞争。

注：本次范围不包含“从 Parquet 生成报告”的具体实现，该部分将作为后续迭代处理。


# 关键设计

## 1. 总体流程与组件（集成到 dump）
- ParquetSorter（新，内嵌于 Parquet 输出路径）
  - 作用：在 dump 流水线内对每个实例执行外部归并排序，输出“实例内按 (db, key) 有序”的 Parquet。
  - 阶段：
    1) Run 生成（块级排序）：dump 产生的记录按实例聚合到内存块（按行数阈值），达到阈值后在内存中按 (db, key) 排序，写出临时有序分段文件（`<instance>.000N.parquet`）。
    2) 实例内 k 路归并：当该实例的 dump 结束时，对该实例的所有分段文件以 (db, key) 做 k 路归并，合并为单一文件 `<instance>.parquet` 并清理分段。
  - 并发：直接使用 `DumpArgs.concurrency` 控制实例级并发；排序与 dump 共享协程（同一 Tokio runtime 中调度），避免另起额外并行度导致 CPU 争用。

- 批次最终化（Batch Finalize）
  - 当且仅当本批次所有实例文件均已生成 `<instance>.parquet` 后，执行一次性目录改名，将临时批次目录重命名为最终分区目录，实现“全有或全无”的可见性。

## 2. 数据与文件布局（Hadoop 分区风格 + 批次级临时目录）
- 最终根目录：`<output_root>/cluster=<cluster>/batch=<batch>/`
- 临时根目录（本批次）：`<output_root>/cluster=<cluster>/_tmp_batch=<batch>/`
  - Run 分段文件：`<instance>.0001.parquet`、`<instance>.0002.parquet`、...
  - 实例内归并的单一文件：`<instance>.parquet`（完成后清理分段文件）。
- 压缩策略：Run 分段默认使用 lz4（可配为 `intermediate-compression`），最终 `<instance>.parquet` 默认使用 zstd（可配为 `compression`）。
- 最终化（Finalize）：
  - `rename(<output_root>/cluster=<cluster>/_tmp_batch=<batch>/, <output_root>/cluster=<cluster>/batch=<batch>/)`
  - 失败时保留 `_tmp_batch=...` 目录便于排查与整体重试。

## 3. 排序键说明（(db, key)）
- 排序键定义：按 (db, key) 升序排序。
- 动机：先按 `db` 分组，再在组内按 `key` 排序，利于 DB 维度的局部性与流式聚合；也更接近 ClickHouse 视图的使用方式。

## 4. 关键算法与内存模型
- Run 生成（per-instance）：
  - 顺序接收 dump 流中的记录，累积至“行数上限”（见下文“运行规模与并发”），在内存中按 (db, key) 排序，写出一个 Run；重复直至实例结束；
  - 内存峰值 ≈ 块大小 × 平均行宽 + 排序开销系数；由阈值控制。

- 实例内 k 路归并：
  - 对该实例的所有 Runs 以最小堆按 (db, key) 归并输出到 `<instance>.parquet`；
  - 完成后清理该实例的 Run 分段文件；
  - 始终产出单个最终文件。

## 5. 运行规模与并发
- 并发来源：使用 `DumpArgs.concurrency` 控制实例级并发，排序与 dump 共用同一协程模型（Tokio），避免过度并发导致 CPU 争用。
- Run 尺寸控制：
  - 优先级：理论上更倾向 `run-bytes` 限制，但缺乏可靠、低开销的实时内存占用感知；
  - MVP：采用 `run-rows` 上限控制，默认安全值为 100,000 行；
  - 后续改进（备选）：
    - 轻量估算每行近似字节（key 长度 + 常数因子）以形成近似 `run-bytes` 的软上限；
    - 尝试全局自定义分配器（global allocator）以统计分配量，作为“硬上限”触发 Run 切分；实现复杂且需评估性能影响，暂不纳入本次范围。

## 6. 失败与可观测性
- 断点续传：本次不支持；若中断，可保留 `_tmp_batch=...` 目录并整体清理后重跑该批次。
- 可观测性：
  - 指标：输入行数与字节（可选）、Run 数、实例内归并耗时、每层耗时、内存峰值估算、临时目录峰值；
  - 日志：阶段（run_gen/merge/finalize）、参数、进度（比例）与速率（MB/s、rows/s）、并发实例列表；
  - 资源控制：`DumpArgs.concurrency`、`run-rows`、`compression`（最终，默认 zstd）、`intermediate-compression`（Run，默认 lz4）。

## 后续迭代（非本次范围）
- 报告生成：
  - 基于已排序的 Parquet，使用跨实例 k 路归并迭代器获得全局有序视图，执行显著前缀（≥1%）与其他聚合；
  - 提供 `report from-parquet` 命令；
  - 可能的优化：利用目录分区与列统计进行剪枝、预读与向量化、并发读写优化等。
- 更精细的 `run-bytes` 控制与内存探测（global allocator 或 Arrow/Parquet 层的精确内存统计）。


# 实施建议

- 阶段一：实例级排序（MVP）
  - 在 Parquet 输出路径中集成 ParquetSorter：Run 生成 + 实例内 k 路归并；
  - 产出 `<output_root>/cluster=<cluster>/_tmp_batch=<batch>/<instance>.000N.parquet` 分段文件，并在临时目录内合并为单一 `<instance>.parquet`；
  - 参数：复用 `DumpArgs.concurrency`，提供 `run-rows`（默认 100000）、`compression`（最终，默认 zstd）、`intermediate-compression`（Run，默认 lz4）；
  - 指标与日志；小规模端到端测试验证实例内有序与数据一致性。

- 阶段二：批次最终化
  - 执行批次目录一次性重命名为 `batch=<batch>`；
  - 补充完整性校验（是否所有实例均产出单一文件、是否存在残留分段文件等）。

- 阶段三：优化与稳健性
  - 大量 Run 的分层归并策略、预读与向量化优化、并发读写；
  - 探索 `run-bytes` 软/硬上限的可行实现（估算或 allocator 路线），在压测后再启用。


# 建议标题

```text
feat(parquet): integrate per-instance external merge sort into dump with batch-level finalize
```
