# 开发计划

本计划旨在指导 AI 与人类协作完成开发任务。请遵循以下协作流程：

*   **AI**：负责执行【实现步骤】，并在完成后勾选对应的复选框 (`- [x]`)。AI **绝不能**勾选【验证步骤】。
*   **人类**：负责审核【实现步骤】的产出，并执行【验证步骤】以确保质量。

---

## 阶段一：配置与 CLI 接入（最小可用参数）

本阶段仅做必要参数与默认值的接入，不引入无关配置；并发复用 `DumpArgs.concurrency`。

### 实现步骤
- [ ] 为 Parquet 输出增加运行规模控制参数：`run_rows`（默认 100000，测试可下调以强制生成多 Run）。
- [ ] 为最终文件增加压缩参数：`compression`（默认 zstd）。
- [ ] 为 Run 分段增加压缩参数：`intermediate_compression`（默认 lz4）。
- [ ] 在现有 CLI 中为 into-parquet 分支接入上述参数（名称与文档与设计一致）。
- [ ] 确认并复用 `DumpArgs.concurrency` 作为实例级并发度来源（排序与 dump 共享协程）。

### 验证步骤
- [ ] 编译与 Lint：`cargo clippy --all-targets --all-features -- -D warnings` 通过。
- [ ] 运行：`cargo run --bin rdbinsight -- dump ... into-parquet --help` 能正确显示新参数与默认值。
- [ ] 配置反序列化与默认值单测通过（新增/调整 tests）。

---

## 阶段二：路径布局与批次级临时目录

引入 Hadoop 分区风格目录与批次级临时目录，最终以目录级 rename 完成“全有或全无”。

### 实现步骤
- [ ] 更新 `output/parquet/path.rs`：
  - [ ] 临时根：`<output_root>/cluster=<cluster>/_tmp_batch=<batch>/`
  - [ ] 最终根：`<output_root>/cluster=<cluster>/batch=<batch>/`
  - [ ] 规范实例分段 Run 文件命名：`<instance>.0001.parquet`、`<instance>.0002.parquet`...
  - [ ] 规范实例最终文件命名：`<instance>.parquet`。
  - [ ] 提供批次“最终化”函数：将 `_tmp_batch=<batch>` 一次性 rename 为 `batch=<batch>`。
- [ ] 日志中输出新路径与关键操作（创建临时目录、最终化）。

### 验证步骤
- [ ] 编译与 Lint 通过。
- [ ] 新增/调整针对路径格式的单测：确保目录与文件路径符合预期（特别是最终化逻辑）。

---

## 阶段三：Run 生成（增量有序缓冲 + 分段落盘）

将 dump 输出在内存聚合到“行数上限”的 Run，采用 BTree 增量有序缓冲摊平排序成本，按 (db, key) 顺序写出分段文件（使用 `intermediate_compression`）。

### 实现步骤
- [ ] 在 Parquet 输出模块中新增“Run 缓冲器”（按实例维护）：
  - [ ] 使用 `BTreeMap<(db, key), Record>` 作为增量有序结构（以 O(log n) 插入；基于 RDB 语义假设不会产生重复 (db, key)）。
  - [ ] 接收记录（沿用现有 Record/Arrow 路径），每条插入 BTree；达到 `run_rows` 上限后触发落盘；
  - [ ] 落盘时按 BTree 的 in-order 迭代 key，输出为 `<instance>.000N.parquet`（使用 `intermediate_compression`）。
  - [ ] 设置 WriterProperties.sorting_columns = [(db ASC), (key ASC)]，在分段文件的 Parquet 元数据中标明排序键。
  - [ ] 清空缓冲并递增 N。
- [ ] 实例结束时，如有未满 Run 的尾块，同样按有序迭代落盘，并设置相同的 sorting_columns 元数据。
- [ ] 统一 tracing：记录每个 Run 的行数/耗时/输出大小/压缩方式，并输出插入/落盘的速率以观察是否存在 CPU 尖刺。

### 验证步骤
- [ ] 将 `run_rows` 在测试中下调为很小值（如 10~50），强制形成 ≥2 个 Run，检查分段文件存在与行数正确。
- [ ] 读取分段 Parquet，验证 (db, key) 局部有序（跨 Run 的全局有序在下一阶段验证）。
- [ ] 检查分段文件 Parquet 元数据包含 sorting_columns 且为 (db, key) 升序。

---

## 阶段四：实例内 k 路归并（合并为单一文件）

将该实例的所有分段文件按 (db, key) 做 k 路归并，生成单一 `<instance>.parquet`（使用最终 `compression`）。

### 实现步骤
- [ ] 实现基于最小堆的多路归并迭代器（或“锦标赛树” selection tree）：
  - [ ] 数据结构：
    - [ ] 每个 Run 的“批游标”（BatchCursor）：持有当前 RecordBatch、当前行索引、源文件句柄；
    - [ ] 最小堆（`BinaryHeap` with `Reverse` 或自建 tournament tree），键为 `(db, key)` 与来源 run_id；
    - [ ] 输出批构建器：按列的 Arrow Array Builders，达到目标行数后形成 RecordBatch 写出。
  - [ ] 算法：
    - [ ] 初始化：从每个 Run 读取首个 RecordBatch，将首行放入堆，携带 run_id 与行位置；
    - [ ] 循环：弹出堆顶（最小 `(db, key)`），将该行追加到输出构建器；然后推动对应 Run 的游标到下一行；若当前批用尽，则拉取该 Run 的下一 RecordBatch；若该 Run 结束，从堆中移除；
    - [ ] Flush：输出构建器达到阈值（例如 64K 行）或所有 Run 结束时，生成 RecordBatch 写入 AsyncArrowWriter；
    - [ ] 终止：所有 Run 耗尽。
  - [ ] Writer：
    - [ ] 使用最终 `compression` 写出 `<instance>.parquet`；
    - [ ] 设置 WriterProperties.sorting_columns = [(db ASC), (key ASC)]；
    - [ ] 可选设置目标 Row Group 行数/估算大小（如 128MB），以平衡读取性能与元数据体量。
  - [ ] 资源与性能控制：
    - [ ] Fan-in 限制：当 Run 数极大时（例如 >128），分批归并（多轮）：先将若干 Runs 合并为少量临时“大 Run”（仍写入临时目录），最终一轮合并出 `<instance>.parquet`；
    - [ ] 批大小：输入 RecordBatch 尺寸与输出批尺寸可调（例如 8K/64K 行），以兼顾内存与吞吐；
    - [ ] I/O：顺序读取、尽量避免随机访问；可在文件层面启用较大的读缓冲；
    - [ ] 内存上界：约为 k 个输入批的同时常驻量 + 一个输出批 + 堆（k log k），k 为同时参与的 Run 数；用 fan-in 控制 k；
    - [ ] 分配复用：循环复用列构建器与工作缓冲，减少分配与拷贝。
- [ ] 归并结束后删除该实例的 `*.000N.parquet`。
- [ ] 归并中输出 tracing：归并路数、写出行数、耗时、压缩比、堆大小、批尺寸等关键指标。

### 验证步骤
- [ ] 在测试中将 `run_rows` 下调以确保触发多 Run 情况，归并后读取 `<instance>.parquet`，验证全局 (db, key) 有序。
- [ ] 验证最终文件压缩算法为 `compression`、分段文件压缩算法为 `intermediate_compression`。
- [ ] 验证最终文件 Parquet 元数据包含 sorting_columns 且为 (db, key) 升序。
- [ ] 压测：构造高 k（如 200+）的 Run 数量，验证 fan-in 多轮归并路径正确性与资源峰值受控。

---

## 阶段五：与 dump 流程集成与并发控制

确保排序过程与 dump 共享协程、并使用 `DumpArgs.concurrency` 控制实例级并发，避免额外 CPU 竞争。

### 实现步骤
- [ ] 在当前 Parquet 输出 Writer 生命周期中插入 Run 生成与实例归并流程；
- [ ] 串联与 `DumpArgs.concurrency`：确保实例任务数量上限与 dump 相同；
- [ ] 在所有实例完成后，调用“批次最终化”完成目录 rename。

### 验证步骤
- [ ] 在本地/CI 环境执行端到端 dump（多实例）流程，观察 CPU 与并发度符合预期（无额外独立线程池/阻塞 IO）。
- [ ] 验证最终目录为 `cluster=<cluster>/batch=<batch>/`，且每实例仅有一个 `<instance>.parquet`。

---

## 阶段六：可观测性与健壮性

补充必要的日志、错误处理与清理逻辑，确保失败后可重试。

### 实现步骤
- [ ] 为关键阶段增加 tracing（run_gen/merge/finalize）与统计（行数、大小、耗时）。
- [ ] 失败清理：异常时保留 `_tmp_batch=<batch>` 目录（便于排查）；允许整体清理后重跑。
- [ ] 错误信息包含上下文（实例名、分段序号、路径、压缩参数）。

### 验证步骤
- [ ] 人工注入故障（写入失败/读出失败），确认错误日志可定位问题，且不会产生部分可见的最终目录。

---

## 阶段七：测试与文档

调整/新增测试覆盖新的路径布局与排序行为，补充 README/变更日志的要点。

### 实现步骤
- [ ] 更新现有 Parquet 相关测试以适配新路径（Hadoop 风格目录与最终化语义）。
- [ ] 新增：
  - [ ] Run 切分与落盘测试（将 `run_rows` 调小以强制生成多个 Run）。
  - [ ] 实例内多路归并正确性测试（依赖多 Run 场景）。
  - [ ] 压缩算法组合（最终 zstd、分段 lz4）测试；
  - [ ] 多实例端到端测试（含并发）。
  - [ ] Parquet 元数据校验：分段与最终文件的 sorting_columns 均为 (db, key) 升序。
- [ ] 更新 README.zh_CN/README：说明新的 Parquet 目录布局与参数（简要）。

### 验证步骤
- [ ] 执行 `cargo test --all` 通过。
- [ ] 手工对比旧版本与新版本的 dump 时长/CPU/磁盘使用（非必须，作为补充观察）。

---

## 里程碑与范围说明
- 本次范围仅实现“实例级外部归并排序 + 批次级最终化（目录重命名）”。
- 不包含：从 Parquet 生成报告、跨实例全局合并产物、`run-bytes` 硬上限（需 allocator）。
- 参数回顾：
  - `DumpArgs.concurrency`（并发来源，共享协程）；
  - `run_rows`（默认 100000，可在测试中调小以覆盖多 Run 场景）；
  - `compression`（最终，默认 zstd）；
  - `intermediate_compression`（Run，默认 lz4）。
- 实现要点：Run 采用 BTree 增量有序缓冲（避免一次性排序导致 CPU 尖刺），实例内通过 k 路归并生成最终文件；所有输出文件通过 WriterProperties 的 sorting_columns 明确声明 (db, key) 排序键，以便下游引擎感知；基于 RDB 语义假设不会产生重复 (db, key)。
