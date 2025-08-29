# 背景与思路

现有基于 Parquet 作为中间 run 的多路归并在实践中出现内存峰值不可控的问题：在归并阶段每路至少常驻一个 Arrow RecordBatch，列式解码的中间缓冲（列数组、bitmap、预取等）进一步放大常驻，导致 O(fan_in × batch_rows × 行宽) 的峰值内存。即便限制 fan-in、缩小 batch 也难以从根本上消除这类不可控占用。

为解决该问题，本次将“中间 run 文件”替换为极简的 LZ4 流式格式，并在 finalize 阶段做“流式多路归并 → 最终 Parquet”。核心思想：
- run 写入时流式压缩，记录逐条落盘，无列式批量内存；
- 归并阶段每路仅常驻 1 条记录（Heap 的当前项），整体内存 ~ O(k)（k 为 run 数量），可显著降低峰值；
- 最终产物仍为 Parquet，保留完整 sorting_columns 与既有目录结构和原子重命名的可见性；
- 默认 run_rows（触发一次 flush 的 BTreeMap 条数）提升为 64K，以减少 run 个数与句柄开销。

压缩/解压采用 lz4_flex 的 Frame 接口以获得流式能力，参考文档：`https://docs.rs/lz4_flex/latest/lz4_flex/`。


# 关键设计

## 1. 中间 run 文件格式（.run.lz4）
- 扩展名：`.run.lz4`（与既有 Parquet `.run` 明确区分）。
- 容器：LZ4 Frame（流式），使用 `FrameEncoder` 写、`FrameDecoder` 读（参考 lz4_flex 文档）。
- 文件内容：一系列“记录块（record block）”顺序拼接，不含全局 header/索引（无需断点续传与兼容）。
- 记录块结构：
  - `len_be_u32`：payload 长度（大端 u32）。
  - `payload`：使用 bincode（v1，serde 驱动）序列化的结构体（见下）。
  - `crc32_be_u32`：CRC32（IEEE）校验，仅覆盖 `payload` 字节。
- 序列化对象：直接对 `Record`（以及其依赖的 `RDBStr`、枚举等）`derive(Serialize, Deserialize)`，按 bincode 默认配置编码；无需对 `RDBStr::Int` 做特别处理。
- 排序保证：单个 run 内所有记录按 `(db asc, key asc)` 严格有序。

## 2. 写 run（集成在 ParquetChunkWriter）
- 仍在内存中维护 `BTreeMap<SortKey, Record>`（键为 `(db, key)`），累积至阈值触发 flush。
- 默认阈值 `run_rows = 65536`（64K，可配）。
- flush 流程：
  - 申请自增 `idx`，输出路径：`<instance_sanitized>.<idx:06>.run.lz4`，位于本批次临时目录。
  - 打开 `FrameEncoder`，遍历 `BTreeMap` 的升序条目，逐条：
    - 使用 bincode 序列化为 `payload`；
    - 计算 `crc32(payload)`；
    - 依次写入 `len_be_u32 | payload | crc32_be_u32`；
  - 成功后清空内存 `BTreeMap`，并将该 run 路径加入候选集合。
- I/O 模式：在 `tokio::task::spawn_blocking` 中以同步 I/O 执行（`std::fs::File` + `BufWriter` + `FrameEncoder`），避免先聚合到 `Vec<u8>` 带来的瞬时内存峰值；对外接口保持 async，不阻塞 runtime。
- run 本身不携带 Parquet 元数据（sorting_columns 仅在最终产物设置）。

## 3. finalize：流式多路归并到最终 Parquet
- 输入：所有候选 `.run.lz4`；输出：最终 `<instance>.parquet`（位于 `_tmp_batch=` 目录下）。
- 执行模型：整个归并过程在 `tokio::task::spawn_blocking` 中以同步 I/O 执行；读端使用 `FrameDecoder`，写端使用 `ArrowWriter`，避免阻塞 async runtime。
- I/O 细节（读端）：每个输入文件以 `std::fs::File` 打开并通过 `std::io::BufReader` 包装后交给 `FrameDecoder`，减少系统调用与小块读放大；缓冲区建议 128–256 KiB。
- 读端：
  - 为每个输入构建 `FrameDecoder` 与 `RunCursor`：按块读取 `len|payload|crc`，校验后使用 bincode 反序列化为 `Record`，缓存“当前记录”。
  - 仅当该路被从堆取出并前进时，才继续读取下一块；若 EOF，则该路从堆中移除。
- 归并：
  - 以 `(db asc, key asc)` 为键建立最小堆（`BinaryHeap<Reverse<HeapItem>>`）。
  - 循环弹出最小项，将其转换/填充为 Arrow 行（常量列如 `cluster`、`instance`、`batch_ts` 由归并器统一填写），按 8K~16K 行批量写入 ArrowWriter。
  - ArrowWriter 的 `WriterProperties` 设置完整 `sorting_columns = schema::create_db_key_sorting_columns(...)`，最终压缩算法使用用户配置（默认 zstd）。
- 资源保护（FD 上限）：
  - 对外“不限 fan_in”。若 run 数量超出进程可用文件描述符上限，则内部分批执行（例如每组打开 N 路归并生成临时 `.run.lz4`，再归并这些临时产物直至单一输出），该策略对外透明。
- 成功关闭最终 Parquet 后，删除所有输入 `.run.lz4`。

## 4. 目录与命名
- 临时与最终目录沿用既有 Hadoop 风格分区：
  - 最终根：`<output_root>/cluster=<cluster>/batch=<batch>/`
  - 临时根：`<output_root>/cluster=<cluster>/_tmp_batch=<batch>/`
- 文件：
  - run：`<instance_sanitized>.<idx:06>.run.lz4`
  - 最终：`<instance_sanitized>.parquet`
- 批次最终化：全部实例完成后，将 `_tmp_batch=` 原子重命名为 `batch=`。

## 5. 参数与默认值
- `run_rows`：64K（65536；达到即触发一次 flush），可通过 CLI/配置调整。
- 最终 Parquet `compression`：默认 zstd；中间 run 的压缩固定为 LZ4（frame）。
- 可选内部限流：根据 ulimit 估算可并行打开的 run 数，超出时自动分批归并（对外不暴露为显式参数）。

## 6. 可观测性与错误处理
- 日志（遵循规范：`operation` 首位，描述置尾）：
  - run 写入：`parquet_run_lz4_flush_started/finished`，字段含 `idx`、`rows`、`bytes_uncompressed`、`bytes_compressed`、耗时与速率。
  - 归并：`parquet_run_lz4_merge_started/finished`，字段含输入数量、输出路径、批量大小、rows/s、MB/s；如触发分批归并，记录每轮的输入/输出规模与耗时。
  - 批次最终化：`parquet_batch_finalize`（保持不变）。
- 校验：
  - 读端对每条 `payload` 做 CRC32 校验，失败报错并中止当前实例合并，保留输入以便排查。
- 清理：
  - 仅在最终 Parquet 成功关闭后删除所有 `.run.lz4`；失败路径不删除。

## 7. 兼容性与范围
- run 文件仅为临时产物，无需兼容历史版本、无需断点续传；如中断，删除 `_tmp_batch=...` 后重跑。
- 最终 Parquet 的 schema、排序、目录结构与可见性保持既有行为。

## 8. 复杂度与资源占用（估算）
- 内存：~ O(k × 单条记录平均大小 + 输出批构建器)，远小于列式批读的 O(k × batch_rows × 行宽)。
- IO：相较“Parquet run → Parquet 合并”，取消了中间列式解码与批内存，CPU 更聚焦于 LZ4 与 bincode，整体 IO 更线性与可预测。


# 实施建议

- 定义与派生：
  - 为 `Record`、`RDBStr` 及相关枚举 `derive(Serialize, Deserialize)`；引入 `crc32fast` 与 `lz4_flex` 依赖。
- RunWriter/RunReader：
  - RunWriter：管理 `FrameEncoder`，提供 `write_record(&Record)`，内部完成 bincode、长度与 CRC 写入；在 `spawn_blocking` 中以同步 I/O 实现（`std::fs::File` + `BufWriter` + `FrameEncoder`）。
  - RunReader（游标）：管理 `FrameDecoder`，提供 `read_next() -> Option<Record>`，在 `spawn_blocking` 中以同步 I/O 实现（`std::fs::File` + `BufReader` + `FrameDecoder`），内部完成长度读取、payload 和 CRC 校验、bincode 反序列化。
- ParquetChunkWriter 集成：
  - 保持 `BTreeMap<SortKey, Record>` 聚合；当达到 `run_rows=64K` 时，创建 `RunWriter` 将所有条目按序写入新 `.run.lz4`，清空内存并登记候选。
  - `finalize_instance`：在 `spawn_blocking` 中以同步 I/O 执行归并，创建全部 `RunReader`，基于最小堆按 `(db,key)` 归并，批量写 `ArrowWriter` 到最终 Parquet，并在成功后删除候选。
- 资源保护（可选）：
  - 若候选过多导致 FD 不足，内部进行分批归并输出临时 `.run.lz4`，直至剩余数量足够一次性归并为最终 Parquet。
- 测试与验证：
  - 单元：RunWriter/RunReader 的回归（多条记录序列化/反序列化、CRC 校验错误路径）。
  - 集成：强制产生多 run（小 `run_rows`），验证最终 Parquet 顺序、sorting_columns 元数据、run 清理；构造大量 run 验证内存峰值与 FD 保护策略。


# 建议标题

```text
feat(parquet): switch run segments to LZ4 streaming with bincode blocks; stream k-way merge; default run_rows=64k
```
