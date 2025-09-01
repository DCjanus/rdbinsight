## Parquet 报告 Provider：Top Prefix 动态前缀扫描（基于现有实现的开发计划）

本文件描述基于当前代码实现状态下，完成“Top Prefix（动态前缀发现）”能力的开发计划。目标是最小化扫描开销：大部分统计来自写入端的元数据摘要，只有“前缀发现”需要实际读取数据列，并最终把结果写入 `ReportData.top_prefixes`。

本计划以现有代码为准，不再拘泥于早先的实现步骤或具体 API 选择（例如异步 Reader）。

## 需求背景

- 需要在“跨集群内所有实例、所有 DB 的所有 key”维度上，找出显著的 key 前缀集合，用于研判数据分布与业务命名规范是否合理。
- 显著性的阈值规则固定：`threshold = max(1, total_size / 100)`，其中 `total_size` 为全局 key 的 `rdb_size` 之和（来自元数据聚合）。
- 仅为“前缀扫描”进行最小化读取：投影 `{ key, rdb_size }` 两列，其余聚合指标依赖元数据摘要。

## 现状（以当前代码为准）

位置：`src/report/parquet.rs`

- 目录与批次
  - `find_batch_dir`：按 `cluster=<cluster>/batch=<slug>` 目录约定定位批次目录；若未指定 `slug`，按字典序选择最新。
  - `list_parquet_files`：列出批次目录下 `*.parquet` 并排序。

- 元数据解析与统计聚合
  - `parse_file_metadata(path)`：从 Parquet KV 中取出 base64-encoded MessagePack 摘要，并反序列化为 `RecordsSummary`；随后在 `generate_report_data` 中做全局聚合（db/type/instance、top_keys、big_keys、槽倾斜判断等）。
  - 键名约定：仅使用 `rdbinsight.meta.v1.summary.msgpack.b64`，若缺失则报错。

- 为前缀扫描准备的 per-db 数据迭代器
  - `DBElementsIterator`：基于 `ParquetRecordBatchReaderBuilder` 构建，仅投影 `{ key, rdb_size }`。
  - 已使用行组统计对 `db` 做行组剪枝；并尝试使用 RowFilter 做 `db == <target>` 的行过滤。
  - 退化路径（RowFilter 不可用时）尚未实现；同时 per-db 迭代器的批内过滤逻辑缺失。

- 结果拼装
  - `generate_report_data` 已输出除 `top_prefixes` 外的其它字段；`top_prefixes` 目前为 `vec![]`。

## 与最初计划的差异（作为后续设计的边界）

- Reader 路线：当前使用同步 `ParquetRecordBatchReader`，而非异步 Arrow Reader。后续计划以同步实现为基准完成 Top Prefix。
- 索引与约束：不强制要求 Page/Column/Offset Index 必开（若存在则可利用），核心依赖行组统计的 min/max 剪枝。

## 待实现的部分（达成目标所需）

### 1) 元数据键名与阈值来源

- 在 `parse_file_metadata(path)` 中，仅读取键名 `rdbinsight.meta.v1.summary.msgpack.b64`；若不存在，报错并指明缺失该键名。
- `threshold` 的 `total_size` 来自所有文件汇总后的 `keys_statistics.total_size`，无需额外扫描。

### 2) 构建所有 per-db 迭代器（签名与行为调整）

- 将 `fn build_all_iterators(self)` 改为 `fn build_all_iterators(&self)`，避免移动 `self`，以便在 `generate_report_data` 内复用。
- 实现逻辑：
  - 遍历批次目录内的所有 parquet 文件；
  - 通过 `parse_file_metadata` 取出该文件存在的 `db` 列表；
  - 对每个 `db` 调用 `db_elements_iterator(path, db)` 生成 `DBElementsIterator`；
  - 返回所有迭代器的列表。

### 3) per-db 迭代器的 RowFilter 退化路径

- 目标：当 RowFilter 不可用或失效时，仍能在“批内”正确过滤 `db == <target>` 行，避免跨 DB 串联导致前缀逻辑错误。
- 实现要点：
  - 退化时把 `db` 列加入投影（即 `{ key, rdb_size, db }`）。
  - 在 `DBElementsIterator::fill()` 中对 batch 内的行执行 `db == target_db` 判断；仅将匹配行压入缓冲，并对外仍暴露 `(key, rdb_size)`。
  - 注意 `db` 物理类型通常为有符号整型（`Int64`），需做非负校验并转换为 `u64` 再比较。

### 4) 行组剪枝索引一致性检查（推荐）

- 当前通过 Arrow 字段索引获取行组中 `db` 列统计，通常可行，但更稳妥的方式是以 Parquet 物理列索引定位 `db` 列，避免潜在错位。
- 若切换为 Parquet 列索引，需在构建 `ProjectionMask` 与读取统计时保持一致。

### 5) K 路归并为“重复 key 分组”流

- 目标：把所有 `DBElementsIterator` 合并为一个按 key 递增的迭代器；当来自不同子流的相同 key 连续出现时，合并为单条分组输出 `(key, size_sum, dup_count)`。
- 形态：基于 `BinaryHeap` 的最小堆（堆元素包含 `key`、`size`、`iter_idx`），按字节序比较 `key`。
- 输出：`Iterator<Item = AnyResult<(Bytes, u64, usize)>>`。

### 6) LCP 单遍扫描产出前缀聚合

- 目标：在“重复 key 分组”流上执行 LCP 算法，维护活跃前缀链（每一层保存 `prefix/size_sum/key_count`），以单遍完成聚合。
- 阈值：`threshold = max(1, total_size / 100)`。
- 输出：`Vec<PrefixAggregate>`，其中 `prefix` 为 `Bytes`，`total_size` 为累加的 `rdb_size`，`key_count` 为累计的“重复组”计数。
- 排序：默认按前缀字节序升序输出；如需与其它管道对齐，可切换为按 `total_size` 降序（留待产品口径统一时调整）。

### 7) 在 `generate_report_data` 中接线

- 既有聚合流程保持不变（db/type/instance/top_keys/big_keys/slot skew）。
- 新增步骤：
  1. 通过汇总的 `keys_statistics.total_size` 计算 `threshold`；
  2. 调用 `build_all_iterators(&self)` 获取所有 `DBElementsIterator`；
  3. 用“最小堆合并”构建“重复 key 分组”迭代器；
  4. 调用 LCP 扫描产出 `top_prefixes`；
  5. 将 `top_prefixes` 写入 `ReportData`。

## 非功能性要求

- 性能：
  - 前缀扫描仅投影 `{ key, rdb_size }`；优先利用行组剪枝与（可用时）RowFilter。
  - 退化路径下的批内过滤应保持线性扫描，避免附加分配与拷贝。
- 正确性：
  - 任何情况下不得把不同 `db` 的 key 串联进同一子流；退化路径必须严格过滤。
  - 元数据缺失（特别是 `rdbinsight.meta.v1.summary.msgpack.b64`）时，应报出明确的错误信息。

## 验收标准

- `ReportData.top_prefixes` 按阈值规则产出，字段：`prefix_base64/total_size/key_count` 正确。
- 对同一数据集，结果在不同机器/多次运行间一致（与排序口径一致）。
- RowFilter 不可用时，退化路径能产出同样的结果。
- 大样本下性能优于“全列读取”的实现；扫描仅限于 `{ key, rdb_size }`。

## 测试建议

- 单元测试：
  - 元数据解析：仅 `rdbinsight.meta.v1.summary.msgpack.b64` 可被识别；缺失时给出清晰错误；`total_size` 聚合正确。
  - 行组剪枝：`db` 在边界（min/max）时仍被包含；统计缺失能给出清晰错误。
  - K 路归并去重：来自不同子流的相同 key 能被合并；`size_sum/dup_count` 正确。
  - LCP 扫描：
    - LCP 计算在相邻不同 key 上正确收敛。
    - 阈值边界（`total_size=0` 时阈值为 1）。
    - 跨 DB 的相同前缀累计（依赖 per-db 子流与合并器的正确性）。
  - 退化路径：关闭 RowFilter 后，“批内过滤”结果与 RowFilter 路径一致。

- 集成测试：
  - 真实 parquet 样本：多文件、多 DB、多实例，验证 `top_prefixes` 与 `top_keys`、聚合统计不冲突。

## 风险与对策

- RowFilter API/实现差异导致推不下：提供健壮的批内过滤退化路径。
- Arrow 字段索引与 Parquet 列索引错位：推荐使用 Parquet 列索引读取统计，或在现有实现上增加一致性断言。
- 迭代器数量较多时的内存与堆开销：
  - 单条元素体量小（`Bytes+u64`），堆仅存活每个子流一个元素，通常可控。
  - 如遇超大规模 DB 数量，可分批归并（后续优化）。
- `key` 列类型变体：当前写端为 Binary，若历史数据存在 LargeBinary，需要在读取处做分支兼容。

## 工作顺序（建议）

1. 元数据键名兼容与错误信息优化（`parse_file_metadata`）。
2. `build_all_iterators(&self)` 签名调整与实现稳定化（保持最小投影与行组剪枝）。
3. 为 `DBElementsIterator` 增加 RowFilter 退化路径（批内过滤）。
4. 实现 K 路归并的“重复 key 分组”合并器。
5. 实现 LCP 单遍扫描与阈值过滤；输出 `Vec<PrefixAggregate>`。
6. 在 `generate_report_data` 中接线并产出 `top_prefixes`。
7. 单元/集成测试与性能回归。

## 接口草案（仅供参考，最终以实现为准）

```rust
// 读取 RecordsSummary（仅 v1 键名）
fn parse_file_metadata(path: &Path) -> AnyResult<RecordsSummary>;

// 构建所有 per-db 迭代器
fn build_all_iterators(&self) -> AnyResult<Vec<DBElementsIterator>>;

// 最小堆合并为“重复 key 分组”流
fn merge_grouped_iters(
    iters: Vec<DBElementsIterator>,
) -> impl Iterator<Item = AnyResult<(Bytes, u64, usize)>>;

// LCP 单遍扫描，返回显著前缀
fn scan_top_prefixes<I>(
    grouped: I,
    total_size: u64,
) -> AnyResult<Vec<crate::report::model::PrefixAggregate>>
where
    I: Iterator<Item = AnyResult<(Bytes, u64, usize)>>;
```

---

完成以上待实现项后，`ParquetReportProvider::generate_report_data` 将能够产出包含 `top_prefixes` 的完整 `ReportData`，且在 RowFilter 不可用时仍保持正确性与可接受的性能。

## 实现指南：Parquet 报告 Provider 的动态前缀扫描（TOP Prefix）

本指南旨在指导 AI 在不了解其它上下文的情况下，实现基于 Parquet 的“动态前缀发现（Top Prefix）”逻辑，并将结果汇入报告数据结构。实现以最小扫描为目标：绝大部分统计来自写入端元数据摘要，仅为前缀发现做一次最小化主扫描。

参考 API 文档：
- parquet 异步 Arrow 读取器（`ParquetRecordBatchStreamBuilder`）：[docs.rs 链接](https://docs.rs/parquet/latest/parquet/arrow/async_reader/type.ParquetRecordBatchStreamBuilder.html)

---

### 目标与约束
- 全局维度计算显著前缀：跨集群内所有实例、所有 DB 的所有 key。相同前缀在不同 DB 出现应合并累计。
- 仅为“前缀扫描”读取数据页：投影 `{ key, rdb_size }` 两列；其它统计从元数据聚合得出。
- 严格校验写入端元数据：
  - 要求 KV：`rdbinsight.meta.version == "1"`；`rdbinsight.meta.summary.b64_msgpack` 存在且可解析。
  - 要求 `db` 列具备 row-group/page 级 min/max 统计，且启用了 Page/Column/Offset Index。
- 行组剪枝 + 行过滤：先以统计选择候选 row groups；再应用 `db == <value>` Row Filter 获取“单一 db 的按 key 递增子序列”。若 Row Filter 不可用，退化为“批内过滤”（性能较弱但结果正确）。
- 阈值规则固定：`threshold = max(1, total_size/100)`，其中 `total_size` 来自元数据的全局求和。

---

### 目录约定与输入
- 批次目录：`<base_dir>/cluster=<cluster>/batch=<slug>/*.parquet`
- 批次选择：若未显式指定，按 slug 字典序选择最新目录（或以 mtime 兜底）。

---

### 依赖与环境
- 需要 tokio 运行时与以下 crates（版本示意）：
  - `parquet = "56"`，`arrow = "56"`，`futures = "0.3"`，`bytes = "1"`
  - `serde = "1"`，`rmp-serde = "1"`（解析 MessagePack 摘要），`base64 = "0.22"`

---

### 顶层流程
1) 枚举批次目录内 `*.parquet` 文件。
2) 对每个文件：
   - 以 `ArrowReaderMetadata::load_async` 加载元数据一次并复用。
   - 校验 KV 元数据版本与摘要；反序列化摘要，汇总 global totals、per_db/per_type/instance、top_keys_full、dbs、slots 等。
3) 为每个文件的每个 `db` 值构建“仅 `{ key, rdb_size }` 的 per-db 流”，并对行组做剪枝：
   - 使用 `with_row_groups([...])` 选择候选 row groups。
   - 优先用 Row Filter 推下 `db == <value>` 条件；无法使用时退化为批内过滤。
4) 对所有 per-db 流做 K 路归并，得到“全局按 key 的重复分组”流。
5) 在该分组流上执行 LCP 单遍算法，维护活跃前缀链并按阈值过滤，输出 `Vec<PrefixAggregate>`。
6) 将此前由元数据合成的其它统计与 `top_prefixes` 一并组装为 `ReportData`。

---

### 关键实现细节与代码示例

#### 1. 加载文件与元数据
```rust
use parquet::arrow::async_reader::{ParquetRecordBatchStreamBuilder, ArrowReaderMetadata};
use tokio::fs::File;

async fn load_metadata(path: &str) -> anyhow::Result<(tokio::fs::File, ArrowReaderMetadata)> {
    let mut file = File::open(path).await?;
    let meta = ArrowReaderMetadata::load_async(&mut file, Default::default()).await?;
    // 保持文件句柄用于后续 new_with_metadata（可再 clone）
    let file = file; // move
    Ok((file, meta))
}
```

从 `meta.file_metadata()` 读取 schema 与 KV，校验：
```rust
use parquet::file::metadata::KeyValue;

fn kv_get<'a>(kvs: &'a [KeyValue], key: &str) -> Option<&'a str> {
    kvs.iter().find(|kv| kv.key == key).and_then(|kv| kv.value.as_deref())
}

fn validate_and_parse_summary(meta: &ArrowReaderMetadata) -> anyhow::Result<ParsedSummary> {
    let fmeta = meta.file_metadata();
    let kvs = fmeta.key_value_metadata().ok_or_else(|| anyhow::anyhow!("missing kv metadata"))?;
    anyhow::ensure!(kv_get(kvs, "rdbinsight.meta.version") == Some("1"), "metadata version mismatch");
    let b64 = kv_get(kvs, "rdbinsight.meta.summary.b64_msgpack").ok_or_else(|| anyhow::anyhow!("missing summary"))?;
    let raw = base64::decode(b64)?;
    let summary: ParsedSummary = rmp_serde::from_slice(&raw)?;
    Ok(summary)
}

#[derive(serde::Deserialize)]
struct ParsedSummary { /* 对应设计文档的字段：cluster, batch_unix_nanos, instance, ... */ }
```

查找 `db`/`key`/`rdb_size` 的列索引：
```rust
let schema_descr = meta.file_metadata().schema_descr();
let col_idx = |name: &str| -> anyhow::Result<usize> {
    schema_descr
        .columns()
        .iter()
        .position(|c| c.path().string() == name)
        .ok_or_else(|| anyhow::anyhow!(format!("missing column: {}", name)))
};
let db_idx = col_idx("db")?;
let key_idx = col_idx("key")?;
let size_idx = col_idx("rdb_size")?;
```

#### 2. 基于统计的行组剪枝
```rust
use parquet::file::metadata::RowGroupMetaData;

fn row_groups_may_contain_db(
    meta: &ArrowReaderMetadata,
    db_col_idx: usize,
    target_db: i64,
) -> anyhow::Result<Vec<usize>> {
    let rgs = meta.metadata().row_groups();
    let mut out = Vec::new();
    for (i, rg) in rgs.iter().enumerate() {
        let col = rg.column(db_col_idx);
        let Some(stats) = col.statistics() else {
            anyhow::bail!("db column statistics missing in row group {}", i);
        };
        let min_ok = stats.min_value().map_or(true, |min| {
            let mut buf = [0u8; 8];
            if min.len() == 8 { buf.copy_from_slice(min); i64::from_le_bytes(buf) <= target_db } else { true }
        });
        let max_ok = stats.max_value().map_or(true, |max| {
            let mut buf = [0u8; 8];
            if max.len() == 8 { buf.copy_from_slice(max); i64::from_le_bytes(buf) >= target_db } else { true }
        });
        if min_ok && max_ok { out.push(i) }
    }
    anyhow::ensure!(!out.is_empty(), "no candidate row groups for db={}", target_db);
    Ok(out)
}
```

#### 3. 构建 per-db 异步流（仅投影 `{ key, rdb_size }`）
优先使用 Row Filter（若可用），否则退化到批内过滤：
```rust
use parquet::arrow::ProjectionMask;

async fn build_per_db_stream(
    file: &tokio::fs::File,
    meta: &ArrowReaderMetadata,
    key_idx: usize,
    size_idx: usize,
    db_idx: usize,
    target_db: i64,
) -> anyhow::Result<parquet::arrow::async_reader::ParquetRecordBatchStream<tokio::fs::File>> {
    let file_clone = file.try_clone().await?;
    let projection = ProjectionMask::roots(meta.file_metadata().schema_descr(), [key_idx, size_idx]);
    let rg_candidates = row_groups_may_contain_db(meta, db_idx, target_db)?;

    let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(file_clone, meta.clone())
        .with_projection(projection)
        .with_row_groups(rg_candidates);

    // 可选：若启用 Row Filter 能力，可推下 db == target_db 的过滤
    // 具体 API 以所用 parquet 版本为准（伪代码占位）：
    // use parquet::arrow::arrow_reader::{RowFilter, RowFilterBuilder};
    // let row_filter = RowFilter::new(vec![RowFilterBuilder::new().eq_i64("db", target_db).build()]);
    // builder = builder.with_row_filter(row_filter);

    Ok(builder.build()?)
}
```

退化路径：若无法使用 Row Filter，则把 `db` 列加入投影，并在批内过滤行；过滤后只向后续阶段投喂 `(key, rdb_size)`。

#### 4. 将 RecordBatch 流转为元素流
```rust
use arrow_array::{RecordBatch, Array, UInt64Array};
use bytes::Bytes;
use futures::{Stream, StreamExt};

async fn to_element_stream<S>(mut rb_stream: S, key_is_large: bool)
    -> anyhow::Result<impl Stream<Item = anyhow::Result<(Bytes, u64)>>>
where
    S: futures::Stream<Item = parquet::errors::Result<RecordBatch>> + Unpin + Send + 'static,
{
    let s = async_stream::try_stream! {
        while let Some(batch) = rb_stream.next().await.transpose()? {
            let key_col = batch.column(0); // 依据 Projection 顺序：key, rdb_size
            let size_col = batch.column(1);
            let size_arr = size_col.as_any().downcast_ref::<UInt64Array>().ok_or_else(|| anyhow::anyhow!("rdb_size type"))?;

            for row in 0..batch.num_rows() {
                let key_bytes = if key_is_large {
                    let arr = arrow_array::cast::as_large_binary_array(key_col.as_ref());
                    Bytes::copy_from_slice(arr.value(row))
                } else {
                    let arr = arrow_array::cast::as_binary_array(key_col.as_ref());
                    Bytes::copy_from_slice(arr.value(row))
                };
                let size = size_arr.value(row);
                yield (key_bytes, size);
            }
        }
    };
    Ok(s)
}
```

#### 5. K 路归并为“重复 key 分组”流
```rust
use std::{cmp::Ordering, collections::BinaryHeap};
use bytes::Bytes;

struct HeapItem { key: Bytes, size: u64, stream_id: usize }
impl PartialEq for HeapItem { fn eq(&self, o:&Self)->bool { self.key == o.key } }
impl Eq for HeapItem {}
impl PartialOrd for HeapItem { fn partial_cmp(&self,o:&Self)->Option<Ordering>{ Some(o.key.cmp(&self.key)) } }
impl Ord for HeapItem { fn cmp(&self,o:&Self)->Ordering { o.key.cmp(&self.key) } }

// 假设每个 per-db 元素流都可通过 next_element(stream_id).await 拉取下一条 Option<(Bytes,u64)>
async fn merge_grouped<FutNext>(mut firsts: Vec<Option<(Bytes,u64)>>, mut next: impl FnMut(usize) -> FutNext)
    -> impl futures::Stream<Item = (Bytes, u64, usize)>
where
    FutNext: std::future::Future<Output = anyhow::Result<Option<(Bytes,u64)>>> + Send + 'static,
{
    use futures::StreamExt;
    let s = async_stream::stream! {
        let mut heap = BinaryHeap::new();
        for (i, item) in firsts.into_iter().enumerate() {
            if let Some((k, s)) = item { heap.push(HeapItem{ key: k, size: s, stream_id: i }); }
        }
        while let Some(mut top) = heap.pop() {
            let curr_key = top.key.clone();
            let mut sum = top.size;
            let mut dup = 1usize;
            if let Some((k,s)) = next(top.stream_id).await.unwrap() { heap.push(HeapItem{ key:k, size:s, stream_id: top.stream_id }); }
            while heap.peek().map(|h| h.key.as_ref()) == Some(curr_key.as_ref()) {
                let mut same = heap.pop().unwrap();
                sum += same.size; dup += 1;
                if let Some((k,s)) = next(same.stream_id).await.unwrap() { heap.push(HeapItem{ key:k, size:s, stream_id: same.stream_id }); }
            }
            yield (curr_key, sum, dup);
        }
    };
    s
}
```

#### 6. LCP 单遍算法（活跃前缀链 + 阈值过滤）
```rust
use bytes::Bytes;

fn lcp_len(a: &[u8], b: &[u8]) -> usize {
    let n = a.len().min(b.len());
    for i in 0..n { if a[i] != b[i] { return i; } }
    n
}

struct Node { prefix: Bytes, size_sum: u64, key_count: u64 }

fn close_tail_until(active: &mut Vec<Node>, upto: usize, threshold: u64, out: &mut Vec<PrefixAggregate>) {
    while active.len() > upto {
        let node = active.pop().unwrap();
        if node.size_sum >= threshold {
            out.push(PrefixAggregate { prefix: String::from_utf8_lossy(&node.prefix).to_string(), key_count: node.key_count, total_size: node.size_sum });
        }
    }
}

fn scan_prefixes(grouped: impl futures::Stream<Item=(Bytes,u64,usize)> + Unpin, total_size: u64) -> impl futures::Future<Output=anyhow::Result<Vec<PrefixAggregate>>> {
    use futures::StreamExt;
    async move {
        let threshold = total_size / 100; let threshold = if threshold == 0 { 1 } else { threshold };
        let mut prev_key = Bytes::new();
        let mut active: Vec<Node> = Vec::new();
        let mut out: Vec<PrefixAggregate> = Vec::new();

        futures::pin_mut!(grouped);
        while let Some((key, size_sum, dup)) = grouped.next().await {
            let l = lcp_len(&prev_key, &key);
            close_tail_until(&mut active, l, threshold, &mut out);
            // 扩展缺失层级
            for i in active.len()..key.len() {
                active.push(Node{ prefix: Bytes::copy_from_slice(&key[..=i]), size_sum: 0, key_count: 0 });
            }
            // 累加到活跃链所有节点（可按需优化为仅叶子累加 + 回溯聚合）
            for node in &mut active { node.size_sum += size_sum; node.key_count += dup as u64; }
            prev_key = key;
        }
        close_tail_until(&mut active, 0, threshold, &mut out);
        out.sort_by(|a,b| a.prefix.cmp(&b.prefix));
        Ok(out)
    }
}
```

> 注意：LCP 仅由相邻“重复 key 分组”的 `prev_key` 与 `curr_key` 决定，与 db 无关；无需记录 `prev_db_id`。

#### 7. 汇总为 ReportData
在 `ParquetReportProvider::generate_report_data` 中：
1) 先基于元数据摘要合成 `db_aggregates`、`type_aggregates`、`instance_aggregates`、`total_size`、`top_keys`、槽倾斜等。
2) 构建所有 per-db 元素流，K 路归并并执行 LCP 扫描，得到 `top_prefixes`。
3) 组装 `ReportData` 并返回。

---

### 测试建议
- 单元测试：
  - 元数据解析与版本校验；`total_size` 聚合正确。
  - 行组剪枝正确性：候选 row groups 覆盖且不多解码无关行组。
  - K 路归并重复分组：相同 key 被连续处理，`sum` 与 `dup` 正确。
  - LCP 前缀：跨 DB 同一前缀累计；阈值边界（`total_size=0` 时阈值=1）。
- 性能回归：确保仅 `{ key, rdb_size }` 投影下性能优于全列读取。

---

### 常见坑与规约
- 缺失统计或索引：按设计要求报错而非静默降级（除非明确走 DuckDB 等回退路径）。
- Row Filter 不可用时，必须将 `db` 列加入投影并在批内过滤，避免跨 DB 串联导致逻辑错误。
- 注意 `key` 列可能是 `Binary` 或 `LargeBinary`；根据实际 schema 选择对应 Array 访问器。
- 堆比较使用字节序，保持与写入侧排序一致（原始排序 `(cluster,batch,instance,db,key)`，在单一 db 子流上保证 `key` 递增）。

---

### 参考链接
- 异步 Arrow 读取器（`ParquetRecordBatchStreamBuilder`）：[docs.rs（parquet/arrow/async_reader）](https://docs.rs/parquet/latest/parquet/arrow/async_reader/type.ParquetRecordBatchStreamBuilder.html)

---

### 交付清单（完成标准）
- `ParquetReportProvider::generate_report_data` 能从目标批次目录产出完整 `ReportData`，其中 `top_prefixes` 来自本指南算法。
- 仅投影 `{ key, rdb_size }`；行组剪枝生效；Row Filter 可用时被推下。
- 对不符合规范的 Parquet 文件（版本或统计缺失）给出明确错误信息。
- 小样本验证结果与 ClickHouse 路径口径一致（排序、字段、阈值规则）。


