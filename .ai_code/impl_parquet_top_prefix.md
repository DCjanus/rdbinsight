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


