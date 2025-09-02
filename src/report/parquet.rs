use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, VecDeque},
    fs,
    ops::AddAssign,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use arrow::{
    array::{Array, AsArray, RecordBatch, UInt64Array},
    datatypes::UInt64Type,
};
use base64::Engine;
use bytes::Bytes;
use itertools::Itertools;
use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::{
            ArrowPredicateFn, ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder, RowFilter,
        },
    },
    file::statistics::Statistics,
};

use crate::{
    helper::AnyResult,
    output::parquet::{
        merge::{KeysStatistics, RecordsSummary},
        schema::find_field_index,
    },
    record::RecordType,
    report::model::{
        BigKey, ClusterIssues, DbAggregate, InstanceAggregate, PrefixAggregate, ReportData,
        ReportDataProvider, TopKeyRecord, TypeAggregate,
    },
};

#[derive(Debug, Clone)]
pub struct ParquetReportProvider {
    pub base_dir: PathBuf,
    pub cluster: String,
    pub batch_slug: Option<String>,
}

impl ParquetReportProvider {
    pub fn new(base_dir: PathBuf, cluster: String, batch_slug: Option<String>) -> Self {
        Self {
            base_dir,
            cluster,
            batch_slug,
        }
    }

    fn find_batch_dir(&self) -> Result<PathBuf> {
        let cluster_dir = self.base_dir.join(format!("cluster={}", self.cluster));
        if !cluster_dir.exists() {
            bail!(
                "Cluster directory does not exist: {}",
                cluster_dir.display()
            );
        }

        if let Some(slug) = &self.batch_slug {
            let candidate = cluster_dir.join(format!("batch={}", slug));
            if candidate.exists() {
                return Ok(candidate);
            } else {
                bail!("Specified batch slug not found: {}", candidate.display());
            }
        }

        // choose latest by lexicographical order of slug under cluster dir
        let mut slugs: Vec<_> = fs::read_dir(&cluster_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
            .filter_map(|e| e.file_name().into_string().ok())
            .filter(|name| name.starts_with("batch="))
            .collect();

        slugs.sort();
        if let Some(latest) = slugs.pop() {
            return Ok(cluster_dir.join(latest));
        }

        bail!("No batch directories found under {}", cluster_dir.display());
    }

    fn list_parquet_files(batch_dir: &PathBuf) -> Result<Vec<PathBuf>> {
        let mut files = Vec::new();
        for entry in fs::read_dir(batch_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file()
                && path
                    .extension()
                    .and_then(|s| s.to_str())
                    .map(|ext| ext.eq_ignore_ascii_case("parquet"))
                    .unwrap_or_default()
            {
                files.push(path);
            }
        }
        files.sort();
        Ok(files)
    }

    fn build_group_merge_iterator(&self) -> AnyResult<GroupedMergeIterator> {
        let batch_dir = self.find_batch_dir()?;
        let files = Self::list_parquet_files(&batch_dir)?;
        let mut iterators = Vec::new();
        for file in files {
            let summary = Self::parse_file_metadata(&file)?;
            for db in summary.db_statistics.keys() {
                let iterator = Self::build_db_iterator(&file, *db)?;
                iterators.push(iterator);
            }
        }
        let iter = GroupedMergeIterator::new(iterators)?;
        Ok(iter)
    }

    fn build_db_iterator(path: &Path, target: u64) -> AnyResult<DBElementsIterator> {
        let file = std::fs::File::open(path).context("open parquet file")?;
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .context("try new parquet record batch reader builder")?;

        let metadata = builder.metadata().clone();
        let schema = builder.schema().clone();
        let parquet_schema = builder.parquet_schema();
        let db_mask = ProjectionMask::columns(parquet_schema, ["db"]);

        let key_and_size_mask = ProjectionMask::columns(parquet_schema, ["key", "rdb_size"]);
        builder = builder.with_projection(key_and_size_mask);

        let db_col_idx =
            find_field_index(&schema, "db").ok_or_else(|| anyhow!("missing 'db' field"))?;
        let mut row_group_idx = vec![];
        for (idx, rg) in metadata.row_groups().iter().enumerate() {
            let db_statistics = rg
                .column(db_col_idx)
                .statistics()
                .ok_or_else(|| anyhow!("found 'db' column without statistics"))?;
            let db_statistics = match db_statistics {
                Statistics::Int64(s) => s,
                _ => bail!("unexpected statistics type for 'db' field"),
            };
            let min = db_statistics
                .min_opt()
                .cloned()
                .ok_or_else(|| anyhow!("missing min statistics"))?;
            ensure!(min >= 0, "'db' field must be non-negative");
            let min = min as u64;
            let max = db_statistics
                .max_opt()
                .cloned()
                .ok_or_else(|| anyhow!("missing max statistics"))?;
            ensure!(max >= 0, "'db' field must be non-negative");
            let max = max as u64;
            if (min..=max).contains(&target) {
                row_group_idx.push(idx);
            }
        }
        builder = builder.with_row_groups(row_group_idx);

        let predict = ArrowPredicateFn::new(db_mask, move |batch: RecordBatch| {
            let scale = UInt64Array::new_scalar(target);
            let column = batch
                .column_by_name("db")
                .expect("no 'db' field")
                .as_primitive::<UInt64Type>();
            arrow::compute::kernels::cmp::eq(column, &scale)
        });
        builder = builder.with_row_filter(RowFilter::new(vec![Box::new(predict)]));

        let reader = builder.build()?;

        Ok(DBElementsIterator {
            reader,
            buffer: VecDeque::new(),
        })
    }

    fn parse_file_metadata(path: &Path) -> Result<RecordsSummary> {
        use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};

        let file = std::fs::File::open(path)
            .with_context(|| format!("Failed to open parquet file: {}", path.display()))?;
        let mut reader =
            ParquetMetaDataReader::new().with_page_index_policy(PageIndexPolicy::Required);
        reader
            .try_parse(&file)
            .with_context(|| format!("Failed to parse parquet file: {}", path.display()))?;
        let metadata = reader.finish().with_context(|| {
            format!("Failed to finish parsing parquet file: {}", path.display())
        })?;

        let file_meta = metadata.file_metadata();
        let kvs = file_meta.key_value_metadata().ok_or_else(|| {
            anyhow::anyhow!(
                "Missing key-value metadata in parquet file: {}",
                path.display()
            )
        })?;
        let summary_b64 = kvs
            .iter()
            .find(|kv| kv.key == "rdbinsight.meta.v1.summary.msgpack.b64")
            .ok_or_else(|| anyhow::anyhow!("Missing summary metadata in {}", path.display()))?
            .value
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Missing summary value in {}", path.display()))?;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(summary_b64)
            .with_context(|| format!("Failed to base64-decode summary in {}", path.display()))?;
        let summary: RecordsSummary = rmp_serde::from_slice(&decoded).with_context(|| {
            format!("Failed to decode messagepack summary in {}", path.display())
        })?;

        Ok(summary)
    }

    fn scan_top_prefix(&self, total_size: u64) -> AnyResult<Vec<PrefixAggregate>> {
        let threshold = (total_size / 100).max(1);
        let iter = self.build_group_merge_iterator()?;
        let mut active_prefixes: HashMap<Bytes, PrefixAggregate> = HashMap::new();
        let mut out: Vec<PrefixAggregate> = Vec::new();

        for ret in iter {
            let (key, size) = ret.context("failed to get key and size")?;

            let iter = active_prefixes
                .extract_if(|prefix, _| !key.starts_with(prefix))
                .filter(|(_, aggregate)| aggregate.total_size >= threshold)
                .map(|(_, aggregate)| aggregate);
            out.extend(iter);

            for new_prefix in (1..key.len()).map(|i| key.slice(0..i)) {
                let prefix_clone = new_prefix.clone();
                let item = active_prefixes
                    .entry(new_prefix)
                    .or_insert_with(|| PrefixAggregate {
                        prefix: prefix_clone,
                        total_size: 0,
                        key_count: 0,
                    });
                item.total_size += size;
                item.key_count += 1;
            }
        }

        out.extend(
            active_prefixes
                .into_iter()
                .filter(|(_, aggregate)| aggregate.total_size >= threshold)
                .map(|(_, aggregate)| aggregate),
        );

        Ok(merge_prefix_aggregates(out))
    }
}

/// Merge a list of prefix aggregates for the common case used by this codebase.
///
/// Assumptions:
/// - The input `aggregates` contains no duplicate prefix bytes (each `prefix` is unique).
/// - Prefixes may be nested (e.g. `b"f"`, `b"fo"`, `b"foo"`) but exact duplicates
///   are not present.
///
/// Behavior:
/// - Sorts the aggregates by prefix bytes so nested prefixes are adjacent.
/// - When a longer prefix is a descendant of the previous (shorter) prefix and
///   they have identical `total_size` and `key_count`, the longer prefix is kept
///   (we prefer the most-specific prefix) and the shorter is discarded.
/// - When nested prefixes have different statistics, both entries are kept to
///   avoid losing information.
///
/// This simplified implementation omits handling for exact duplicate prefixes
/// because callers guarantee uniqueness.
fn merge_prefix_aggregates(mut aggregates: Vec<PrefixAggregate>) -> Vec<PrefixAggregate> {
    // Sort by prefix bytes ascending so nested prefixes are adjacent
    aggregates.sort_by(|a, b| a.prefix.cmp(&b.prefix));

    let mut out: Vec<PrefixAggregate> = Vec::new();

    for agg in aggregates.into_iter() {
        if let Some(last) = out.last_mut() {
            // If current prefix is a descendant (longer) of the last prefix
            if agg.prefix.as_ref().starts_with(last.prefix.as_ref()) {
                // If stats are identical, prefer the longer (agg) and replace last
                if agg.total_size == last.total_size && agg.key_count == last.key_count {
                    *last = agg;
                    continue;
                }
                // Different stats -> keep both
                out.push(agg);
                continue;
            }
        }

        out.push(agg);
    }

    out
}

struct DBElementsIterator {
    reader: ParquetRecordBatchReader,
    buffer: VecDeque<(Bytes, u64)>,
}

impl DBElementsIterator {
    fn fill(&mut self) -> AnyResult {
        let batch = match self.reader.next() {
            None => {
                return Ok(());
            }
            Some(Err(e)) => Err(e)?,

            Some(Ok(batch)) => batch,
        };
        let key_array = batch
            .column_by_name("key")
            .ok_or_else(|| anyhow!("can't find field 'key' in parquet file"))?
            .as_binary::<i32>();
        let size_array = batch
            .column_by_name("rdb_size")
            .ok_or_else(|| anyhow!("can't find field 'size' in parquet file"))?
            .as_primitive::<UInt64Type>();
        for i in 0..key_array.len() {
            let key = key_array.value(i);
            let size = size_array.value(i);
            self.buffer.push_back((Bytes::copy_from_slice(key), size));
        }
        Ok(())
    }
}

impl Iterator for DBElementsIterator {
    type Item = AnyResult<(Bytes, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(head) = self.buffer.pop_front() {
            return Some(Ok(head));
        };
        if let Err(e) = self.fill() {
            return Some(Err(e));
        }
        self.buffer.pop_front().map(Ok)
    }
}

struct GroupedMergeIterator {
    iters: Vec<DBElementsIterator>,
    heap: BinaryHeap<HeapItem>,
}

impl GroupedMergeIterator {
    fn new(mut iters: Vec<DBElementsIterator>) -> AnyResult<Self> {
        let mut heap = BinaryHeap::new();
        for (idx, iter) in iters.iter_mut().enumerate() {
            if let Some(ret) = iter.next() {
                let (key, size) = ret?;
                heap.push(HeapItem {
                    key,
                    size,
                    iter_idx: idx,
                });
            }
        }
        Ok(Self { iters, heap })
    }
}

impl Iterator for GroupedMergeIterator {
    type Item = AnyResult<(Bytes, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        let top = self.heap.pop()?;

        let poped_iter = &mut self.iters[top.iter_idx];
        match poped_iter.next() {
            None => {}
            Some(Ok((key, size))) => {
                self.heap.push(HeapItem {
                    key,
                    size,
                    iter_idx: top.iter_idx,
                });
            }
            Some(Err(e)) => return Some(Err(e)),
        }

        Some(Ok((top.key, top.size)))
    }
}

#[async_trait::async_trait]
impl ReportDataProvider for ParquetReportProvider {
    async fn generate_report_data(&self) -> AnyResult<ReportData> {
        let batch_dir = self
            .find_batch_dir()
            .context("Failed to find batch directory")?;
        let files = Self::list_parquet_files(&batch_dir).context("Failed to list parquet files")?;

        if files.is_empty() {
            anyhow::bail!(
                "No parquet files found in batch dir: {}",
                batch_dir.display()
            );
        }

        let mut db_statistics: HashMap<u64, KeysStatistics> = HashMap::new();
        let mut type_statistics: HashMap<RecordType, KeysStatistics> = HashMap::new();
        let mut instance_statistics: HashMap<String, KeysStatistics> = HashMap::new();
        let mut keys_statistics: KeysStatistics = KeysStatistics::default();
        let mut top_keys: Vec<TopKeyRecord> = Vec::new();
        let mut big_keys: Vec<BigKey> = Vec::new();
        let mut codis_slots_map: HashMap<u16, u64> = HashMap::new(); // codis slot -> instance count
        let mut redis_slots_map: HashMap<u16, u64> = HashMap::new(); // redis slot -> instance count

        for path in files {
            let meta = Self::parse_file_metadata(&path)
                .with_context(|| format!("Failed to parse metadata for {}", path.display()))?;

            keys_statistics.add_assign(meta.keys_statistics.clone());

            for (db, statistics) in meta.db_statistics {
                db_statistics
                    .entry(db)
                    .or_default()
                    .add_assign(statistics.clone());
            }

            for (data_type, statistics) in meta.type_statistics {
                type_statistics
                    .entry(data_type)
                    .or_default()
                    .add_assign(statistics.clone());
            }

            instance_statistics
                .entry(meta.instance.clone())
                .or_default()
                .add_assign(meta.keys_statistics.clone());

            top_keys.extend(meta.top_keys.into_iter().map(|r| TopKeyRecord {
                key: Bytes::from(r.key.to_string()),
                rdb_size: r.rdb_size,
                member_count: r.member_count,
                r#type: r.type_name().to_string(),
                instance: meta.instance.clone(),
                db: r.db,
                encoding: r.encoding_name(),
                expire_at: r.expire_at_ms.map(|ms| ms.to_string()),
            }));
            top_keys.sort_by_key(|r| Reverse(r.rdb_size));
            top_keys.truncate(100);

            big_keys.extend(meta.big_keys.into_iter().map(|r| BigKey {
                key: Bytes::from(r.key.to_string()),
                instance: meta.instance.clone(),
                db: r.db,
                r#type: r.type_name().to_string(),
                rdb_size: r.rdb_size,
            }));

            for codis_slot in meta.codis_slots {
                codis_slots_map.entry(codis_slot).or_default().add_assign(1);
            }

            for redis_slot in meta.redis_slots {
                redis_slots_map.entry(redis_slot).or_default().add_assign(1);
            }
        }

        let total_size = keys_statistics.total_size;
        let this = self.clone();
        let top_prefixes =
            tokio::task::spawn_blocking(move || this.scan_top_prefix(total_size)).await??;
        let report = ReportData {
            cluster: self.cluster.clone(),
            batch: self.batch_slug.clone().unwrap_or_default(),
            db_aggregates: db_statistics
                .into_iter()
                .map(|(db, stats)| DbAggregate {
                    db,
                    key_count: stats.key_count,
                    total_size: stats.total_size,
                })
                .sorted_by_key(|x| x.db)
                .collect(),
            type_aggregates: type_statistics
                .into_iter()
                .map(|(data_type, stats)| TypeAggregate {
                    data_type: data_type.type_name().to_string(),
                    key_count: stats.key_count,
                    total_size: stats.total_size,
                })
                .sorted_by_key(|x| Reverse(x.total_size))
                .collect(),
            instance_aggregates: instance_statistics
                .into_iter()
                .map(|(instance, stats)| InstanceAggregate {
                    instance,
                    key_count: stats.key_count,
                    total_size: stats.total_size,
                })
                .sorted_by_key(|x| Reverse(x.total_size))
                .collect(),
            top_keys,
            top_prefixes,
            cluster_issues: ClusterIssues {
                big_keys,
                codis_slot_skew: codis_slots_map.into_values().any(|c| c > 1),
                redis_cluster_slot_skew: redis_slots_map.into_values().any(|c| c > 1),
            },
        };

        Ok(report)
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct HeapItem {
    key: Bytes,
    size: u64,
    iter_idx: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_heapitem_ordering_basic() {
        let a = HeapItem {
            key: Bytes::from_static(b"abc"),
            size: 10,
            iter_idx: 1,
        };
        let b = HeapItem {
            key: Bytes::from_static(b"abc"),
            size: 10,
            iter_idx: 2,
        };
        let c = HeapItem {
            key: Bytes::from_static(b"abd"),
            size: 5,
            iter_idx: 0,
        };
        let d = HeapItem {
            key: Bytes::from_static(b"abb"),
            size: 20,
            iter_idx: 3,
        };

        // Ord: key, size, iter_idx
        assert!(d < a); // "abb" < "abc"
        assert!(a < c); // "abc" < "abd"
        assert!(a < b); // same key/size, iter_idx 1 < 2
        assert!(b > a);
    }

    #[test]
    fn test_heapitem_ordering_stability() {
        // Items with same key and size but different iter_idx
        let mut items = vec![
            HeapItem {
                key: Bytes::from_static(b"foo"),
                size: 42,
                iter_idx: 2,
            },
            HeapItem {
                key: Bytes::from_static(b"foo"),
                size: 42,
                iter_idx: 0,
            },
            HeapItem {
                key: Bytes::from_static(b"foo"),
                size: 42,
                iter_idx: 1,
            },
        ];
        items.sort();

        assert_eq!(items[0].iter_idx, 0);
        assert_eq!(items[1].iter_idx, 1);
        assert_eq!(items[2].iter_idx, 2);
    }

    #[test]
    fn test_heapitem_binaryheap_minheap_behavior() {
        use std::collections::BinaryHeap;

        let mut heap = BinaryHeap::new();
        heap.push(HeapItem {
            key: Bytes::from_static(b"b"),
            size: 1,
            iter_idx: 0,
        });
        heap.push(HeapItem {
            key: Bytes::from_static(b"a"),
            size: 1,
            iter_idx: 0,
        });
        heap.push(HeapItem {
            key: Bytes::from_static(b"c"),
            size: 1,
            iter_idx: 0,
        });

        // BinaryHeap is max-heap by default, so pop order: "c", "b", "a"
        let mut keys = vec![];
        while let Some(item) = heap.pop() {
            keys.push(item.key.clone());
        }
        assert_eq!(keys, vec![
            Bytes::from_static(b"c"),
            Bytes::from_static(b"b"),
            Bytes::from_static(b"a"),
        ]);
    }

    #[test]
    fn test_heapitem_reverse_for_minheap() {
        use std::{cmp::Reverse, collections::BinaryHeap};

        let mut heap = BinaryHeap::new();
        heap.push(Reverse(HeapItem {
            key: Bytes::from_static(b"b"),
            size: 1,
            iter_idx: 0,
        }));
        heap.push(Reverse(HeapItem {
            key: Bytes::from_static(b"a"),
            size: 1,
            iter_idx: 0,
        }));
        heap.push(Reverse(HeapItem {
            key: Bytes::from_static(b"c"),
            size: 1,
            iter_idx: 0,
        }));

        // With Reverse, pop order: "a", "b", "c"
        let mut keys = vec![];
        while let Some(Reverse(item)) = heap.pop() {
            keys.push(item.key.clone());
        }
        assert_eq!(keys, vec![
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
            Bytes::from_static(b"c"),
        ]);
    }
    fn make_prefix(p: &'static [u8], total_size: u64, key_count: u64) -> PrefixAggregate {
        PrefixAggregate {
            prefix: Bytes::from_static(p),
            total_size,
            key_count,
        }
    }

    #[test]
    fn test_merge_prefix_aggregates_cases() {
        struct Case {
            name: &'static str,
            input: Vec<PrefixAggregate>,
            expect: Vec<(&'static [u8], u64, u64)>,
        }

        let cases = vec![
            Case {
                name: "nested_identical",
                input: vec![
                    make_prefix(b"f", 100, 3),
                    make_prefix(b"fo", 100, 3),
                    make_prefix(b"foo", 100, 3),
                ],
                expect: vec![(b"foo", 100, 3)],
            },
            Case {
                name: "nested_different",
                input: vec![
                    make_prefix(b"f", 100, 3),
                    make_prefix(b"fo", 50, 1),
                    make_prefix(b"foo", 30, 1),
                ],
                expect: vec![(b"f", 100, 3), (b"fo", 50, 1), (b"foo", 30, 1)],
            },
        ];

        for case in cases {
            let out = merge_prefix_aggregates(case.input);
            assert_eq!(out.len(), case.expect.len(), "{}: length", case.name);
            for (i, (p, s, k)) in case.expect.iter().enumerate() {
                assert_eq!(
                    out[i].prefix,
                    Bytes::from_static(*p),
                    "{}: prefix {}",
                    case.name,
                    i
                );
                assert_eq!(out[i].total_size, *s, "{}: total_size {}", case.name, i);
                assert_eq!(out[i].key_count, *k, "{}: key_count {}", case.name, i);
            }
        }
    }
}
