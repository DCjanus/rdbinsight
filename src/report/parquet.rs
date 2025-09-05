use std::{
    cmp::Reverse,
    collections::{HashMap, VecDeque},
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
use time::OffsetDateTime;

use crate::{
    helper::{AnyResult, SortMergeIterator},
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
    pub batch_slug: String,
}

impl ParquetReportProvider {
    pub fn new(base_dir: PathBuf, cluster: String, batch_slug: String) -> Self {
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

        let candidate = cluster_dir.join(format!("batch={}", self.batch_slug));
        if candidate.exists() {
            Ok(candidate)
        } else {
            bail!("Specified batch slug not found: {}", candidate.display());
        }
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

    fn build_group_merge_iterator(
        &self,
    ) -> AnyResult<SortMergeIterator<DBElementsIterator, (Bytes, u64)>> {
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
        let iter = SortMergeIterator::new(iterators)?;
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
        const PROGRESS_INTERVAL: u64 = 10_000_000;

        let threshold = (total_size / 100).max(1);
        let mut processed: u64 = 0;

        let iter = self.build_group_merge_iterator()?;

        struct Node {
            prefix: Bytes,
            total_size: u64,
            key_count: u64,
        }

        let mut stack: Vec<Node> = Vec::new();
        let mut prev_key: Bytes = Bytes::new();
        let mut out: Vec<PrefixAggregate> = Vec::new();

        fn lcp_len(a: &Bytes, b: &Bytes) -> usize {
            let min_len = a.len().min(b.len());
            let mut i = 0;
            while i < min_len && a[i] == b[i] {
                i += 1;
            }
            i
        }

        // Helper: pop nodes deeper than target depth, bubble up totals to parent,
        // and emit significant prefixes using the existing deduplicate_push logic.
        let pop_to_depth = |target_depth: usize, stack: &mut Vec<Node>, out: &mut Vec<PrefixAggregate>| {
            let mut agg_candidates: Vec<PrefixAggregate> = Vec::new();
            while stack.len() > target_depth {
                let node = stack.pop().expect("stack not empty when popping");
                // Bubble up to parent so ancestors receive full subtree totals
                if let Some(parent) = stack.last_mut() {
                    parent.total_size = parent.total_size.saturating_add(node.total_size);
                    parent.key_count = parent.key_count.saturating_add(node.key_count);
                }
                if node.total_size >= threshold {
                    agg_candidates.push(PrefixAggregate {
                        prefix: node.prefix,
                        total_size: node.total_size,
                        key_count: node.key_count,
                    });
                }
            }
            if !agg_candidates.is_empty() {
                deduplicate_push(agg_candidates, out);
            }
        };

        for ret in iter {
            let (key, size) = ret.context("failed to get key and size")?;

            processed = processed.saturating_add(1);
            if processed.is_multiple_of(PROGRESS_INTERVAL) {
                let processed_fmt = crate::helper::format_number(processed as f64);
                tracing::info!(
                    operation = "scan_top_prefix_progress",
                    processed = %processed_fmt,
                    total_size = total_size,
                    "Report scanning progress"
                );
            }

            if !prev_key.is_empty() {
                let common = lcp_len(&prev_key, &key);
                // Close deeper-than-LCP prefixes from previous path
                pop_to_depth(common, &mut stack, &mut out);
            }

            // Extend stack to match current key length; create nodes lazily.
            // Only create missing depths above current stack len.
            let start_depth = stack.len();
            for depth in (start_depth + 1)..=key.len() {
                stack.push(Node {
                    prefix: key.slice(0..depth),
                    total_size: 0,
                    key_count: 0,
                });
            }

            // Accumulate this key only at the deepest node; totals bubble up on pop
            if let Some(deepest) = stack.last_mut() {
                deepest.total_size = deepest.total_size.saturating_add(size);
                deepest.key_count = deepest.key_count.saturating_add(1);
            }

            prev_key = key;
        }

        // Flush remaining nodes
        pop_to_depth(0, &mut stack, &mut out);

        out.sort_by(|a, b| a.prefix.cmp(&b.prefix));
        Ok(out)
    }
}

fn deduplicate_push(mut agg: Vec<PrefixAggregate>, out: &mut Vec<PrefixAggregate>) {
    if agg.len() < 2 {
        for a in &agg {
            let total_size_fmt = crate::helper::format_number(a.total_size as f64);
            tracing::info!(
                operation = "significant_prefix_discovered",
                prefix = %String::from_utf8_lossy(&a.prefix),
                total_size = %total_size_fmt,
                key_count = a.key_count,
                "Discovered new significant prefix"
            );
        }
        out.extend_from_slice(&agg);
        return;
    }

    agg.sort_by(|x, y| x.prefix.cmp(&y.prefix));

    for (cur, nxt) in agg.iter().tuple_windows() {
        // some checks to ensure the invariant is met
        assert!(
            cur.prefix.len() < nxt.prefix.len(),
            "unexpected error found: prefix length invariant violated"
        );
        assert!(
            nxt.prefix.starts_with(&cur.prefix),
            "unexpected error found: prefix order invariant violated"
        );

        if cur.key_count == nxt.key_count {
            assert_eq!(
                cur.total_size, nxt.total_size,
                "unexpected error found: total_size should be equal when key_count is equal"
            );
            continue;
        }

        assert!(
            cur.key_count > nxt.key_count,
            "unexpected error found: key_count should be greater when total_size is greater"
        );
        assert!(
            cur.total_size > nxt.total_size,
            "unexpected error found: total_size should be greater when key_count is greater"
        );
        {
            let total_size_fmt = crate::helper::format_number(cur.total_size as f64);
            tracing::info!(
                operation = "significant_prefix_discovered",
                prefix = %String::from_utf8_lossy(&cur.prefix),
                total_size = %total_size_fmt,
                key_count = cur.key_count,
                "Discovered new significant prefix"
            );
        }
        out.push(cur.clone());
    }
    // last one is the longest prefix, always should be pushed
    if let Some(last) = agg.last() {
        let total_size_fmt = crate::helper::format_number(last.total_size as f64);
        tracing::info!(
            operation = "significant_prefix_discovered",
            prefix = %String::from_utf8_lossy(&last.prefix),
            total_size = %total_size_fmt,
            key_count = last.key_count,
            "Discovered new significant prefix"
        );
        out.push(last.clone());
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

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
            expect: Vec<PrefixAggregate>,
        }

        let cases = vec![
            Case {
                name: "nested_identical",
                input: vec![
                    make_prefix(b"f", 100, 3),
                    make_prefix(b"fo", 100, 3),
                    make_prefix(b"foo", 100, 3),
                ],
                expect: vec![make_prefix(b"foo", 100, 3)],
            },
            Case {
                name: "nested_different",
                input: vec![
                    make_prefix(b"f", 100, 3),
                    make_prefix(b"fo", 50, 2),
                    make_prefix(b"foo", 30, 1),
                ],
                expect: vec![
                    make_prefix(b"f", 100, 3),
                    make_prefix(b"fo", 50, 2),
                    make_prefix(b"foo", 30, 1),
                ],
            },
            Case {
                name: "nested_same_size",
                input: vec![
                    make_prefix(b"f", 100, 3),
                    make_prefix(b"fo", 30, 1),
                    make_prefix(b"foo", 30, 1),
                ],
                expect: vec![make_prefix(b"f", 100, 3), make_prefix(b"foo", 30, 1)],
            },
        ];

        for case in cases {
            let mut out = Vec::new();
            deduplicate_push(case.input, &mut out);
            assert_eq!(out, case.expect, "{}: {:?}", case.name, out);
        }
    }

    #[test]
    #[should_panic(expected = "prefix length invariant violated")]
    fn dedup_prefix_length_invariant_violation() {
        let a = PrefixAggregate {
            prefix: Bytes::from_static(b"aa"),
            total_size: 1,
            key_count: 1,
        };
        let b = PrefixAggregate {
            prefix: Bytes::from_static(b"bb"),
            total_size: 1,
            key_count: 1,
        };
        let mut out = Vec::new();
        deduplicate_push(vec![a, b], &mut out);
    }

    #[test]
    #[should_panic(expected = "prefix order invariant violated")]
    fn dedup_prefix_order_invariant_violation() {
        let cur = PrefixAggregate {
            prefix: Bytes::from_static(b"a"),
            total_size: 10,
            key_count: 5,
        };
        let nxt = PrefixAggregate {
            prefix: Bytes::from_static(b"bc"),
            total_size: 5,
            key_count: 2,
        };
        let mut out = Vec::new();
        deduplicate_push(vec![cur, nxt], &mut out);
    }

    #[test]
    #[should_panic(expected = "total_size should be equal when key_count is equal")]
    fn dedup_equal_key_count_total_size_mismatch() {
        let cur = PrefixAggregate {
            prefix: Bytes::from_static(b"a"),
            total_size: 10,
            key_count: 2,
        };
        let nxt = PrefixAggregate {
            prefix: Bytes::from_static(b"ab"),
            total_size: 5,
            key_count: 2,
        };
        let mut out = Vec::new();
        deduplicate_push(vec![cur, nxt], &mut out);
    }

    #[test]
    #[should_panic(expected = "key_count should be greater when total_size is greater")]
    fn dedup_key_count_invariant_violation() {
        let cur = PrefixAggregate {
            prefix: Bytes::from_static(b"a"),
            total_size: 100,
            key_count: 1,
        };
        let nxt = PrefixAggregate {
            prefix: Bytes::from_static(b"ab"),
            total_size: 50,
            key_count: 2,
        };
        let mut out = Vec::new();
        deduplicate_push(vec![cur, nxt], &mut out);
    }

    #[test]
    #[should_panic(expected = "total_size should be greater when key_count is greater")]
    fn dedup_total_size_invariant_violation() {
        let cur = PrefixAggregate {
            prefix: Bytes::from_static(b"a"),
            total_size: 10,
            key_count: 3,
        };
        let nxt = PrefixAggregate {
            prefix: Bytes::from_static(b"ab"),
            total_size: 20,
            key_count: 2,
        };
        let mut out = Vec::new();
        deduplicate_push(vec![cur, nxt], &mut out);
    }
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
            .ok_or_else(|| anyhow!("can't find field 'rdb_size' in parquet file"))?
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

        let mut inferred_batch_nanos: Option<i64> = None;
        let mut inferred_cluster: Option<String> = None;
        for path in files {
            let meta = Self::parse_file_metadata(&path)
                .with_context(|| format!("Failed to parse metadata for {}", path.display()))?;

            if inferred_batch_nanos.is_none() {
                inferred_batch_nanos = Some(meta.batch_unix_nanos);
            } else if inferred_batch_nanos != Some(meta.batch_unix_nanos) {
                // Different files report different batch timestamps -> hard error
                bail!(
                    "Parquet file {} reports batch_unix_nanos={} which differs from first file's batch_unix_nanos={}",
                    path.display(),
                    meta.batch_unix_nanos,
                    inferred_batch_nanos.unwrap()
                );
            }

            if inferred_cluster.is_none() {
                inferred_cluster = Some(meta.cluster.clone());
            } else if inferred_cluster.as_deref() != Some(meta.cluster.as_str()) {
                // Different files report different cluster -> hard error
                bail!(
                    "Parquet file {} reports cluster='{}' which differs from first file's cluster='{}'",
                    path.display(),
                    meta.cluster,
                    inferred_cluster.as_ref().unwrap()
                );
            }

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
                key: r.key.to_bytes(),
                rdb_size: r.rdb_size,
                member_count: r.member_count,
                r#type: r.type_name().to_string(),
                instance: meta.instance.clone(),
                db: r.db,
                encoding: r.encoding_name(),
                expire_at: r.expire_at_ms.map(|ms| {
                    // Convert milliseconds since Unix epoch to an RFC3339 string in UTC
                    let odt = OffsetDateTime::from_unix_timestamp_nanos((ms as i128) * 1_000_000)
                        .unwrap_or_else(|_| OffsetDateTime::from_unix_timestamp_nanos(0).unwrap());
                    odt.format(&time::format_description::well_known::Rfc3339)
                        .unwrap_or_default()
                }),
            }));
            top_keys.sort_by_key(|r| (Reverse(r.rdb_size), r.key.clone()));
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

        let nanos = inferred_batch_nanos
            .ok_or_else(|| anyhow!("Missing summary metadata batch timestamp in parquet files"))?;
        let odt = OffsetDateTime::from_unix_timestamp_nanos(nanos as i128)
            .with_context(|| "Failed to construct batch timestamp from parquet metadata")?;
        let batch_str = odt
            .format(&time::format_description::well_known::Rfc3339)
            .with_context(|| "Failed to format batch timestamp")?;

        let cluster_str = inferred_cluster
            .ok_or_else(|| anyhow!("Missing summary metadata cluster in parquet files"))?;

        let report = ReportData {
            cluster: cluster_str,
            batch: batch_str,
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
