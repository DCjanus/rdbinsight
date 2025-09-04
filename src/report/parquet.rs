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
        let threshold = (total_size / 100).max(1);
        let iter = self.build_group_merge_iterator()?;
        let mut active_prefixes: HashMap<Bytes, PrefixAggregate> = HashMap::new();
        let mut out: Vec<PrefixAggregate> = Vec::new();

        for ret in iter {
            let (key, size) = ret.context("failed to get key and size")?;

            let aggs = active_prefixes
                .extract_if(|prefix, _| !key.starts_with(prefix))
                .filter(|(_, aggregate)| aggregate.total_size >= threshold)
                .map(|(_, aggregate)| aggregate)
                .collect_vec();
            deduplicate_push(aggs, &mut out);

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

        let aggs = active_prefixes
            .into_iter()
            .filter(|(_, aggregate)| aggregate.total_size >= threshold)
            .map(|(_, aggregate)| aggregate)
            .collect_vec();
        deduplicate_push(aggs, &mut out);

        out.sort_by(|a, b| a.prefix.cmp(&b.prefix));
        Ok(out)
    }
}

fn deduplicate_push(mut agg: Vec<PrefixAggregate>, out: &mut Vec<PrefixAggregate>) {
    if agg.len() < 2 {
        out.extend_from_slice(&agg);
        return;
    }

    agg.sort_by(|x, y| x.prefix.cmp(&y.prefix));

    for (cur, nxt) in agg.iter().tuple_windows() {
        // some checks to ensure the invariant is met
        assert!(
            cur.key_count <= nxt.key_count,
            "key_count invariant violated"
        );
        assert!(
            cur.total_size <= nxt.total_size,
            "total_size invariant violated"
        );
        assert!(
            cur.prefix.len() < nxt.prefix.len(),
            "prefix length invariant violated"
        );
        assert!(
            nxt.prefix.starts_with(&cur.prefix),
            "prefix order invariant violated"
        );

        if cur.key_count == nxt.key_count {
            continue;
        }
        out.push(cur.clone());
    }
    // last one is the longest prefix, always should be pushed
    if let Some(last) = agg.last() {
        out.push(last.clone());
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
