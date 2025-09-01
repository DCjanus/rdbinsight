use std::{
    cmp::Reverse,
    collections::{HashMap, VecDeque},
    fs,
    ops::AddAssign,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use arrow::{
    array::{Array, ArrayAccessor, AsArray, RecordBatch, UInt64Array},
    datatypes::UInt64Type,
};
use base64::Engine;
use bytes::Bytes;
use futures_util::StreamExt;
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
        BigKey, ClusterIssues, DbAggregate, InstanceAggregate, ReportData, ReportDataProvider,
        TopKeyRecord, TypeAggregate,
    },
};

#[derive(Debug)]
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

    fn build_all_iterators(self) -> AnyResult<Vec<DBElementsIterator>> {
        let batch_dir = self.find_batch_dir()?;
        let files = Self::list_parquet_files(&batch_dir)?;
        let mut iterators = Vec::new();
        for file in files {
            let summary = Self::parse_file_metadata(&file)?;
            for db in summary.db_statistics.keys() {
                let iterator = Self::db_elements_iterator(&file, *db)?;
                iterators.push(iterator);
            }
        }
        Ok(iterators)
    }

    fn db_elements_iterator(path: &Path, target: u64) -> AnyResult<DBElementsIterator> {
        let file = std::fs::File::open(path).context("open parquet file")?;
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .context("try new parquet record batch reader builder")?;

        let metadata = builder.metadata().clone();
        let schema = builder.schema().clone();
        let parquet_schema = builder.parquet_schema();
        let db_mask = ProjectionMask::columns(&parquet_schema, ["db"]);

        let key_and_size_mask = ProjectionMask::columns(&parquet_schema, ["key", "rdb_size"]);
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
            top_prefixes: vec![], // TODO: implement top prefixes
            cluster_issues: ClusterIssues {
                big_keys,
                codis_slot_skew: codis_slots_map.into_values().any(|c| c > 1),
                redis_cluster_slot_skew: redis_slots_map.into_values().any(|c| c > 1),
            },
        };

        Ok(report)
    }
}
