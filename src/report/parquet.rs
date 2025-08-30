use std::{cmp::Reverse, collections::HashMap, fs, ops::AddAssign, path::PathBuf};

use anyhow::{Context, Result};
use base64::Engine;
use bytes::Bytes;
use itertools::Itertools;

use crate::{
    helper::AnyResult,
    output::parquet::merge::{KeysStatistics, RecordsSummary},
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
            anyhow::bail!(
                "Cluster directory does not exist: {}",
                cluster_dir.display()
            );
        }

        if let Some(slug) = &self.batch_slug {
            let candidate = cluster_dir.join(format!("batch={}", slug));
            if candidate.exists() {
                return Ok(candidate);
            } else {
                anyhow::bail!("Specified batch slug not found: {}", candidate.display());
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

        anyhow::bail!("No batch directories found under {}", cluster_dir.display());
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

    fn parse_file_metadata(path: &PathBuf) -> Result<RecordsSummary> {
        use std::fs::File;

        use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};

        let file = File::open(path)
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
            .find(|kv| kv.key == "rdbinsight.meta.summary.b64_msgpack")
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
