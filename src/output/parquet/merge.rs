use std::{
    cmp::Reverse,
    collections::{HashMap, HashSet},
    ops::AddAssign,
    path::PathBuf,
    sync::Arc,
};

use anyhow::{Context, anyhow};
use base64::Engine;
use parquet::{
    arrow::ArrowWriter,
    file::{metadata::KeyValue, properties::WriterProperties},
};
use serde::{Deserialize, Serialize};

use crate::{
    config::ParquetCompression,
    helper::AnyResult,
    output::parquet::{mapper::records_to_columns, run_lz4::RunReader, schema},
    record::RecordType,
};

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct KeysStatistics {
    pub key_count: u64,
    pub total_size: u64,
}

impl std::ops::AddAssign for KeysStatistics {
    fn add_assign(&mut self, other: KeysStatistics) {
        self.key_count += other.key_count;
        self.total_size += other.total_size;
    }
}

#[derive(Serialize, Deserialize)]
pub struct RecordsSummary {
    pub cluster: String,
    pub batch_unix_nanos: i64,
    pub instance: String,
    pub keys_statistics: KeysStatistics,
    pub db_statistics: HashMap<u64, KeysStatistics>,
    pub type_statistics: HashMap<RecordType, KeysStatistics>,
    pub codis_slots: HashSet<u16>,
    pub redis_slots: HashSet<u16>,
    pub top_keys: Vec<crate::record::Record>,
    pub big_keys: Vec<crate::record::Record>,
}

#[allow(dead_code)]
impl RecordsSummary {
    pub fn new(cluster: String, batch_unix_nanos: i64, instance: String) -> Self {
        RecordsSummary {
            cluster,
            batch_unix_nanos,
            instance,
            keys_statistics: KeysStatistics::default(),
            db_statistics: HashMap::new(),
            type_statistics: HashMap::new(),
            codis_slots: HashSet::new(),
            redis_slots: HashSet::new(),
            top_keys: Vec::new(),
            big_keys: Vec::new(),
        }
    }

    pub fn update_from_record(&mut self, record: &crate::record::Record) {
        self.keys_statistics.add_assign(KeysStatistics {
            key_count: 1,
            total_size: record.rdb_size,
        });
        self.db_statistics
            .entry(record.db)
            .or_default()
            .add_assign(KeysStatistics {
                key_count: 1,
                total_size: record.rdb_size,
            });
        self.type_statistics
            .entry(record.r#type)
            .or_default()
            .add_assign(KeysStatistics {
                key_count: 1,
                total_size: record.rdb_size,
            });
        if let Some(s) = record.codis_slot {
            self.codis_slots.insert(s);
        }
        if let Some(s) = record.redis_slot {
            self.redis_slots.insert(s);
        }

        self.update_top_keys(record);
        self.update_big_keys(record);
    }

    fn update_big_keys(&mut self, record: &crate::record::Record) {
        // Consider a key "big" if it's over 1 GiB, or if it's a String over 1 MiB
        if record.rdb_size > 1073741824
            || (record.r#type == RecordType::String && record.rdb_size > 1048576)
        {
            self.big_keys.push(record.clone());
        }
    }

    fn update_top_keys(&mut self, new: &crate::record::Record) {
        const COUNT: usize = 100;

        if self
            .top_keys
            .last()
            .map(|old| old.rdb_size >= new.rdb_size)
            .unwrap_or_default()
        {
            return;
        }

        self.top_keys.push(new.clone());
        self.top_keys
            .sort_by_key(|r| (Reverse(r.rdb_size), r.key.clone()));
        self.top_keys.truncate(COUNT);
    }
}

pub struct MergeContext {
    pub inputs: Vec<PathBuf>,
    pub output: PathBuf,
    pub compression: ParquetCompression,
    pub cluster: String,
    pub instance: String,
    pub batch_ts: time::OffsetDateTime,
}

impl MergeContext {
    pub fn merge(self) -> AnyResult<()> {
        if self.inputs.is_empty() {
            self.create_empty_output()?;
            return Ok(());
        }

        let final_path = self.output.clone();
        let final_compression = self.compression;
        let cluster = self.cluster.clone();
        let instance = self.instance.clone();
        let batch_ts = self.batch_ts;

        let final_file = std::fs::File::create(&final_path).with_context(|| {
            anyhow!(
                "Failed to create final parquet file: {}",
                final_path.display()
            )
        })?;

        let mut writer = Self::create_arrow_writer_for(final_compression, final_file)?;

        let mut summary = RecordsSummary::new(
            cluster.clone(),
            batch_ts.unix_timestamp_nanos() as i64,
            instance.clone(),
        );

        let readers = Self::open_run_readers(&self.inputs)?;

        let merge_iter = crate::helper::sort_merge::SortMergeIterator::new(readers)
            .context("Failed to initialize merge iterator")?;

        const BATCH_CAPACITY: usize = 8 * 1024;
        let mut batch_buf: Vec<crate::record::Record> = Vec::with_capacity(BATCH_CAPACITY);

        for res in merge_iter {
            let sortable = res.context("Failed during merging")?;
            let record = sortable.0;

            summary.update_from_record(&record);
            batch_buf.push(record.clone());

            if batch_buf.len() >= BATCH_CAPACITY {
                Self::flush_batch_buf(&mut writer, &cluster, batch_ts, &instance, &mut batch_buf)?;
            }
        }

        if !batch_buf.is_empty() {
            Self::flush_batch_buf(&mut writer, &cluster, batch_ts, &instance, &mut batch_buf)?;
        }

        let msgpack = rmp_serde::to_vec_named(&summary).context("Failed to serialize summary")?;
        let b64 = base64::engine::general_purpose::STANDARD.encode(&msgpack);

        writer.append_key_value_metadata(KeyValue {
            key: "rdbinsight.meta.v1.summary.msgpack.b64".to_string(),
            value: Some(b64),
        });

        writer
            .close()
            .map_err(|e| anyhow!("Failed to close final parquet writer: {e}"))?;

        for seg in self.inputs {
            let _ = std::fs::remove_file(&seg);
        }

        Ok(())
    }

    fn create_empty_output(&self) -> AnyResult<()> {
        let file = std::fs::File::create(&self.output).with_context(|| {
            anyhow!(
                "Failed to create empty parquet file: {}",
                self.output.display()
            )
        })?;
        let writer = Self::create_arrow_writer_for(self.compression, file)?;
        writer
            .close()
            .map_err(|e| anyhow!("Failed to close empty parquet writer: {e}"))?;
        Ok(())
    }

    fn create_arrow_writer_for(
        compression: ParquetCompression,
        file: std::fs::File,
    ) -> AnyResult<ArrowWriter<std::fs::File>> {
        let schema_arc = Arc::new(schema::create_redis_record_schema());
        let sorting_columns = schema::create_db_key_sorting_columns(&schema_arc)
            .map_err(|e| anyhow!("Failed to create sorting columns: {e}"))?;

        let mut props_builder = match compression {
            ParquetCompression::Zstd => WriterProperties::builder().set_compression(
                parquet::basic::Compression::ZSTD(parquet::basic::ZstdLevel::default()),
            ),
            ParquetCompression::Snappy => {
                WriterProperties::builder().set_compression(parquet::basic::Compression::SNAPPY)
            }
            ParquetCompression::None => WriterProperties::builder()
                .set_compression(parquet::basic::Compression::UNCOMPRESSED),
            ParquetCompression::Lz4 => {
                WriterProperties::builder().set_compression(parquet::basic::Compression::LZ4)
            }
        };
        props_builder = props_builder.set_sorting_columns(Some(sorting_columns));
        props_builder = props_builder.set_max_row_group_size(8192);
        let props = props_builder.build();

        let writer = ArrowWriter::try_new(file, schema_arc, Some(props))
            .map_err(|e| anyhow!("Failed to create ArrowWriter: {e}"))?;
        Ok(writer)
    }

    fn open_run_readers(inputs: &[PathBuf]) -> AnyResult<Vec<RunReader>> {
        let mut readers = Vec::with_capacity(inputs.len());
        for path in inputs {
            let r = RunReader::open(path).with_context(|| {
                anyhow!("Failed to open run file for reading: {}", path.display())
            })?;
            readers.push(r);
        }
        Ok(readers)
    }

    fn flush_batch_buf(
        writer: &mut ArrowWriter<std::fs::File>,
        cluster: &str,
        batch_ts: time::OffsetDateTime,
        instance: &str,
        batch_buf: &mut Vec<crate::record::Record>,
    ) -> AnyResult<()> {
        if batch_buf.is_empty() {
            return Ok(());
        }
        let batch = records_to_columns(cluster, batch_ts, instance, batch_buf)
            .map_err(|e| anyhow!("Failed to convert records to columns: {e}"))?;
        writer
            .write(&batch)
            .map_err(|e| anyhow!("Failed to write merged batch: {e}"))?;
        batch_buf.clear();
        Ok(())
    }
}

/// Tuple wrapper for `Record` that implements ordering by `(db asc, key asc)`.
#[derive(Debug)]
pub struct SortableRecord(pub crate::record::Record);

impl PartialEq for SortableRecord {
    fn eq(&self, other: &Self) -> bool {
        self.0.db == other.0.db && self.0.key == other.0.key
    }
}

impl Eq for SortableRecord {}

impl PartialOrd for SortableRecord {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortableRecord {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.0.db.cmp(&other.0.db) {
            std::cmp::Ordering::Equal => {}
            non_eq => return non_eq,
        };
        self.0.key.cmp(&other.0.key)
    }
}

impl Iterator for RunReader {
    type Item = AnyResult<SortableRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_next() {
            Ok(Some(rec)) => Some(Ok(SortableRecord(rec))),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}
