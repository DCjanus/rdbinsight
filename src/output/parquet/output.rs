use std::path::PathBuf;

use anyhow::{Context, anyhow};
use arrow::record_batch::RecordBatch;
use parquet::{arrow::async_writer::AsyncArrowWriter, file::properties::WriterProperties};
use tokio::fs::File;
use tracing::{debug, info};

use crate::{
    config::ParquetCompression,
    helper::AnyResult,
    output::{
        ChunkWriter, ChunkWriterEnum, Output,
        parquet::{mapper, merge, path},
    },
};

pub struct ParquetOutput {
    base_dir: PathBuf,
    compression: ParquetCompression,
    run_rows: usize,
    intermediate_compression: ParquetCompression,
    cluster: String,
    batch_ts: time::OffsetDateTime,
}

impl ParquetOutput {
    pub fn new(
        base_dir: PathBuf,
        compression: ParquetCompression,
        run_rows: usize,
        intermediate_compression: ParquetCompression,
        cluster: String,
        batch_ts: time::OffsetDateTime,
    ) -> Self {
        Self {
            base_dir,
            compression,
            run_rows,
            intermediate_compression,
            cluster,
            batch_ts,
        }
    }

    fn temp_batch_dir(&self) -> PathBuf {
        let batch_slug = path::format_batch_dir(self.batch_ts);
        let cluster_dir = path::cluster_dir_name(&self.cluster);
        let temp_batch_dir_name = path::temp_batch_dir_name(&batch_slug);
        self.base_dir.join(cluster_dir).join(temp_batch_dir_name)
    }

    fn final_batch_dir(&self) -> PathBuf {
        let batch_slug = path::format_batch_dir(self.batch_ts);
        let cluster_dir = path::cluster_dir_name(&self.cluster);
        let final_batch_dir_name = path::final_batch_dir_name(&batch_slug);
        self.base_dir.join(cluster_dir).join(final_batch_dir_name)
    }
}

#[async_trait::async_trait]
impl Output for ParquetOutput {
    async fn prepare_batch(&self) -> AnyResult<()> {
        let temp_batch_dir = self.temp_batch_dir();
        path::ensure_dir(&temp_batch_dir).await.with_context(|| {
            format!(
                "Failed to create temp batch directory: {}",
                temp_batch_dir.display()
            )
        })?;

        info!(
            operation = "parquet_batch_dir_created",
            cluster = %self.cluster,
            temp_batch_dir = %temp_batch_dir.display(),
            final_batch_dir = %self.final_batch_dir().display(),
            "Created temporary batch directory"
        );
        Ok(())
    }

    async fn create_writer(&self, instance: &str) -> AnyResult<ChunkWriterEnum> {
        let sanitized_instance = path::sanitize_instance_filename(instance);
        let temp_filename = format!("{sanitized_instance}.parquet.tmp");
        let final_filename = path::final_instance_filename(&sanitized_instance);

        let temp_path = self.temp_batch_dir().join(temp_filename);
        let final_path = self.temp_batch_dir().join(final_filename);

        let writer = ParquetChunkWriter::new(
            instance.to_string(),
            sanitized_instance.clone(),
            self.cluster.clone(),
            self.batch_ts,
            self.temp_batch_dir(),
            temp_path,
            final_path,
            self.compression,
            self.run_rows,
            self.intermediate_compression,
        )
        .await
        .with_context(|| format!("Failed to create Parquet writer for instance: {instance}"))?;

        debug!(
            operation = "parquet_run_gen_started",
            cluster = %self.cluster,
            instance = %instance,
            instance_sanitized = %sanitized_instance,
            run_rows = self.run_rows,
            intermediate_compression = ?self.intermediate_compression,
            micro_batch_rows = MICRO_BATCH_ROWS,
            "Initialized Parquet run generation for instance"
        );

        Ok(ChunkWriterEnum::Parquet(Box::new(writer)))
    }

    async fn finalize_batch(self: Box<Self>) -> AnyResult<()> {
        let temp_batch_dir = self.temp_batch_dir();
        let final_batch_dir = self.final_batch_dir();

        let start = std::time::Instant::now();
        path::finalize_batch_dir(&temp_batch_dir, &final_batch_dir).await?;
        let duration_ms = start.elapsed().as_millis() as u64;

        info!(
            operation = "parquet_batch_finalize",
            cluster = %self.cluster,
            temp_batch_dir = %temp_batch_dir.display(),
            final_batch_dir = %final_batch_dir.display(),
            rename_duration_ms = duration_ms,
            "Finalized batch directory via atomic rename"
        );
        Ok(())
    }
}

const MICRO_BATCH_ROWS: usize = 8192;

use std::collections::BTreeMap;

use crate::parser::core::raw::RDBStr;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct SortKey {
    db: i64,
    key: Vec<u8>,
}

impl SortKey {
    fn from_record(record: &crate::record::Record) -> Self {
        let key_bytes = match &record.key {
            RDBStr::Str(bytes) => bytes.to_vec(),
            RDBStr::Int(int_val) => int_val.to_string().into_bytes(),
        };
        Self {
            db: record.db as i64,
            key: key_bytes,
        }
    }
}

pub struct ParquetChunkWriter {
    writer: Option<AsyncArrowWriter<File>>,
    instance: String,
    instance_sanitized: String,
    cluster: String,
    batch_ts: time::OffsetDateTime,
    temp_batch_dir: PathBuf,
    temp_path: PathBuf,
    final_path: PathBuf,
    buffered_records: Vec<crate::record::Record>,
    // Run buffer (sorted) and controls
    run_rows: usize,
    intermediate_compression: ParquetCompression,
    run_index: usize,
    run_buffer: BTreeMap<SortKey, crate::record::Record>,
    final_compression: ParquetCompression,
}

impl ParquetChunkWriter {
    #[allow(clippy::too_many_arguments)]
    async fn new(
        instance: String,
        instance_sanitized: String,
        cluster: String,
        batch_ts: time::OffsetDateTime,
        temp_batch_dir: PathBuf,
        temp_path: PathBuf,
        final_path: PathBuf,
        compression: ParquetCompression,
        run_rows: usize,
        intermediate_compression: ParquetCompression,
    ) -> AnyResult<Self> {
        let file = File::create(&temp_path).await.with_context(|| {
            format!(
                "Failed to create temp parquet file: {}",
                temp_path.display()
            )
        })?;

        let schema = std::sync::Arc::new(super::schema::create_redis_record_schema());
        let mut builder = match compression {
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
        // Create sorting_columns by field name lookups
        let sorting_columns = super::schema::create_db_key_sorting_columns(&schema)
            .map_err(|e| anyhow!("Failed to resolve sorting columns: {e}"))?;
        builder = builder.set_sorting_columns(Some(sorting_columns));
        let props = builder.build();

        let writer = AsyncArrowWriter::try_new(file, schema, Some(props))
            .map_err(|e| anyhow!("Failed to create async arrow writer: {e}"))?;

        Ok(Self {
            writer: Some(writer),
            instance,
            instance_sanitized,
            cluster,
            batch_ts,
            temp_batch_dir,
            temp_path,
            final_path,
            buffered_records: Vec::with_capacity(MICRO_BATCH_ROWS),
            run_rows,
            intermediate_compression,
            run_index: 0,
            run_buffer: BTreeMap::new(),
            final_compression: compression,
        })
    }

    async fn write_batch(&mut self, batch: RecordBatch) -> AnyResult<()> {
        self.writer
            .as_mut()
            .expect("unexpected error: writer not initialized")
            .write(&batch)
            .await
            .with_context(|| {
                anyhow!(
                    "Failed to write batch to parquet file {temp_path} for instance: {instance}",
                    temp_path = self.temp_path.display(),
                    instance = self.instance
                )
            })?;
        Ok(())
    }

    async fn flush_buffer(&mut self) -> AnyResult<()> {
        if self.buffered_records.is_empty() {
            return Ok(());
        }
        let records_count = self.buffered_records.len();
        let instance = self.instance.clone();
        let record_batch = mapper::records_to_columns(
            &self.cluster,
            self.batch_ts,
            &self.instance,
            &self.buffered_records,
        )
        .with_context(|| {
            anyhow!(
                "Failed to convert {records_count} buffered records to columns for instance: {instance}"
            )
        })?;
        self.buffered_records.clear();
        self.write_batch(record_batch).await?;
        Ok(())
    }

    async fn flush_run_segment(&mut self) -> AnyResult<()> {
        if self.run_buffer.is_empty() {
            return Ok(());
        }

        let segment_filename = path::run_segment_filename(&self.instance_sanitized, self.run_index);
        let segment_path = self.temp_batch_dir.join(segment_filename);
        self.run_index += 1;

        let file = File::create(&segment_path).await.with_context(|| {
            format!(
                "Failed to create run segment parquet file: {}",
                segment_path.display()
            )
        })?;

        let schema = std::sync::Arc::new(super::schema::create_redis_record_schema());
        let sorting_columns = super::schema::create_db_key_sorting_columns(&schema)
            .map_err(|e| anyhow!("Failed to resolve sorting columns: {e}"))?;

        let props = {
            let mut builder =
                match self.intermediate_compression {
                    ParquetCompression::Zstd => WriterProperties::builder().set_compression(
                        parquet::basic::Compression::ZSTD(parquet::basic::ZstdLevel::default()),
                    ),
                    ParquetCompression::Snappy => WriterProperties::builder()
                        .set_compression(parquet::basic::Compression::SNAPPY),
                    ParquetCompression::None => WriterProperties::builder()
                        .set_compression(parquet::basic::Compression::UNCOMPRESSED),
                    ParquetCompression::Lz4 => WriterProperties::builder()
                        .set_compression(parquet::basic::Compression::LZ4),
                };
            // Mark run segments as sorted by (db ASC, key ASC)
            builder = builder.set_sorting_columns(Some(sorting_columns));
            builder.build()
        };

        let mut run_writer = AsyncArrowWriter::try_new(file, schema, Some(props))
            .map_err(|e| anyhow!("Failed to create async arrow run writer: {e}"))?;

        // Drain in-order records for this run
        let taken_map = std::mem::take(&mut self.run_buffer);
        let mut ordered_records: Vec<crate::record::Record> = Vec::with_capacity(taken_map.len());
        for (_k, v) in taken_map {
            ordered_records.push(v);
        }

        let record_batch = mapper::records_to_columns(
            &self.cluster,
            self.batch_ts,
            &self.instance,
            &ordered_records,
        )
        .with_context(|| {
            anyhow!(
                "Failed to convert {} run records to columns for instance: {}",
                ordered_records.len(),
                self.instance
            )
        })?;

        let start = std::time::Instant::now();
        run_writer.write(&record_batch).await.with_context(|| {
            anyhow!(
                "Failed to write run segment parquet file: {}",
                segment_path.display()
            )
        })?;

        run_writer.close().await.with_context(|| {
            format!(
                "Failed to close run segment parquet writer for file: {}",
                segment_path.display()
            )
        })?;
        let duration_ms = start.elapsed().as_millis() as u64;

        let segment_size_bytes = tokio::fs::metadata(&segment_path)
            .await
            .map(|m| m.len())
            .unwrap_or(0);

        debug!(
            operation = "parquet_run_flushed",
            instance = %self.instance,
            segment = %segment_path.file_name().and_then(|s| s.to_str()).unwrap_or("<unknown>"),
            rows = ordered_records.len(),
            compression = ?self.intermediate_compression,
            duration_ms = duration_ms,
            size_bytes = segment_size_bytes,
            "Flushed sorted run segment"
        );

        Ok(())
    }
}

#[async_trait::async_trait]
impl ChunkWriter for ParquetChunkWriter {
    async fn write_record(&mut self, record: crate::record::Record) -> AnyResult<()> {
        // Feed final-file micro-batch path (existing behaviour)
        self.buffered_records.push(record.clone());
        if self.buffered_records.len() >= MICRO_BATCH_ROWS {
            self.flush_buffer().await?;
        }

        // Feed run buffer (sorted by (db, key))
        let key = SortKey::from_record(&record);
        // On duplicate keys, newer record overwrites previous one. This should be rare/non-existent in typical RDB dumps.
        self.run_buffer.insert(key, record);
        if self.run_buffer.len() >= self.run_rows {
            self.flush_run_segment().await?;
        }
        Ok(())
    }

    async fn finalize_instance(self) -> AnyResult<()> {
        let mut this = self;

        if !this.buffered_records.is_empty() {
            this.flush_buffer().await?;
        }

        // Flush tail run segment if any
        if !this.run_buffer.is_empty() {
            this.flush_run_segment().await?;
        }

        // Ensure temp writer (micro-batch file) is closed before merge
        if let Some(writer) = this.writer.take() {
            writer.close().await.with_context(|| {
                format!(
                    "Failed to close parquet writer for file {temp_path} for instance: {instance}",
                    temp_path = this.temp_path.display(),
                    instance = this.instance
                )
            })?;
        }

        // Build merge context and drop non-Send writer before awaiting heavy merge
        let merge_ctx = merge::MergeContext {
            cluster: this.cluster.clone(),
            instance: this.instance.clone(),
            batch_ts: this.batch_ts,
            temp_batch_dir: this.temp_batch_dir.clone(),
            temp_path: this.temp_path.clone(),
            final_path: this.final_path.clone(),
            instance_sanitized: this.instance_sanitized.clone(),
            final_compression: this.final_compression,
            run_count: this.run_index,
        };

        // Explicitly drop `this` to ensure no non-Send fields are held across await
        drop(this);

        // Perform in-instance k-way merge of run segments into final parquet file
        merge_ctx.merge_run_segments_into_final().await?;

        // Best-effort cleanup: remove intermediate micro-batch temp file if still present
        let _ = tokio::fs::remove_file(&merge_ctx.temp_path).await;

        Ok(())
    }
}
