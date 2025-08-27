use std::path::PathBuf;

use anyhow::{Context, anyhow};
use arrow::record_batch::RecordBatch;
use parquet::{arrow::async_writer::AsyncArrowWriter, file::properties::WriterProperties};
use tokio::fs::File;
use tracing::info;

use crate::{
    config::ParquetCompression,
    helper::AnyResult,
    output::{
        ChunkWriter, ChunkWriterEnum, Output,
        parquet::{mapper, path},
    },
};

pub struct ParquetOutput {
    base_dir: PathBuf,
    compression: ParquetCompression,
    cluster: String,
    batch_ts: time::OffsetDateTime,
}

impl ParquetOutput {
    pub fn new(
        base_dir: PathBuf,
        compression: ParquetCompression,
        cluster: String,
        batch_ts: time::OffsetDateTime,
    ) -> Self {
        Self {
            base_dir,
            compression,
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
            self.cluster.clone(),
            self.batch_ts,
            temp_path,
            final_path,
            self.compression,
        )
        .await
        .with_context(|| format!("Failed to create Parquet writer for instance: {instance}"))?;

        Ok(ChunkWriterEnum::Parquet(Box::new(writer)))
    }

    async fn finalize_batch(self: Box<Self>) -> AnyResult<()> {
        let temp_batch_dir = self.temp_batch_dir();
        let final_batch_dir = self.final_batch_dir();

        path::finalize_batch_dir(&temp_batch_dir, &final_batch_dir).await?;

        info!(
            operation = "parquet_batch_finalize",
            cluster = %self.cluster,
            temp_batch_dir = %temp_batch_dir.display(),
            final_batch_dir = %final_batch_dir.display(),
            "Finalized batch directory via atomic rename"
        );
        Ok(())
    }
}

const MICRO_BATCH_ROWS: usize = 8192;

pub struct ParquetChunkWriter {
    writer: Option<AsyncArrowWriter<File>>,
    instance: String,
    cluster: String,
    batch_ts: time::OffsetDateTime,
    temp_path: PathBuf,
    final_path: PathBuf,
    buffered_records: Vec<crate::record::Record>,
}

impl ParquetChunkWriter {
    async fn new(
        instance: String,
        cluster: String,
        batch_ts: time::OffsetDateTime,
        temp_path: PathBuf,
        final_path: PathBuf,
        compression: ParquetCompression,
    ) -> AnyResult<Self> {
        let file = File::create(&temp_path).await.with_context(|| {
            format!(
                "Failed to create temp parquet file: {}",
                temp_path.display()
            )
        })?;

        let schema = std::sync::Arc::new(super::schema::create_redis_record_schema());

        let props = match compression {
            ParquetCompression::Zstd => WriterProperties::builder()
                .set_compression(parquet::basic::Compression::ZSTD(
                    parquet::basic::ZstdLevel::default(),
                ))
                .build(),
            ParquetCompression::Snappy => WriterProperties::builder()
                .set_compression(parquet::basic::Compression::SNAPPY)
                .build(),
            ParquetCompression::None => WriterProperties::builder()
                .set_compression(parquet::basic::Compression::UNCOMPRESSED)
                .build(),
            ParquetCompression::Lz4 => WriterProperties::builder()
                .set_compression(parquet::basic::Compression::LZ4)
                .build(),
        };

        let writer = AsyncArrowWriter::try_new(file, schema, Some(props))
            .map_err(|e| anyhow!("Failed to create async arrow writer: {e}"))?;

        Ok(Self {
            writer: Some(writer),
            instance,
            cluster,
            batch_ts,
            temp_path,
            final_path,
            buffered_records: Vec::with_capacity(MICRO_BATCH_ROWS),
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
}

#[async_trait::async_trait]
impl ChunkWriter for ParquetChunkWriter {
    async fn write_record(&mut self, record: crate::record::Record) -> AnyResult<()> {
        self.buffered_records.push(record);
        if self.buffered_records.len() >= MICRO_BATCH_ROWS {
            self.flush_buffer().await?;
        }
        Ok(())
    }

    async fn finalize_instance(self) -> AnyResult<()> {
        let mut this = self;

        if !this.buffered_records.is_empty() {
            this.flush_buffer().await?;
        }

        if let Some(writer) = this.writer {
            writer.close().await.with_context(|| {
                format!(
                    "Failed to close parquet writer for file {temp_path} for instance: {instance}",
                    temp_path = this.temp_path.display(),
                    instance = this.instance
                )
            })?;
        }

        tokio::fs::rename(&this.temp_path, &this.final_path)
            .await
            .with_context(|| {
                format!(
                    "Failed to rename parquet file from {} to {} for instance: {}",
                    this.temp_path.display(),
                    this.final_path.display(),
                    this.instance
                )
            })?;
        Ok(())
    }
}
