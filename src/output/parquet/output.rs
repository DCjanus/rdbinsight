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
        let batch_dir_name = path::format_batch_dir(self.batch_ts);
        let temp_batch_dir_name = path::make_tmp_batch_dir(&batch_dir_name);
        self.base_dir.join(&self.cluster).join(temp_batch_dir_name)
    }

    fn final_batch_dir(&self) -> PathBuf {
        let batch_dir_name = path::format_batch_dir(self.batch_ts);
        self.base_dir.join(&self.cluster).join(batch_dir_name)
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
        let final_filename = format!("{sanitized_instance}.parquet");

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

        info!(
            operation = "parquet_batch_dir_renaming",
            temp_batch_dir = %temp_batch_dir.display(),
            final_batch_dir = %final_batch_dir.display(),
            "Renaming temporary batch directory to final"
        );

        tokio::fs::rename(&temp_batch_dir, &final_batch_dir)
			.await
			.with_context(|| {
				format!(
					"Failed to rename batch directory from {} to {} (ensure no other process is accessing these files)",
					temp_batch_dir.display(),
					final_batch_dir.display()
				)
			})?;

        info!(
            operation = "parquet_batch_finalized",
            temp_batch_dir = %temp_batch_dir.display(),
            final_batch_dir = %final_batch_dir.display(),
            "Batch finalized and directory renamed"
        );
        Ok(())
    }
}

pub struct ParquetChunkWriter {
    writer: Option<AsyncArrowWriter<File>>,
    instance: String,
    cluster: String,
    batch_ts: time::OffsetDateTime,
    temp_path: PathBuf,
    final_path: PathBuf,
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
        };

        let writer = AsyncArrowWriter::try_new(file, schema, Some(props))
            .map_err(|e| anyhow!("Failed to create async arrow writer: {e}"))?;

        info!(
            operation = "parquet_writer_open",
            instance = %instance,
            temp_path = %temp_path.display(),
            final_path = %final_path.display(),
            compression = ?compression,
            "Opened Parquet writer"
        );

        Ok(Self {
            writer: Some(writer),
            instance,
            cluster,
            batch_ts,
            temp_path,
            final_path,
        })
    }

    async fn write_batch(&mut self, batch: RecordBatch) -> AnyResult<()> {
        match self.writer.as_mut() {
            Some(writer) => {
                writer.write(&batch).await.with_context(|| {
					anyhow!(
						"Failed to write batch to parquet file {temp_path} for instance: {instance}",
						temp_path = self.temp_path.display(),
						instance = self.instance
					)
				})?;
                Ok(())
            }
            None => Err(anyhow!(
                "Attempted to write after writer was closed for instance: {}",
                self.instance
            )),
        }
    }
}

#[async_trait::async_trait]
impl ChunkWriter for ParquetChunkWriter {
    async fn write_chunk(&mut self, chunk: crate::output::types::Chunk) -> AnyResult<()> {
        let records_count = chunk.records.len();
        let instance = &chunk.instance;
        let record_batch = mapper::chunk_to_columns(&chunk).with_context(|| {
            anyhow!("Failed to convert {records_count} records to columns for instance: {instance}")
        })?;
        self.write_batch(record_batch).await?;
        info!(
            operation = "parquet_batch_written",
            instance = %chunk.instance,
            records_count = chunk.records.len(),
            "Batch written to Parquet"
        );
        Ok(())
    }

    async fn write_record(&mut self, record: crate::record::Record) -> AnyResult<()> {
        let chunk = crate::output::types::Chunk {
            cluster: self.cluster.clone(),
            batch_ts: self.batch_ts,
            instance: self.instance.clone(),
            records: vec![record],
        };
        self.write_chunk(chunk).await
    }

    async fn finalize_instance(&mut self) -> AnyResult<()> {
        info!(
            operation = "parquet_writer_closing",
            instance = %self.instance,
            temp_path = %self.temp_path.display(),
            final_path = %self.final_path.display(),
            "Closing Parquet writer"
        );

        if let Some(writer) = self.writer.take() {
            writer.close().await.with_context(|| {
                format!(
                    "Failed to close parquet writer for file {temp_path} for instance: {instance}",
                    temp_path = self.temp_path.display(),
                    instance = self.instance
                )
            })?;
        }

        tokio::fs::rename(&self.temp_path, &self.final_path)
            .await
            .with_context(|| {
                format!(
                    "Failed to rename parquet file from {} to {} for instance: {}",
                    self.temp_path.display(),
                    self.final_path.display(),
                    self.instance
                )
            })?;

        info!(
            operation = "parquet_writer_closed",
            instance = %self.instance,
            temp_path = %self.temp_path.display(),
            final_path = %self.final_path.display(),
            "Parquet writer closed and file finalized"
        );
        Ok(())
    }
}
