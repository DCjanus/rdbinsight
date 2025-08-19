use std::{collections::HashMap, path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use parquet::{arrow::async_writer::AsyncArrowWriter, file::properties::WriterProperties};
use tokio::fs::File;
use tracing::{info, warn};

use self::{mapper::record_to_columns, path::*};
use crate::{config::ParquetCompression, record::Record};

pub mod mapper;
pub mod path;
pub mod schema;
pub mod v2;

/// Handle for an active Parquet writer
struct WriterHandle {
    writer: AsyncArrowWriter<File>,
    instance: String,
    temp_path: PathBuf,
    final_path: PathBuf,
}

impl WriterHandle {
    async fn new(
        instance: String,
        temp_path: PathBuf,
        final_path: PathBuf,
        compression: ParquetCompression,
    ) -> Result<Self> {
        let file = File::create(&temp_path).await.with_context(|| {
            format!(
                "Failed to create temp parquet file: {}",
                temp_path.display()
            )
        })?;

        let schema = Arc::new(schema::create_redis_record_schema());

        // Configure compression
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
            .map_err(|e| anyhow::anyhow!("Failed to create async arrow writer: {e}"))?;

        info!(
            operation = "parquet_writer_open",
            instance = %instance,
            temp_path = %temp_path.display(),
            final_path = %final_path.display(),
            compression = ?compression,
            "Opened Parquet writer"
        );

        Ok(Self {
            writer,
            instance,
            temp_path,
            final_path,
        })
    }

    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        self.writer.write(&batch).await.with_context(|| {
            format!(
                "Failed to write batch to parquet file {temp_path} for instance: {instance}",
                temp_path = self.temp_path.display(),
                instance = self.instance
            )
        })?;
        Ok(())
    }

    async fn close(self) -> Result<()> {
        info!(
            operation = "parquet_writer_closing",
            instance = %self.instance,
            temp_path = %self.temp_path.display(),
            final_path = %self.final_path.display(),
            "Closing Parquet writer"
        );

        self.writer.close().await.with_context(|| {
            format!(
                "Failed to close parquet writer for file {temp_path} for instance: {instance}",
                temp_path = self.temp_path.display(),
                instance = self.instance
            )
        })?;

        // Atomically rename from temp to final
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

/// Parquet output handler that manages per-instance writers
pub struct ParquetOutput {
    compression: ParquetCompression,
    writers: HashMap<String, WriterHandle>,
    pub(crate) temp_batch_dir: PathBuf,
    pub(crate) final_batch_dir: PathBuf,
}

impl ParquetOutput {
    /// Create a new ParquetOutput instance
    pub async fn new(
        dir: PathBuf,
        compression: ParquetCompression,
        cluster: &str,
        batch_ts: time::OffsetDateTime,
    ) -> Result<Self> {
        // Generate batch directory names
        let batch_dir_name = format_batch_dir(batch_ts);
        let temp_batch_dir_name = make_tmp_batch_dir(&batch_dir_name);

        let temp_batch_dir = dir.join(cluster).join(temp_batch_dir_name);
        let final_batch_dir = dir.join(cluster).join(batch_dir_name);

        // Create the temporary batch directory
        ensure_dir(&temp_batch_dir).await.with_context(|| {
            format!(
                "Failed to create temp batch directory: {}",
                temp_batch_dir.display()
            )
        })?;

        info!(
            operation = "parquet_batch_dir_created",
            cluster = %cluster,
            temp_batch_dir = %temp_batch_dir.display(),
            final_batch_dir = %final_batch_dir.display(),
            "Created temporary batch directory"
        );

        Ok(Self {
            compression,
            writers: HashMap::new(),
            temp_batch_dir,
            final_batch_dir,
        })
    }

    /// Write records for a specific instance
    pub async fn write(
        &mut self,
        records: &[Record],
        cluster: &str,
        batch_ts: time::OffsetDateTime,
        instance: &str,
    ) -> Result<()> {
        // Get or create writer for this instance
        if !self.writers.contains_key(instance) {
            let sanitized_instance = sanitize_instance_filename(instance);
            let temp_filename = format!("{sanitized_instance}.parquet.tmp");
            let final_filename = format!("{sanitized_instance}.parquet");

            let temp_path = self.temp_batch_dir.join(temp_filename);
            let final_path = self.temp_batch_dir.join(final_filename);

            let writer = WriterHandle::new(
                instance.to_string(),
                temp_path,
                final_path,
                self.compression,
            )
            .await
            .with_context(|| format!("Failed to create Parquet writer for instance: {instance}"))?;

            self.writers.insert(instance.to_string(), writer);
        }

        // Convert records to Arrow RecordBatch
        let record_batch =
            record_to_columns(records, cluster, batch_ts, instance).with_context(|| {
                format!(
                    "Failed to convert {records_count} records to columns for instance: {instance}",
                    records_count = records.len()
                )
            })?;

        // Write to the instance's writer
        let writer = self.writers.get_mut(instance).unwrap();
        writer.write_batch(record_batch).await
            .with_context(|| format!("Failed to write batch of {records_count} records to Parquet for instance: {instance}", records_count = records.len()))?;

        info!(
            operation = "parquet_batch_written",
            instance = %instance,
            records_count = records.len(),
            "Batch written to Parquet"
        );

        Ok(())
    }

    /// Write records from a Chunk
    pub async fn write_chunk(&mut self, chunk: crate::output::types::Chunk) -> Result<()> {
        let instance = &chunk.instance;

        // Get or create writer for this instance
        if !self.writers.contains_key(instance) {
            let sanitized_instance = sanitize_instance_filename(instance);
            let temp_filename = format!("{sanitized_instance}.parquet.tmp");
            let final_filename = format!("{sanitized_instance}.parquet");

            let temp_path = self.temp_batch_dir.join(temp_filename);
            let final_path = self.temp_batch_dir.join(final_filename);

            let writer =
                WriterHandle::new(instance.clone(), temp_path, final_path, self.compression)
                    .await
                    .with_context(|| {
                        format!("Failed to create Parquet writer for instance: {instance}")
                    })?;

            self.writers.insert(instance.clone(), writer);
        }

        // Convert records to Arrow RecordBatch
        let record_batch = mapper::chunk_to_columns(&chunk).with_context(|| {
            format!(
                "Failed to convert {records_count} records to columns for instance: {instance}",
                records_count = chunk.records.len()
            )
        })?;

        // Write to the instance's writer
        let writer = self.writers.get_mut(instance).unwrap();
        writer.write_batch(record_batch).await
            .with_context(|| format!("Failed to write batch of {records_count} records to Parquet for instance: {instance}", records_count = chunk.records.len()))?;

        info!(
            operation = "parquet_batch_written",
            instance = %instance,
            records_count = chunk.records.len(),
            "Batch written to Parquet"
        );

        Ok(())
    }

    /// Finalize a specific instance (close writer and rename file)
    pub async fn finalize_instance(&mut self, instance: &str) -> Result<()> {
        if let Some(writer) = self.writers.remove(instance) {
            writer.close().await?;

            info!(
                operation = "parquet_instance_finalized",
                instance = %instance,
                "Instance Parquet writer finalized"
            );
        } else {
            warn!(
                operation = "parquet_instance_not_found",
                instance = %instance,
                "Attempted to finalize instance that was not found"
            );
        }

        Ok(())
    }

    /// Finalize all remaining instances and rename batch directory
    pub async fn finalize_batch(mut self) -> Result<()> {
        // Close all remaining writers
        for (instance, writer) in self.writers.drain() {
            info!(
                operation = "parquet_writer_finalizing",
                instance = %instance,
                "Finalizing remaining Parquet writer during batch finalization"
            );
            writer.close().await.with_context(|| {
                format!("Failed to close writer during batch finalization for instance: {instance}")
            })?;
        }

        info!(
            operation = "parquet_batch_dir_renaming",
            temp_batch_dir = %self.temp_batch_dir.display(),
            final_batch_dir = %self.final_batch_dir.display(),
            "Renaming temporary batch directory to final"
        );

        // Rename batch directory from tmp_ to final
        tokio::fs::rename(&self.temp_batch_dir, &self.final_batch_dir)
            .await
            .with_context(|| {
                format!(
                    "Failed to rename batch directory from {} to {} (ensure no other process is accessing these files)",
                    self.temp_batch_dir.display(),
                    self.final_batch_dir.display()
                )
            })?;

        info!(
            operation = "parquet_batch_finalized",
            temp_batch_dir = %self.temp_batch_dir.display(),
            final_batch_dir = %self.final_batch_dir.display(),
            "Batch finalized and directory renamed"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use tempfile::TempDir;
    use time::OffsetDateTime;

    use super::*;
    use crate::{
        config::ParquetCompression,
        parser::{core::raw::RDBStr, model::StringEncoding},
        record::{Record, RecordEncoding, RecordType},
    };

    fn test_batch_ts() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp_nanos(1691999696123456789).unwrap()
    }

    fn create_test_record(db: u64, key: &str) -> Record {
        Record::builder()
            .db(db)
            .key(RDBStr::Str(Bytes::from(key.to_string())))
            .r#type(RecordType::String)
            .encoding(RecordEncoding::String(StringEncoding::Raw))
            .rdb_size(100)
            .member_count(Some(1))
            .expire_at_ms(Some(1691999999000))
            .idle_seconds(Some(300))
            .freq(Some(5))
            .codis_slot(Some(256))
            .redis_slot(Some(8192))
            .build()
    }

    #[tokio::test]
    async fn test_parquet_output_write_single_instance() {
        let temp_dir = TempDir::new().unwrap();

        let mut parquet_output = ParquetOutput::new(
            temp_dir.path().to_path_buf(),
            ParquetCompression::None,
            "test-cluster",
            test_batch_ts(),
        )
        .await
        .unwrap();

        let instance = "127.0.0.1:6379";
        let records = vec![create_test_record(0, "key1"), create_test_record(1, "key2")];

        // Write records
        parquet_output
            .write(&records, "test-cluster", test_batch_ts(), instance)
            .await
            .unwrap();

        // Store paths before finalization (since finalize_batch consumes the struct)
        let temp_batch_dir = parquet_output.temp_batch_dir.clone();
        let final_batch_dir = parquet_output.final_batch_dir.clone();

        // Finalize instance
        parquet_output.finalize_instance(instance).await.unwrap();

        // Finalize batch
        parquet_output.finalize_batch().await.unwrap();

        // Check that final files exist
        let final_instance_file = final_batch_dir.join("127.0.0.1-6379.parquet");

        assert!(final_batch_dir.exists());
        assert!(final_instance_file.exists());

        // Check that temp files are gone - temp directory should be renamed away
        assert!(!temp_batch_dir.exists());
    }

    #[tokio::test]
    async fn test_parquet_output_write_multiple_instances() {
        let temp_dir = TempDir::new().unwrap();

        let mut parquet_output = ParquetOutput::new(
            temp_dir.path().to_path_buf(),
            ParquetCompression::Zstd,
            "test-cluster",
            test_batch_ts(),
        )
        .await
        .unwrap();

        let instance1 = "127.0.0.1:6379";
        let instance2 = "127.0.0.1:6380";

        let records1 = vec![create_test_record(0, "key1")];
        let records2 = vec![create_test_record(1, "key2")];

        // Write to both instances
        parquet_output
            .write(&records1, "test-cluster", test_batch_ts(), instance1)
            .await
            .unwrap();
        parquet_output
            .write(&records2, "test-cluster", test_batch_ts(), instance2)
            .await
            .unwrap();

        // Store paths before finalization
        let final_batch_dir = parquet_output.final_batch_dir.clone();

        // Finalize instances individually
        parquet_output.finalize_instance(instance1).await.unwrap();
        parquet_output.finalize_instance(instance2).await.unwrap();

        // Finalize batch
        parquet_output.finalize_batch().await.unwrap();

        // Check that both final files exist
        let final_instance1_file = final_batch_dir.join("127.0.0.1-6379.parquet");
        let final_instance2_file = final_batch_dir.join("127.0.0.1-6380.parquet");

        assert!(final_batch_dir.exists());
        assert!(final_instance1_file.exists());
        assert!(final_instance2_file.exists());
    }

    #[tokio::test]
    async fn test_parquet_output_multiple_batches_same_instance() {
        let temp_dir = TempDir::new().unwrap();

        let mut parquet_output = ParquetOutput::new(
            temp_dir.path().to_path_buf(),
            ParquetCompression::Snappy,
            "test-cluster",
            test_batch_ts(),
        )
        .await
        .unwrap();

        let instance = "127.0.0.1:6379";

        // Write multiple batches to same instance
        let records1 = vec![create_test_record(0, "key1")];
        let records2 = vec![create_test_record(1, "key2")];

        parquet_output
            .write(&records1, "test-cluster", test_batch_ts(), instance)
            .await
            .unwrap();
        parquet_output
            .write(&records2, "test-cluster", test_batch_ts(), instance)
            .await
            .unwrap();

        // Store paths before finalization
        let final_batch_dir = parquet_output.final_batch_dir.clone();

        // Finalize
        parquet_output.finalize_instance(instance).await.unwrap();
        parquet_output.finalize_batch().await.unwrap();

        // Verify final file exists and temp files are gone
        let final_instance_file = final_batch_dir.join("127.0.0.1-6379.parquet");

        assert!(final_batch_dir.exists());
        assert!(final_instance_file.exists());
    }

    #[tokio::test]
    async fn test_parquet_output_finalize_nonexistent_instance() {
        let temp_dir = TempDir::new().unwrap();

        let mut parquet_output = ParquetOutput::new(
            temp_dir.path().to_path_buf(),
            ParquetCompression::None,
            "test-cluster",
            test_batch_ts(),
        )
        .await
        .unwrap();

        // Try to finalize an instance that doesn't exist - should not error
        let result = parquet_output.finalize_instance("nonexistent:6379").await;
        assert!(result.is_ok());

        // Batch finalization should still work
        parquet_output.finalize_batch().await.unwrap();
    }

    #[tokio::test]
    async fn test_directory_creation() {
        let temp_dir = TempDir::new().unwrap();

        // Test that nested directories are created properly
        let parquet_output = ParquetOutput::new(
            temp_dir.path().to_path_buf(),
            ParquetCompression::None,
            "test-cluster",
            test_batch_ts(),
        )
        .await
        .unwrap();

        // Check that the temp batch directory exists (use the actual path from the struct)
        assert!(parquet_output.temp_batch_dir.exists());
        assert!(parquet_output.temp_batch_dir.is_dir());
    }
}
