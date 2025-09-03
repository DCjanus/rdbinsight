use std::path::PathBuf;

use anyhow::{Context, anyhow};
use itertools::Itertools;
use tracing::{debug, info};

use crate::{
    config::ParquetCompression,
    helper::AnyResult,
    output::{
        ChunkWriter, ChunkWriterEnum, Output,
        parquet::{merge, path},
    },
};

pub struct ParquetOutput {
    base_dir: PathBuf,
    compression: ParquetCompression,
    max_run_rows: usize,
    cluster: String,
    batch_ts: time::OffsetDateTime,
}

impl ParquetOutput {
    pub fn new(
        base_dir: PathBuf,
        compression: ParquetCompression,
        max_run_rows: usize,
        cluster: String,
        batch_ts: time::OffsetDateTime,
    ) -> Self {
        Self {
            base_dir,
            compression,
            max_run_rows,

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
        let final_filename = path::final_instance_filename(&sanitized_instance);

        let final_path = self.temp_batch_dir().join(final_filename);

        let writer = ParquetChunkWriter::new(
            instance.to_string(),
            self.cluster.clone(),
            self.batch_ts,
            self.temp_batch_dir(),
            final_path,
            self.compression,
            self.max_run_rows,
        )
        .await
        .with_context(|| format!("Failed to create Parquet writer for instance: {instance}"))?;

        debug!(
            operation = "parquet_run_gen_started",
            cluster = %self.cluster,
            instance = %instance,
            instance_sanitized = %sanitized_instance,
            max_run_rows = self.max_run_rows,

            "Initialized Parquet run generation for instance"
        );

        Ok(ChunkWriterEnum::Parquet(Box::new(writer)))
    }

    async fn finalize_batch(self: Box<Self>) -> AnyResult<()> {
        let temp_batch_dir = self.temp_batch_dir();
        let final_batch_dir = self.final_batch_dir();

        let start: std::time::Instant = std::time::Instant::now();
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
    instance: String,
    cluster: String,
    batch_ts: time::OffsetDateTime,
    final_path: PathBuf,
    max_run_rows: usize,

    run_buffer: std::collections::BTreeMap<SortKey, crate::record::Record>,
    final_compression: ParquetCompression,
    run_file_path: PathBuf,
    run_file: tokio::fs::File,
    run_index: Vec<crate::output::parquet::run_lz4::ChunkDesc>,
}

impl ParquetChunkWriter {
    #[allow(clippy::too_many_arguments)]
    async fn new(
        instance: String,
        cluster: String,
        batch_ts: time::OffsetDateTime,
        temp_batch_dir: PathBuf,
        final_path: PathBuf,
        compression: ParquetCompression,
        max_run_rows: usize,
    ) -> AnyResult<Self> {
        let run_file_path = temp_batch_dir.join(format!("{}.run.lz4", instance));
        let run_file = tokio::fs::File::create(run_file_path.clone())
            .await
            .with_context(|| format!("Failed to create run file: {}", run_file_path.display()))?;

        Ok(Self {
            instance,
            cluster,
            batch_ts,
            final_path,
            max_run_rows,

            run_buffer: std::collections::BTreeMap::new(),
            final_compression: compression,
            run_file_path,
            run_file,
            run_index: Vec::new(),
        })
    }

    async fn flush_run_segment(&mut self) -> AnyResult<()> {
        if self.run_buffer.is_empty() {
            return Ok(());
        }

        let records = std::mem::take(&mut self.run_buffer)
            .into_values()
            .collect_vec();
        let rows = records.len();

        let desc = super::run_lz4::append_chunk(&mut self.run_file, records)
            .await
            .with_context(|| {
                format!(
                    "Failed to append chunk to run file: {}",
                    self.run_file_path.display()
                )
            })?;

        self.run_index.push(desc);

        debug!(
            operation = "parquet_run_lz4_flush_finished",
            instance = %self.instance,
            run_file = %self.run_file_path.file_name().and_then(|s| s.to_str()).unwrap_or("<unknown>"),
            rows = rows,
            "Flushed chunk into single run file"
        );

        Ok(())
    }
}

#[async_trait::async_trait]
impl ChunkWriter for ParquetChunkWriter {
    async fn write_record(&mut self, record: crate::record::Record) -> AnyResult<()> {
        let key = SortKey::from_record(&record);
        self.run_buffer.insert(key, record);
        if self.run_buffer.len() >= self.max_run_rows {
            self.flush_run_segment().await?;
        }
        Ok(())
    }

    async fn finalize_instance(self) -> AnyResult<()> {
        let mut this = self;

        if !this.run_buffer.is_empty() {
            this.flush_run_segment().await?;
        }

        super::run_lz4::write_index(&mut this.run_file, this.run_index)
            .await
            .with_context(|| {
                format!(
                    "Failed to write run index for {}",
                    this.run_file_path.display()
                )
            })?;

        let merge_ctx = merge::MergeContext {
            inputs: vec![this.run_file_path.clone()],
            output: this.final_path.clone(),
            compression: this.final_compression,
            cluster: this.cluster.clone(),
            instance: this.instance.clone(),
            batch_ts: this.batch_ts,
        };

        tokio::task::spawn_blocking(move || merge_ctx.merge())
            .await
            .with_context(|| {
                anyhow!(
                    "Failed to join final merge task for instance {}",
                    this.instance
                )
            })??;

        Ok(())
    }
}
