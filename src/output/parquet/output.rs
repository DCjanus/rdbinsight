use std::path::PathBuf;

use anyhow::{Context, anyhow};
use itertools::Itertools;
use parquet::{arrow::async_writer::AsyncArrowWriter, file::properties::WriterProperties};
use tokio::fs::File;
use tracing::{debug, error, info};

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
    merge_fan_in: usize,
}

impl ParquetOutput {
    pub fn new(
        base_dir: PathBuf,
        compression: ParquetCompression,
        run_rows: usize,
        intermediate_compression: ParquetCompression,
        merge_fan_in: usize,
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
            merge_fan_in,
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
            sanitized_instance.clone(),
            self.cluster.clone(),
            self.batch_ts,
            self.temp_batch_dir(),
            final_path,
            self.compression,
            self.run_rows,
            self.intermediate_compression,
            self.merge_fan_in,
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
            merge_fan_in = self.merge_fan_in,
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
    instance_sanitized: String,
    cluster: String,
    batch_ts: time::OffsetDateTime,
    temp_batch_dir: PathBuf,
    final_path: PathBuf,
    run_rows: usize,
    intermediate_compression: ParquetCompression,
    run_buffer: std::collections::BTreeMap<SortKey, crate::record::Record>,
    final_compression: ParquetCompression,
    issuer: u64,
    candidates: Vec<PathBuf>,
    merge_fan_in: usize,
}

impl ParquetChunkWriter {
    #[allow(clippy::too_many_arguments)]
    async fn new(
        instance: String,
        instance_sanitized: String,
        cluster: String,
        batch_ts: time::OffsetDateTime,
        temp_batch_dir: PathBuf,
        final_path: PathBuf,
        compression: ParquetCompression,
        run_rows: usize,
        intermediate_compression: ParquetCompression,
        merge_fan_in: usize,
    ) -> AnyResult<Self> {
        Ok(Self {
            instance,
            instance_sanitized,
            cluster,
            batch_ts,
            temp_batch_dir,
            final_path,
            run_rows,
            intermediate_compression,
            run_buffer: std::collections::BTreeMap::new(),
            final_compression: compression,
            issuer: 0,
            candidates: Vec::new(),
            merge_fan_in,
        })
    }

    fn next_idx(&mut self) -> u64 {
        let current = self.issuer;
        self.issuer += 1;
        current
    }

    async fn flush_run_segment(&mut self) -> AnyResult<()> {
        if self.run_buffer.is_empty() {
            return Ok(());
        }

        let idx = self.next_idx();
        let segment_path = self
            .temp_batch_dir
            .join(path::run_filename(&self.instance_sanitized, idx));

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

        let records = self.run_buffer.values().cloned().collect_vec();
        self.run_buffer.clear();

        for chunk in records.chunks(8192) {
            let batch =
                mapper::records_to_columns(&self.cluster, self.batch_ts, &self.instance, chunk)
                    .context("failed to convert run records to columns")?;
            run_writer.write(&batch).await.with_context(|| {
                anyhow!(
                    "Failed to write run segment parquet file: {}",
                    segment_path.display()
                )
            })?;
        }

        run_writer.close().await.with_context(|| {
            format!(
                "Failed to close run segment parquet writer for file: {}",
                segment_path.display()
            )
        })?;

        // Track candidate for rolling merge
        self.candidates.push(segment_path.clone());

        debug!(
            operation = "parquet_run_flushed",
            instance = %self.instance,
            segment = %segment_path.file_name().and_then(|s| s.to_str()).unwrap_or("<unknown>"),
            rows = records.len(),
            compression = ?self.intermediate_compression,
            "Flushed sorted run segment"
        );

        Ok(())
    }
}

#[async_trait::async_trait]
impl ChunkWriter for ParquetChunkWriter {
    async fn write_record(&mut self, record: crate::record::Record) -> AnyResult<()> {
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

        // Flush tail run segment if any
        if !this.run_buffer.is_empty() {
            this.flush_run_segment().await?;
        }

        // Stage 3 orchestration: rolling F-way merge until one output remains
        let fan_in: usize = this.merge_fan_in;

        // If there is no candidate (no data), create an empty final parquet
        if this.candidates.is_empty() {
            let merge_ctx = merge::MergeContext {
                inputs: Vec::new(),
                output: this.final_path.clone(),
                compression: this.final_compression,
                cluster: this.cluster.clone(),
                instance: this.instance.clone(),
                batch_ts: this.batch_ts,
            };
            merge_ctx.merge_once_delete_inputs_on_success().await?;
            return Ok(());
        }

        // Helper to sort candidates by lexicographic order (6-digit zero-padded idx ensures correct order)
        let sort_candidates = |cands: &mut Vec<PathBuf>| {
            cands.sort();
        };

        // Rolling merges for intermediate outputs until candidates <= fan_in
        sort_candidates(&mut this.candidates);
        while this.candidates.len() > fan_in {
            // Take the smallest fan_in candidates
            let mut inputs: Vec<PathBuf> = Vec::with_capacity(fan_in);
            for _ in 0..fan_in {
                inputs.push(this.candidates.remove(0));
            }

            let next_idx = this.next_idx();
            let out_path = this
                .temp_batch_dir
                .join(path::run_filename(&this.instance_sanitized, next_idx));

            let min_file = inputs
                .first()
                .and_then(|x| x.file_name())
                .and_then(|x| x.to_str())
                .ok_or_else(|| anyhow!("no input files"))?
                .to_string();
            let max_file = inputs
                .last()
                .and_then(|x| x.file_name())
                .and_then(|x| x.to_str())
                .ok_or_else(|| anyhow!("no input files"))?
                .to_string();
            let out_file = out_path
                .file_name()
                .and_then(|x| x.to_str())
                .ok_or_else(|| anyhow!("no output file"))?
                .to_string();
            let begin = std::time::Instant::now();

            let merge_ctx = merge::MergeContext {
                inputs,
                output: out_path.clone(),
                compression: this.intermediate_compression,
                cluster: this.cluster.clone(),
                instance: this.instance.clone(),
                batch_ts: this.batch_ts,
            };
            merge_ctx
                .merge_once_delete_inputs_on_success()
                .await
                .context("merge parquets failed")?;

            let cost = begin.elapsed();
            info!(
                operation = "parquet_rolling_merge_done",
                instance = %this.instance,
                fan_in = fan_in,
                min_segment = %min_file,
                max_segment = %max_file,
                output_segment = %out_file,
                cost =? cost,
            );

            // Add new output back to candidates and resort
            this.candidates.push(out_path);
            sort_candidates(&mut this.candidates);
        }

        // Final merge to <instance>.parquet using final compression
        let merge_ctx = merge::MergeContext {
            inputs: this.candidates,
            output: this.final_path.clone(),
            compression: this.final_compression,
            cluster: this.cluster.clone(),
            instance: this.instance.clone(),
            batch_ts: this.batch_ts,
        };

        if let Err(e) = merge_ctx.merge_once_delete_inputs_on_success().await {
            error!(
                operation = "parquet_final_merge_failed",
                instance = %this.instance,
                output = %this.final_path.display(),
                error = %e,
                "Final merge failed"
            );
            return Err(e);
        }

        Ok(())
    }
}
