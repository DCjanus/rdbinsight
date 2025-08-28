use std::{cmp::Reverse, collections::BinaryHeap, path::PathBuf, sync::Arc};

use anyhow::{Context, anyhow};
use arrow::{
    array::{
        Array, BinaryArray, BinaryBuilder, Int32Array, Int32Builder, Int64Array, Int64Builder,
        StringArray, StringBuilder, TimestampMillisecondArray, TimestampMillisecondBuilder,
        TimestampNanosecondBuilder,
    },
    record_batch::RecordBatch,
};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    file::properties::WriterProperties,
};
use tracing::info;

use crate::{
    config::ParquetCompression,
    helper::AnyResult,
    output::parquet::{path, schema},
};

/// Merge context containing all information needed for k-way merge
pub struct MergeContext {
    pub cluster: String,
    pub instance: String,
    pub batch_ts: time::OffsetDateTime,
    pub temp_batch_dir: PathBuf,
    pub temp_path: PathBuf,
    pub final_path: PathBuf,
    pub instance_sanitized: String,
    pub final_compression: ParquetCompression,
    pub run_count: usize,
}

impl MergeContext {
    /// Perform k-way merge to combine run segments into final Parquet file
    pub async fn merge_run_segments_into_final(&self) -> AnyResult<()> {
        if self.run_count == 0 {
            return self.handle_no_segments_case().await;
        }

        let segments = self.build_segment_paths();

        info!(
            operation = "parquet_instance_merge_started",
            cluster = %self.cluster,
            instance = %self.instance,
            run_count = self.run_count,
            compression = ?self.final_compression,
            "Starting in-instance k-way merge"
        );

        let merge_params = MergeParams::new(self, segments.clone());

        let start = std::time::Instant::now();
        let result = tokio::task::spawn_blocking(move || merge_params.perform_merge())
            .await
            .map_err(|e| anyhow!("Failed to join merge task: {e}"))?;

        result?;
        let duration_ms = start.elapsed().as_millis() as u64;
        self.cleanup_segments(segments).await?;
        self.log_completion(duration_ms);

        Ok(())
    }

    /// Handle the case where no run segments exist (rename temp file directly)
    async fn handle_no_segments_case(&self) -> AnyResult<()> {
        tokio::fs::rename(&self.temp_path, &self.final_path)
            .await
            .with_context(|| {
                format!(
                    "Failed to rename parquet file from {} to {} for instance: {} (no run segments found)",
                    self.temp_path.display(),
                    self.final_path.display(),
                    self.instance
                )
            })?;
        Ok(())
    }

    /// Build paths for all run segment files
    fn build_segment_paths(&self) -> Vec<PathBuf> {
        (0..self.run_count)
            .map(|idx| {
                let name = path::run_segment_filename(&self.instance_sanitized, idx);
                self.temp_batch_dir.join(name)
            })
            .collect()
    }

    /// Clean up run segment files
    async fn cleanup_segments(&self, segments: Vec<PathBuf>) -> AnyResult<()> {
        for seg in segments {
            let _ = tokio::fs::remove_file(&seg).await;
        }
        Ok(())
    }

    /// Log merge completion
    fn log_completion(&self, duration_ms: u64) {
        info!(
            operation = "parquet_instance_merge_completed",
            instance = %self.instance,
            final_path = %self.final_path.display(),
            duration_ms = duration_ms,
            "Merged run segments into final parquet file"
        );
    }
}

/// Merge parameters containing all data needed for merge operation in blocking task
struct MergeParams {
    cluster: String,
    instance: String,
    batch_ts: time::OffsetDateTime,
    final_path: PathBuf,
    final_compression: ParquetCompression,
    segments: Vec<PathBuf>,
}

impl MergeParams {
    fn new(ctx: &MergeContext, segments: Vec<PathBuf>) -> Self {
        Self {
            cluster: ctx.cluster.clone(),
            instance: ctx.instance.clone(),
            batch_ts: ctx.batch_ts,
            final_path: ctx.final_path.clone(),
            final_compression: ctx.final_compression,
            segments,
        }
    }

    /// Perform the actual merge operation
    fn perform_merge(self) -> AnyResult<()> {
        let merger = RunMerger::new(self)?;
        merger.perform_k_way_merge()
    }
}

/// Run segment merger responsible for specific k-way merge logic
struct RunMerger {
    cluster: String,
    instance: String,
    batch_ts: time::OffsetDateTime,
    cursors: Vec<RunCursor>,
    heap: BinaryHeap<Reverse<HeapItem>>,
    field_indices: ColumnIndices,
    builders: OutputBuilders,
    writer: ArrowWriter<std::fs::File>,
}

impl RunMerger {
    const OUTPUT_BATCH_ROWS: usize = 64 * 1024;

    fn new(params: MergeParams) -> AnyResult<Self> {
        let writer = Self::create_writer(&params.final_path, params.final_compression)?;
        let cursors = Self::create_cursors(&params.segments)?;
        let field_indices = ColumnIndices::new()?;
        let builders = OutputBuilders::with_capacity(Self::OUTPUT_BATCH_ROWS);

        let mut merger = Self {
            cluster: params.cluster,
            instance: params.instance,
            batch_ts: params.batch_ts,
            cursors,
            heap: BinaryHeap::new(),
            field_indices,
            builders,
            writer,
        };

        merger.initialize_heap()?;
        Ok(merger)
    }

    fn create_writer(
        final_path: &PathBuf,
        compression: ParquetCompression,
    ) -> AnyResult<ArrowWriter<std::fs::File>> {
        let final_file = std::fs::File::create(final_path).with_context(|| {
            format!(
                "Failed to create final parquet file: {}",
                final_path.display()
            )
        })?;

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
        let props = props_builder.build();

        ArrowWriter::try_new(final_file, schema_arc, Some(props))
            .map_err(|e| anyhow!("Failed to create ArrowWriter: {e}"))
    }

    fn create_cursors(segments: &[PathBuf]) -> AnyResult<Vec<RunCursor>> {
        let mut cursors = Vec::with_capacity(segments.len());
        for path in segments {
            let mut cursor = RunCursor::new(path)?;
            if cursor.ensure_batch()? {
                cursors.push(cursor);
            }
        }
        Ok(cursors)
    }

    fn initialize_heap(&mut self) -> AnyResult<()> {
        for (idx, cursor) in self.cursors.iter_mut().enumerate() {
            let (db, key) = cursor.peek_key(&self.field_indices)?;
            self.heap.push(Reverse(HeapItem {
                db,
                key,
                run_idx: idx,
            }));
        }
        Ok(())
    }

    fn perform_k_way_merge(mut self) -> AnyResult<()> {
        while let Some(item) = self.heap.pop() {
            self.process_heap_item(item)?;
            self.flush_if_full()?;
        }

        self.flush_remaining()?;
        self.writer
            .close()
            .map_err(|e| anyhow!("Failed to close final parquet writer: {e}"))?;
        Ok(())
    }

    fn process_heap_item(&mut self, item: Reverse<HeapItem>) -> AnyResult<()> {
        let cursor = &mut self.cursors[item.0.run_idx];
        cursor.append_current_row(
            &self.cluster,
            &self.instance,
            self.batch_ts,
            &self.field_indices,
            &mut self.builders,
        )?;
        self.builders.rows += 1;

        // If current cursor has more data, push back to heap
        if cursor.ensure_batch()? && cursor.has_more_rows() {
            let (db, key) = cursor.peek_key(&self.field_indices)?;
            self.heap.push(Reverse(HeapItem {
                db,
                key,
                run_idx: item.0.run_idx,
            }));
        }

        Ok(())
    }

    fn flush_if_full(&mut self) -> AnyResult<()> {
        if self.builders.is_full() {
            let batch = self.builders.finish_to_batch()?;
            self.builders = OutputBuilders::with_capacity(Self::OUTPUT_BATCH_ROWS);
            self.writer
                .write(&batch)
                .map_err(|e| anyhow!("Failed to write merged batch: {e}"))?;
        }
        Ok(())
    }

    fn flush_remaining(&mut self) -> AnyResult<()> {
        if !self.builders.is_empty() {
            let batch = self.builders.finish_to_batch()?;
            self.writer
                .write(&batch)
                .map_err(|e| anyhow!("Failed to write final merged batch: {e}"))?;
        }
        Ok(())
    }
}

/// Heap item for min-heap sorting
#[derive(Eq, PartialEq, Ord, PartialOrd)]
pub struct HeapItem {
    pub db: i64,
    pub key: Vec<u8>,
    pub run_idx: usize,
}

/// Run segment cursor for reading individual run segments
pub struct RunCursor {
    reader: parquet::arrow::arrow_reader::ParquetRecordBatchReader,
    current: Option<RecordBatch>,
    row: usize,
}

impl RunCursor {
    pub fn new(path: &PathBuf) -> AnyResult<Self> {
        let file = std::fs::File::open(path).with_context(|| {
            format!("Failed to open run segment for reading: {}", path.display())
        })?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).with_context(|| {
            format!(
                "Failed to create ParquetRecordBatchReaderBuilder for {}",
                path.display()
            )
        })?;
        let reader = builder.build().with_context(|| {
            format!(
                "Failed to build ParquetRecordBatchReader for {}",
                path.display()
            )
        })?;
        Ok(Self {
            reader,
            current: None,
            row: 0,
        })
    }

    pub fn ensure_batch(&mut self) -> AnyResult<bool> {
        if let Some(ref batch) = self.current
            && self.row < batch.num_rows()
        {
            return Ok(true);
        }

        match self.reader.next() {
            Some(Ok(batch)) => {
                self.current = Some(batch);
                self.row = 0;
                Ok(true)
            }
            Some(Err(e)) => Err(e.into()),
            None => Ok(false),
        }
    }

    pub fn has_more_rows(&self) -> bool {
        self.current.is_some() && self.row < self.current.as_ref().unwrap().num_rows()
    }

    pub fn peek_key(&self, indices: &ColumnIndices) -> AnyResult<(i64, Vec<u8>)> {
        let batch = self.current.as_ref().expect("batch should be present");

        let db_array = batch
            .column(indices.db)
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("Failed to downcast db column to Int64Array")?;

        let key_array = batch
            .column(indices.key)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .context("Failed to downcast key column to BinaryArray")?;

        Ok((db_array.value(self.row), key_array.value(self.row).to_vec()))
    }

    pub fn append_current_row(
        &mut self,
        cluster: &str,
        instance: &str,
        batch_ts: time::OffsetDateTime,
        indices: &ColumnIndices,
        builders: &mut OutputBuilders,
    ) -> AnyResult<()> {
        let batch = self.current.as_ref().expect("batch should be present");

        // Constant columns
        builders.cluster.append_value(cluster);
        builders
            .batch
            .append_value(batch_ts.unix_timestamp_nanos() as i64);
        builders.instance.append_value(instance);

        // db (required)
        let db_array = batch
            .column(indices.db)
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("Failed to downcast db column to Int64Array")?;
        builders.db.append_value(db_array.value(self.row));

        // key (required)
        let key_array = batch
            .column(indices.key)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .context("Failed to downcast key column to BinaryArray")?;
        builders.key.append_value(key_array.value(self.row));

        // type (required Utf8)
        let type_array = batch
            .column(indices.r#type)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("Failed to downcast type column to StringArray")?;
        builders.r#type.append_value(type_array.value(self.row));

        // member_count (required i64)
        let mc_array = batch
            .column(indices.member_count)
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("Failed to downcast member_count to Int64Array")?;
        builders.member_count.append_value(mc_array.value(self.row));

        // rdb_size (required i64)
        let size_array = batch
            .column(indices.rdb_size)
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("Failed to downcast rdb_size to Int64Array")?;
        builders.rdb_size.append_value(size_array.value(self.row));

        // encoding (required Utf8)
        let enc_array = batch
            .column(indices.encoding)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("Failed to downcast encoding to StringArray")?;
        builders.encoding.append_value(enc_array.value(self.row));

        // expire_at (nullable ts ms)
        let expire_array = batch
            .column(indices.expire_at)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .context("Failed to downcast expire_at to TimestampMillisecondArray")?;
        if expire_array.is_null(self.row) {
            builders.expire_at.append_null();
        } else {
            builders
                .expire_at
                .append_value(expire_array.value(self.row));
        }

        // idle_seconds (nullable i64)
        let idle_array = batch
            .column(indices.idle_seconds)
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("Failed to downcast idle_seconds to Int64Array")?;
        if idle_array.is_null(self.row) {
            builders.idle_seconds.append_null();
        } else {
            builders
                .idle_seconds
                .append_value(idle_array.value(self.row));
        }

        // freq (nullable i32)
        let freq_array = batch
            .column(indices.freq)
            .as_any()
            .downcast_ref::<Int32Array>()
            .context("Failed to downcast freq to Int32Array")?;
        if freq_array.is_null(self.row) {
            builders.freq.append_null();
        } else {
            builders.freq.append_value(freq_array.value(self.row));
        }

        // codis_slot (nullable i32)
        let codis_array = batch
            .column(indices.codis_slot)
            .as_any()
            .downcast_ref::<Int32Array>()
            .context("Failed to downcast codis_slot to Int32Array")?;
        if codis_array.is_null(self.row) {
            builders.codis_slot.append_null();
        } else {
            builders
                .codis_slot
                .append_value(codis_array.value(self.row));
        }

        // redis_slot (nullable i32)
        let redis_array = batch
            .column(indices.redis_slot)
            .as_any()
            .downcast_ref::<Int32Array>()
            .context("Failed to downcast redis_slot to Int32Array")?;
        if redis_array.is_null(self.row) {
            builders.redis_slot.append_null();
        } else {
            builders
                .redis_slot
                .append_value(redis_array.value(self.row));
        }

        // Advance to next row
        self.row += 1;
        Ok(())
    }
}

/// Column indices caching all column positions to avoid repeated lookups
#[derive(Clone, Copy)]
pub struct ColumnIndices {
    pub db: usize,
    pub key: usize,
    pub r#type: usize,
    pub member_count: usize,
    pub rdb_size: usize,
    pub encoding: usize,
    pub expire_at: usize,
    pub idle_seconds: usize,
    pub freq: usize,
    pub codis_slot: usize,
    pub redis_slot: usize,
}

impl ColumnIndices {
    pub fn new() -> AnyResult<Self> {
        let schema = Arc::new(schema::create_redis_record_schema());

        Ok(Self {
            db: schema::find_field_index(&schema, "db").unwrap(),
            key: schema::find_field_index(&schema, "key").unwrap(),
            r#type: schema::find_field_index(&schema, "type").unwrap(),
            member_count: schema::find_field_index(&schema, "member_count").unwrap(),
            rdb_size: schema::find_field_index(&schema, "rdb_size").unwrap(),
            encoding: schema::find_field_index(&schema, "encoding").unwrap(),
            expire_at: schema::find_field_index(&schema, "expire_at").unwrap(),
            idle_seconds: schema::find_field_index(&schema, "idle_seconds").unwrap(),
            freq: schema::find_field_index(&schema, "freq").unwrap(),
            codis_slot: schema::find_field_index(&schema, "codis_slot").unwrap(),
            redis_slot: schema::find_field_index(&schema, "redis_slot").unwrap(),
        })
    }
}

/// Output builders for constructing Arrow RecordBatch
pub struct OutputBuilders {
    pub cluster: StringBuilder,
    pub batch: TimestampNanosecondBuilder,
    pub instance: StringBuilder,
    pub db: Int64Builder,
    pub key: BinaryBuilder,
    pub r#type: StringBuilder,
    pub member_count: Int64Builder,
    pub rdb_size: Int64Builder,
    pub encoding: StringBuilder,
    pub expire_at: TimestampMillisecondBuilder,
    pub idle_seconds: Int64Builder,
    pub freq: Int32Builder,
    pub codis_slot: Int32Builder,
    pub redis_slot: Int32Builder,
    pub capacity: usize,
    pub rows: usize,
}

impl OutputBuilders {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            cluster: StringBuilder::with_capacity(capacity, capacity * 8),
            batch: TimestampNanosecondBuilder::with_capacity(capacity),
            instance: StringBuilder::with_capacity(capacity, capacity * 8),
            db: Int64Builder::with_capacity(capacity),
            key: BinaryBuilder::with_capacity(capacity, capacity * 16),
            r#type: StringBuilder::with_capacity(capacity, capacity * 6),
            member_count: Int64Builder::with_capacity(capacity),
            rdb_size: Int64Builder::with_capacity(capacity),
            encoding: StringBuilder::with_capacity(capacity, capacity * 6),
            expire_at: TimestampMillisecondBuilder::with_capacity(capacity),
            idle_seconds: Int64Builder::with_capacity(capacity),
            freq: Int32Builder::with_capacity(capacity),
            codis_slot: Int32Builder::with_capacity(capacity),
            redis_slot: Int32Builder::with_capacity(capacity),
            capacity,
            rows: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.rows
    }

    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    pub fn is_full(&self) -> bool {
        self.len() >= self.capacity
    }

    pub fn finish_to_batch(&mut self) -> AnyResult<RecordBatch> {
        let schema = Arc::new(schema::create_redis_record_schema());
        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.cluster.finish()),
            Arc::new(self.batch.finish().with_timezone("UTC")),
            Arc::new(self.instance.finish()),
            Arc::new(self.db.finish()),
            Arc::new(self.key.finish()),
            Arc::new(self.r#type.finish()),
            Arc::new(self.member_count.finish()),
            Arc::new(self.rdb_size.finish()),
            Arc::new(self.encoding.finish()),
            Arc::new(self.expire_at.finish().with_timezone("UTC")),
            Arc::new(self.idle_seconds.finish()),
            Arc::new(self.freq.finish()),
            Arc::new(self.codis_slot.finish()),
            Arc::new(self.redis_slot.finish()),
        ];
        let batch = RecordBatch::try_new(schema, arrays)?;

        // Reset builders for reuse
        self.reset();
        Ok(batch)
    }

    pub fn reset(&mut self) {
        // Reinitialize all builders
        *self = Self::with_capacity(self.capacity);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_heap_item_ord() {
        // Test HeapItem sorting logic
        let item1 = HeapItem {
            db: 1,
            key: b"key1".to_vec(),
            run_idx: 0,
        };
        let item2 = HeapItem {
            db: 1,
            key: b"key2".to_vec(),
            run_idx: 0,
        };
        let item3 = HeapItem {
            db: 2,
            key: b"key1".to_vec(),
            run_idx: 0,
        };

        // Test sorting logic with cmp method
        assert_eq!(item1.cmp(&item2), std::cmp::Ordering::Less); // key1 < key2
        assert_eq!(item2.cmp(&item1), std::cmp::Ordering::Greater); // key2 > key1
        assert_eq!(item1.cmp(&item3), std::cmp::Ordering::Less); // db 1 < db 2
        assert_eq!(item3.cmp(&item1), std::cmp::Ordering::Greater); // db 2 > db 1
    }

    #[test]
    fn test_heap_behavior() {
        // Test min-heap behavior
        let mut heap = BinaryHeap::new();

        let item1 = Reverse(HeapItem {
            db: 2,
            key: b"key2".to_vec(),
            run_idx: 0,
        });
        let item2 = Reverse(HeapItem {
            db: 1,
            key: b"key1".to_vec(),
            run_idx: 0,
        });
        let item3 = Reverse(HeapItem {
            db: 3,
            key: b"key3".to_vec(),
            run_idx: 0,
        });

        heap.push(item1);
        heap.push(item2);
        heap.push(item3);

        // Smallest item should come out first (db=1, key=key1)
        let popped = heap.pop().unwrap();
        assert_eq!(popped.0.db, 1);
        assert_eq!(popped.0.key, b"key1");

        // Next should be db=2, key=key2
        let popped = heap.pop().unwrap();
        assert_eq!(popped.0.db, 2);
        assert_eq!(popped.0.key, b"key2");

        // Last should be db=3, key=key3
        let popped = heap.pop().unwrap();
        assert_eq!(popped.0.db, 3);
        assert_eq!(popped.0.key, b"key3");
    }

    #[test]
    fn test_column_indices_new() {
        // Test ColumnIndices creation
        let indices = ColumnIndices::new().unwrap();

        // Verify all required fields have valid indices
        assert!(indices.db < 20); // Reasonable upper limit for column count
        assert!(indices.key < 20);
        assert!(indices.r#type < 20);
        assert!(indices.member_count < 20);
        assert!(indices.rdb_size < 20);
        assert!(indices.encoding < 20);
        assert!(indices.expire_at < 20);
        assert!(indices.idle_seconds < 20);
        assert!(indices.freq < 20);
        assert!(indices.codis_slot < 20);
        assert!(indices.redis_slot < 20);
    }

    #[test]
    fn test_output_builders_capacity() {
        let capacity = 1000;
        let builders = OutputBuilders::with_capacity(capacity);

        assert_eq!(builders.capacity, capacity);
        assert_eq!(builders.len(), 0);
        assert!(!builders.is_full());
    }

    #[test]
    fn test_output_builders_is_full() {
        let capacity = 10;
        let mut builders = OutputBuilders::with_capacity(capacity);

        // Initial state should not be full and should be empty
        assert!(!builders.is_full());
        assert!(builders.is_empty());

        // Manually set rows to reach capacity
        builders.rows = capacity;
        assert!(builders.is_full());
        assert!(!builders.is_empty());
    }
}
