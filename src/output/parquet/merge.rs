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

use crate::{
    config::ParquetCompression,
    helper::AnyResult,
    output::parquet::{mapper::records_to_columns, run_lz4::RunReader, schema},
};

/// Merge context for a single merge execution (k-way merge, delete inputs on success)
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
        let _input_count = self.inputs.len();

        if self.inputs.is_empty() {
            self.create_empty_output()?;
            return Ok(());
        }

        // Writer
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

        // Open run readers and initialize heap
        let (mut readers, mut current_records) = Self::open_run_readers(&self.inputs)?;

        let mut heap: BinaryHeap<Reverse<HeapItem>> = BinaryHeap::new();
        for (idx, opt_rec) in current_records.iter().enumerate() {
            if let Some(rec) = opt_rec {
                let key_bytes = match &rec.key {
                    crate::parser::core::raw::RDBStr::Str(bytes) => bytes.to_vec(),
                    crate::parser::core::raw::RDBStr::Int(v) => v.to_string().into_bytes(),
                };
                heap.push(Reverse(HeapItem {
                    db: rec.db as i64,
                    key: key_bytes,
                    run_idx: idx,
                }));
            }
        }

        let mut batch_buf: Vec<crate::record::Record> = Vec::with_capacity(8 * 1024);
        let batch_capacity = 8 * 1024;

        // Merge loop
        while let Some(item) = heap.pop() {
            let run_idx = item.0.run_idx;
            // take current record
            let record = current_records[run_idx]
                .as_ref()
                .expect("current record should be present");

            batch_buf.push(record.clone());

            match readers[run_idx].read_next() {
                Ok(Some(next_rec)) => {
                    current_records[run_idx] = Some(next_rec);
                    let key_bytes = match &current_records[run_idx].as_ref().unwrap().key {
                        crate::parser::core::raw::RDBStr::Str(bytes) => bytes.to_vec(),
                        crate::parser::core::raw::RDBStr::Int(v) => v.to_string().into_bytes(),
                    };
                    heap.push(Reverse(HeapItem {
                        db: current_records[run_idx].as_ref().unwrap().db as i64,
                        key: key_bytes,
                        run_idx,
                    }));
                }
                Ok(None) => {
                    current_records[run_idx] = None;
                }
                Err(e) => {
                    return Err(e).with_context(|| anyhow!("Failed to read from run {}", run_idx));
                }
            }

            if batch_buf.len() >= batch_capacity {
                Self::flush_batch_buf(&mut writer, &cluster, batch_ts, &instance, &mut batch_buf)?;
            }
        }

        if !batch_buf.is_empty() {
            Self::flush_batch_buf(&mut writer, &cluster, batch_ts, &instance, &mut batch_buf)?;
        }

        writer
            .close()
            .map_err(|e| anyhow!("Failed to close final parquet writer: {e}"))?;

        // Delete inputs only after successful close of output
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

    // Helper: open run readers and read first record for each
    fn open_run_readers(
        inputs: &[PathBuf],
    ) -> AnyResult<(Vec<RunReader>, Vec<Option<crate::record::Record>>)> {
        let mut readers = Vec::with_capacity(inputs.len());
        let mut current_records = Vec::with_capacity(inputs.len());
        for path in inputs {
            let mut r = RunReader::open(path).with_context(|| {
                anyhow!("Failed to open run file for reading: {}", path.display())
            })?;
            let rec = r
                .read_next()
                .with_context(|| anyhow!("Failed to read first record from: {}", path.display()))?;
            readers.push(r);
            current_records.push(rec);
        }
        Ok((readers, current_records))
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
            anyhow!("Failed to open run segment for reading: {}", path.display())
        })?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .with_context(|| {
                anyhow!(
                    "Failed to create ParquetRecordBatchReaderBuilder for {}",
                    path.display()
                )
            })?
            .with_batch_size(1024);
        let reader = builder.build().with_context(|| {
            anyhow!(
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
            None => {
                self.current = None;
                self.row = 0;
                Ok(false)
            }
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
