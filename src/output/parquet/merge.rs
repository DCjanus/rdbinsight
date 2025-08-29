use std::{cmp::Reverse, collections::BinaryHeap, path::PathBuf, sync::Arc};

use anyhow::{Context, anyhow};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};

use crate::{
    config::ParquetCompression,
    helper::AnyResult,
    output::parquet::{mapper::records_to_columns, run_lz4::RunReader, schema},
};

pub struct MergeContext {
    pub inputs: Vec<PathBuf>,
    pub output: PathBuf,
    pub compression: ParquetCompression,
    pub cluster: String,
    pub instance: String,
    pub batch_ts: time::OffsetDateTime,
}

impl MergeContext {
    fn make_heap_item_from_record(rec: &crate::record::Record, run_idx: usize) -> HeapItem {
        let key_bytes = match &rec.key {
            crate::parser::core::raw::RDBStr::Str(bytes) => bytes.to_vec(),
            crate::parser::core::raw::RDBStr::Int(v) => v.to_string().into_bytes(),
        };
        HeapItem {
            db: rec.db as i64,
            key: key_bytes,
            run_idx,
        }
    }
    pub fn merge(self) -> AnyResult<()> {
        let _input_count = self.inputs.len();

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

        let (mut readers, mut current_records) = Self::open_run_readers(&self.inputs)?;

        let mut heap: BinaryHeap<Reverse<HeapItem>> = BinaryHeap::new();

        for (idx, opt_rec) in current_records.iter().enumerate() {
            if let Some(rec) = opt_rec {
                heap.push(Reverse(Self::make_heap_item_from_record(rec, idx)));
            }
        }

        let mut batch_buf: Vec<crate::record::Record> = Vec::with_capacity(8 * 1024);
        let batch_capacity = 8 * 1024;

        while let Some(item) = heap.pop() {
            let run_idx = item.0.run_idx;

            let record = current_records[run_idx]
                .as_ref()
                .expect("current record should be present");
            batch_buf.push(record.clone());

            match readers[run_idx].read_next() {
                Ok(Some(next_rec)) => {
                    current_records[run_idx] = Some(next_rec);
                    heap.push(Reverse(Self::make_heap_item_from_record(
                        current_records[run_idx].as_ref().unwrap(),
                        run_idx,
                    )));
                }
                Ok(None) => current_records[run_idx] = None,
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
#[derive(Eq, PartialEq, Ord, PartialOrd)]
pub struct HeapItem {
    pub db: i64,
    pub key: Vec<u8>,
    pub run_idx: usize,
}

// ColumnIndices removed — no longer needed

// OutputBuilders removed — no longer needed

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

    // ColumnIndices tests removed

    // OutputBuilders tests removed
}
