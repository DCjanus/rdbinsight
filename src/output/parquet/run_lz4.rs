use std::{fs::File, io::Write, path::Path, time::Instant};

use anyhow::{Context, Result};
use bincode::config::standard;
use crc32fast::Hasher as Crc32Hasher;
use lz4_flex::frame::FrameEncoder;
use tracing::debug;

use crate::record::Record;

/// Write all records into a single LZ4-framed run file. This consumes the provided
/// records iterator (ownership of `Record`) which fits the batch-write scenario.
pub fn write_run_file<I>(path: impl AsRef<Path>, records: I) -> Result<()>
where I: IntoIterator<Item = Record> {
    let path_buf = path.as_ref().to_path_buf();
    let file = File::create(&path_buf)?;
    let mut encoder = FrameEncoder::new(file);

    let mut rows_written: u64 = 0;
    let mut bytes_uncompressed: u64 = 0;
    let start = Instant::now();

    for record in records {
        // serialize with bincode
        let payload = bincode::serde::encode_to_vec(&record, standard())?;
        let len = payload.len();

        // crc32
        let mut hasher = Crc32Hasher::new();
        hasher.update(&payload);
        let crc = hasher.finalize();

        // write len_be_u32 | payload | crc32_be_u32
        encoder.write_all(&(len as u32).to_be_bytes())?;
        encoder.write_all(&payload)?;
        encoder.write_all(&crc.to_be_bytes())?;

        rows_written += 1;
        bytes_uncompressed += len as u64;
    }

    // finish the frame and flush
    encoder.finish().context("finish encoder")?;
    let elapsed = start.elapsed();

    debug!(operation = "parquet_run_lz4_flush_finished", path = %path_buf.display(), rows = rows_written, bytes_uncompressed = bytes_uncompressed, elapsed_ms = %elapsed.as_millis(), "Flushed run to {}", path_buf.display());

    Ok(())
}

// Placeholder for RunReader to be implemented in next phase
#[allow(dead_code)]
pub struct RunReader;
