use std::{
    fs::File,
    io::{BufReader, Read, Write},
    path::Path,
    time::Instant,
};

use anyhow::{Context, Result, anyhow};
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
pub struct RunReader {
    decoder: lz4_flex::frame::FrameDecoder<BufReader<File>>,
    path: std::path::PathBuf,
    offset: u64,
}

impl RunReader {
    /// Open a run file for reading. Uses a buffered reader (256 KiB) wrapped by FrameDecoder.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let file = File::open(&path_buf)
            .with_context(|| anyhow!("Failed to open run file: {}", path_buf.display()))?;
        let buf_reader = BufReader::with_capacity(64 * 1024, file);
        let decoder = lz4_flex::frame::FrameDecoder::new(buf_reader);
        Ok(Self {
            decoder,
            path: path_buf,
            offset: 0,
        })
    }

    /// Read the next Record from the run. Returns Ok(None) on EOF.
    pub fn read_next(&mut self) -> Result<Option<Record>> {
        use anyhow::Context;

        // Read 4-byte big-endian length
        let mut len_buf = [0u8; 4];
        if let Err(e) = self.decoder.read_exact(&mut len_buf) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(None);
            } else {
                return Err(e).with_context(|| {
                    anyhow!(
                        "Failed to read length from {} at offset {}",
                        self.path.display(),
                        self.offset
                    )
                });
            }
        }
        let len = u32::from_be_bytes(len_buf) as usize;

        // Read payload
        let mut payload = vec![0u8; len];
        self.decoder.read_exact(&mut payload).with_context(|| {
            anyhow!(
                "Failed to read payload from {} at offset {}",
                self.path.display(),
                self.offset
            )
        })?;

        // Read CRC32
        let mut crc_buf = [0u8; 4];
        self.decoder.read_exact(&mut crc_buf).with_context(|| {
            anyhow!(
                "Failed to read crc from {} at offset {}",
                self.path.display(),
                self.offset
            )
        })?;
        let crc_read = u32::from_be_bytes(crc_buf);

        // Validate CRC
        let crc_calc = crc32fast::hash(&payload);
        if crc_calc != crc_read {
            return Err(anyhow!(
                "CRC mismatch for {} at offset {}: expected {}, got {}",
                self.path.display(),
                self.offset,
                crc_read,
                crc_calc
            ));
        }

        // Deserialize with bincode
        let (record, _): (Record, usize) = bincode::serde::decode_from_slice(&payload, standard())
            .with_context(|| {
                anyhow!(
                    "Failed to deserialize record from {} at offset {}",
                    self.path.display(),
                    self.offset
                )
            })?;

        // Advance offset (len field + payload + crc)
        self.offset += 4 + len as u64 + 4;

        Ok(Some(record))
    }
}

// Async wrapper that runs `write_run_file` inside `tokio::task::spawn_blocking` to avoid blocking the async runtime.
// Accepts a pre-collected Vec<Record> so ownership can be moved into the blocking thread.
pub async fn write_run_file_blocking<P>(path: P, records: Vec<Record>) -> Result<()>
where P: AsRef<Path> + Send + 'static {
    let path_buf = path.as_ref().to_path_buf();
    let join_handle =
        tokio::task::spawn_blocking(move || write_run_file(path_buf, records.into_iter()));

    match join_handle.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(join_err) => Err(anyhow!("spawn_blocking join error: {}", join_err)),
    }
}
