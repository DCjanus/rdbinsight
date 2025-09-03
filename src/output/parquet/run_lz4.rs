use std::{
    fs::File,
    io::{BufReader, Read, Seek, SeekFrom, Write},
    path::Path,
};

use anyhow::{Context, Result, anyhow, ensure};
use bincode::config::standard;
use lz4_flex::frame::{FrameDecoder, FrameEncoder};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

use crate::record::Record;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ChunkDesc {
    pub offset: u64,
    pub length: u64,
}

pub struct RunChunkReader {
    decoder: FrameDecoder<BufReader<std::io::Take<File>>>,
    path: std::path::PathBuf,
    offset: u64,
}

pub fn read_run_index(path: impl AsRef<Path>) -> Result<Vec<ChunkDesc>> {
    let path_buf = path.as_ref().to_path_buf();
    let mut file = File::open(&path_buf).with_context(|| {
        anyhow!(
            "Failed to open run file for index read: {}",
            path_buf.display()
        )
    })?;
    let file_len = file
        .metadata()
        .with_context(|| anyhow!("Failed to stat run file: {}", path_buf.display()))?
        .len();
    ensure!(
        file_len >= 8,
        "Run file too small to contain index: {}",
        path_buf.display()
    );

    file.seek(SeekFrom::End(-8)).context("seek to end - 8")?;
    let mut len_buf = [0u8; 8];
    file.read_exact(&mut len_buf).context("read index length")?;
    let index_len = u64::from_be_bytes(len_buf);

    let index_start = file_len.saturating_sub(8 + index_len);
    file.seek(SeekFrom::Start(index_start))
        .context("seek to index start")?;

    let mut compressed_index = vec![0u8; index_len as usize];
    file.read_exact(&mut compressed_index)
        .context("read compressed index")?;

    let mut decoder = FrameDecoder::new(BufReader::new(std::io::Cursor::new(compressed_index)));
    let mut index_bytes = Vec::new();
    decoder
        .read_to_end(&mut index_bytes)
        .context("decompress index")?;

    let (chunks, _): (Vec<ChunkDesc>, usize) =
        bincode::serde::decode_from_slice(&index_bytes, standard()).context("decode index")?;
    Ok(chunks)
}

impl RunChunkReader {
    pub fn open(path: impl AsRef<Path>, offset: u64, length: u64) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let mut file = File::open(&path_buf)
            .with_context(|| anyhow!("Failed to open run file: {}", path_buf.display()))?;
        file.seek(SeekFrom::Start(offset)).with_context(|| {
            anyhow!(
                "Failed to seek run file: {} to offset {}",
                path_buf.display(),
                offset
            )
        })?;
        let take = file.take(length);
        let buf_reader = BufReader::with_capacity(64 * 1024, take);
        let decoder = FrameDecoder::new(buf_reader);
        Ok(Self {
            decoder,
            path: path_buf,
            offset: 0,
        })
    }

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
                        "Failed to read length from {} at chunk-offset {}",
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
                "Failed to read payload from {} at chunk-offset {}",
                self.path.display(),
                self.offset
            )
        })?;

        // Deserialize with bincode
        let (record, _): (Record, usize) = bincode::serde::decode_from_slice(&payload, standard())
            .with_context(|| {
                anyhow!(
                    "Failed to deserialize record from {} at chunk-offset {}",
                    self.path.display(),
                    self.offset
                )
            })?;

        // Advance offset (len field + payload)
        self.offset += 4 + len as u64;

        Ok(Some(record))
    }
}

pub async fn append_chunk(file: &mut tokio::fs::File, records: Vec<Record>) -> Result<ChunkDesc> {
    let mut encoder = FrameEncoder::new(Vec::new());
    for record in records {
        let payload = bincode::serde::encode_to_vec(&record, standard())?;
        let len = payload.len();
        encoder.write_all(&(len as u32).to_be_bytes())?;
        encoder.write_all(&payload)?;
    }
    let compressed = encoder.finish().context("finish encoder")?;

    let offset = file.metadata().await?.len();
    file.write_all(&compressed).await?;

    Ok(ChunkDesc {
        offset,
        length: compressed.len() as u64,
    })
}

pub async fn write_index(file: &mut tokio::fs::File, index: Vec<ChunkDesc>) -> Result<()> {
    let index_bytes = bincode::serde::encode_to_vec(&index, standard())?;
    let mut encoder = FrameEncoder::new(Vec::new());
    encoder.write_all(&index_bytes)?;
    let compressed = encoder.finish().context("finish encoder")?;
    file.write_all(&compressed).await?;
    file.write_all(&(compressed.len() as u64).to_be_bytes())
        .await?;
    Ok(())
}
