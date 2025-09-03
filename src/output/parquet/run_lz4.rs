use std::{
    fs::File,
    io::{BufReader, Read, Write},
    path::Path,
};

use anyhow::{Context, Result, anyhow};
use bincode::config::standard;
use crc::{CRC_32_ISO_HDLC, Crc};
use lz4_flex::frame::FrameEncoder;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

use crate::record::Record;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ChunkDesc {
    pub offset: u64,
    pub length: u64,
}

pub struct RunReader {
    decoder: lz4_flex::frame::FrameDecoder<BufReader<File>>,
    path: std::path::PathBuf,
    offset: u64,
}

impl RunReader {
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
        let crc_calc = Crc::<u32>::new(&CRC_32_ISO_HDLC).checksum(&payload);
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
