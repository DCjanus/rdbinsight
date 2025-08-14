use std::path::PathBuf;

use anyhow::Result;

use crate::config::ParquetCompression;

pub mod path;

/// Placeholder ParquetOutput struct for Parquet file output
pub struct ParquetOutput {
    pub dir: PathBuf,
    pub compression: ParquetCompression,
}

impl ParquetOutput {
    /// Create a new ParquetOutput instance
    pub async fn new(dir: PathBuf, compression: ParquetCompression) -> Result<Self> {
        Ok(Self { dir, compression })
    }
}
