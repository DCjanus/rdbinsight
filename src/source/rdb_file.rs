use anyhow::Context;
use async_trait::async_trait;
use bytes::BytesMut;
use tokio::{fs::File, io::AsyncReadExt};

use crate::{helper::AnyResult, source::Source};

pub struct RDBFileSource {
    file: File,
}

impl RDBFileSource {
    pub async fn new(path: String) -> AnyResult<Self> {
        let file = File::open(path).await.context("open RDB file")?;
        Ok(Self { file })
    }
}

#[async_trait]
impl Source for RDBFileSource {
    async fn prepare(&mut self) -> AnyResult<()> {
        Ok(())
    }

    async fn read(&mut self, out: &mut BytesMut) -> AnyResult<usize> {
        // XXX: reuse buffer to improve performance
        let mut buf = vec![0u8; 1024 * 16];
        let n = self.file.read_buf(&mut buf).await?;
        out.extend_from_slice(&buf[..n]);
        Ok(n)
    }
}
