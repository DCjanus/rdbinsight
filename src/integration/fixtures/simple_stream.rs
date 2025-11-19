use anyhow::{anyhow, ensure};
use async_trait::async_trait;
use redis::aio::MultiplexedConnection;
use semver::Version;

use super::TestFixture;
use crate::{
    helper::AnyResult,
    parser::{Item, model::StreamEncoding},
};

#[derive(Debug, Default)]
pub struct SimpleStreamFixture;

impl SimpleStreamFixture {
    pub fn new() -> Self {
        Self
    }
}

const KEY: &str = "integration:stream";
const MESSAGE_COUNT: usize = 64;

#[async_trait]
impl TestFixture for SimpleStreamFixture {
    fn name(&self) -> &'static str {
        "simple_stream_fixture"
    }

    fn supported(&self, version: &Version) -> bool {
        version.major >= 5
    }

    async fn load(&self, conn: &mut MultiplexedConnection) -> AnyResult<()> {
        redis::cmd("DEL").arg(KEY).query_async::<()>(conn).await?;

        let mut pipe = redis::pipe();
        for idx in 0..MESSAGE_COUNT {
            let payload = format!("stream-payload-{idx:04}");
            pipe.cmd("XADD")
                .arg(KEY)
                .arg("*")
                .arg("data")
                .arg(payload)
                .ignore();
        }
        pipe.query_async::<()>(&mut *conn).await?;

        Ok(())
    }

    fn assert(&self, version: &Version, items: &[Item]) -> AnyResult<()> {
        let item = items
            .iter()
            .filter(|item| matches!(item, Item::StreamRecord { .. }))
            .find(|item| item.key().is_some_and(|key| key == KEY))
            .ok_or_else(|| {
                anyhow!(
                    "Expected to find stream record with key '{KEY}' but none found. Total items: {}",
                    items.len()
                )
            })?;

        match item {
            Item::StreamRecord {
                encoding,
                message_count,
                ..
            } => {
                ensure!(
                    *message_count == MESSAGE_COUNT as u64,
                    "unexpected message count {message_count}, expected {MESSAGE_COUNT}"
                );
                let expected_encoding = match version.major {
                    0..=6 => StreamEncoding::ListPacks,
                    7 => StreamEncoding::ListPacks2,
                    _ => StreamEncoding::ListPacks3,
                };
                ensure!(
                    *encoding == expected_encoding,
                    "unexpected stream encoding {encoding:?}, expected {expected_encoding:?} for Redis {version}"
                );
            }
            _ => unreachable!("checked stream record variant"),
        }

        Ok(())
    }
}
