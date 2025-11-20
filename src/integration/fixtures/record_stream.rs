use anyhow::{anyhow, ensure};
use async_trait::async_trait;
use redis::{Value, aio::MultiplexedConnection};
use semver::Version;

use super::TestFixture;
use crate::{
    helper::AnyResult,
    parser::{Item, model::StreamEncoding},
};

#[derive(Debug, Default)]
pub struct StreamRecordFixture;

impl StreamRecordFixture {
    pub fn new() -> Self {
        Self
    }
}

#[derive(Debug, Default)]
pub struct StreamConsumerGroupFixture;

impl StreamConsumerGroupFixture {
    pub fn new() -> Self {
        Self
    }
}

const KEY: &str = "integration:stream";
const MESSAGE_COUNT: usize = 64;
const GROUP_KEY: &str = "integration:stream:groups";
const GROUP_NAME: &str = "integration:cg";
const GROUP_MESSAGE_COUNT: usize = 48;
const GROUP_READ_COUNT: usize = 5;
const GROUP_CONSUMERS: [&str; 2] = ["consumer-alpha", "consumer-beta"];

fn expected_encoding(version: &Version) -> StreamEncoding {
    if version.major >= 8 {
        StreamEncoding::ListPacks3
    } else if version.major == 7 {
        if version.minor >= 4 {
            StreamEncoding::ListPacks3
        } else {
            StreamEncoding::ListPacks2
        }
    } else {
        StreamEncoding::ListPacks
    }
}

#[async_trait]
impl TestFixture for StreamRecordFixture {
    fn name(&self) -> &'static str {
        "stream_record_fixture"
    }

    fn supported(&self, version: &Version) -> bool {
        version.major >= 5
    }

    async fn load(&self, conn: &mut MultiplexedConnection) -> AnyResult<()> {
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
                let expected_encoding = expected_encoding(version);
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

#[async_trait]
impl TestFixture for StreamConsumerGroupFixture {
    fn name(&self) -> &'static str {
        "stream_consumer_group_fixture"
    }

    fn supported(&self, version: &Version) -> bool {
        version.major >= 7
    }

    async fn load(&self, conn: &mut MultiplexedConnection) -> AnyResult<()> {
        let mut pipe = redis::pipe();
        for idx in 0..GROUP_MESSAGE_COUNT {
            let payload = format!("cg-payload-{idx:04}");
            pipe.cmd("XADD")
                .arg(GROUP_KEY)
                .arg("*")
                .arg("data")
                .arg(payload)
                .ignore();
        }
        pipe.query_async::<()>(&mut *conn).await?;

        redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(GROUP_KEY)
            .arg(GROUP_NAME)
            .arg("0")
            .arg("MKSTREAM")
            .query_async::<()>(&mut *conn)
            .await?;

        for consumer in GROUP_CONSUMERS {
            redis::cmd("XREADGROUP")
                .arg("GROUP")
                .arg(GROUP_NAME)
                .arg(consumer)
                .arg("COUNT")
                .arg(GROUP_READ_COUNT as u64)
                .arg("STREAMS")
                .arg(GROUP_KEY)
                .arg(">")
                .query_async::<Value>(&mut *conn)
                .await?;
        }

        Ok(())
    }

    fn assert(&self, version: &Version, items: &[Item]) -> AnyResult<()> {
        let item = items
            .iter()
            .filter(|item| matches!(item, Item::StreamRecord { .. }))
            .find(|item| item.key().is_some_and(|key| key == GROUP_KEY))
            .ok_or_else(|| {
                anyhow!(
                    "Expected to find stream record with key '{GROUP_KEY}' but none found. Total items: {}",
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
                    *message_count == GROUP_MESSAGE_COUNT as u64,
                    "unexpected message count {message_count}, expected {GROUP_MESSAGE_COUNT}"
                );
                let expected = expected_encoding(version);
                ensure!(
                    *encoding == expected,
                    "unexpected stream encoding {encoding:?}, expected {expected:?} for Redis {version}"
                );
            }
            _ => unreachable!("checked stream record variant"),
        }

        Ok(())
    }
}
