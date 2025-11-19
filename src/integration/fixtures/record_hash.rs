use anyhow::{anyhow, ensure};
use async_trait::async_trait;
use redis::aio::MultiplexedConnection;
use semver::Version;

use super::TestFixture;
use crate::{
    helper::AnyResult,
    parser::{Item, model::HashEncoding},
};

#[derive(Debug, Default)]
pub struct HashRecordFixture;

impl HashRecordFixture {
    pub fn new() -> Self {
        Self
    }
}

#[derive(Debug, Default)]
pub struct HashZipListRecordFixture;

impl HashZipListRecordFixture {
    pub fn new() -> Self {
        Self
    }
}

const KEY: &str = "integration:hash";
const FIELD_COUNT: usize = 700;
const VALUE_PAD: &str = "hash-value-padding-to-force-raw-encoding--------------------------------";
const ZIPLIST_KEY: &str = "integration:hash:ziplist";
const ZIPLIST_FIELD_COUNT: usize = 16;

#[async_trait]
impl TestFixture for HashRecordFixture {
    fn name(&self) -> &'static str {
        "hash_record_fixture"
    }

    fn supported(&self, version: &Version) -> bool {
        version.major >= 2
    }

    async fn load(&self, conn: &mut MultiplexedConnection) -> AnyResult<()> {
        // Redis 2.x only supports single field/value per HSET invocation, so
        // pipeline the individual commands for compatibility across versions.
        let mut pipe = redis::pipe();
        for idx in 0..FIELD_COUNT {
            let field = format!("hash-field-{idx:04}");
            let value = format!("{VALUE_PAD}-{idx:04}");
            pipe.cmd("HSET").arg(KEY).arg(field).arg(value).ignore();
        }
        pipe.query_async::<()>(&mut *conn).await?;

        Ok(())
    }

    fn assert(&self, _: &Version, items: &[Item]) -> AnyResult<()> {
        let item = items
            .iter()
            .filter(|item| matches!(item, Item::HashRecord { .. }))
            .find(|item| item.key().is_some_and(|key| key == KEY))
            .ok_or_else(|| {
                anyhow!(
                    "Expected to find hash record with key '{KEY}' but none found. Total items: {}",
                    items.len()
                )
            })?;

        match item {
            Item::HashRecord {
                encoding,
                pair_count,
                ..
            } => {
                ensure!(
                    *pair_count == FIELD_COUNT as u64,
                    "unexpected hash pair count {pair_count}, expected {FIELD_COUNT}"
                );
                ensure!(
                    matches!(encoding, HashEncoding::Raw),
                    "unexpected hash encoding {encoding:?}, expected Raw"
                );
            }
            _ => unreachable!("checked hash record variant"),
        }

        Ok(())
    }
}

#[async_trait]
impl TestFixture for HashZipListRecordFixture {
    fn name(&self) -> &'static str {
        "hash_ziplist_record_fixture"
    }

    fn supported(&self, version: &Version) -> bool {
        match version.major {
            0 | 1 => false,
            2 => version.minor >= 6,
            3 | 4 | 5 | 6 => true,
            _ => false,
        }
    }

    async fn load(&self, conn: &mut MultiplexedConnection) -> AnyResult<()> {
        let mut pipe = redis::pipe();
        for idx in 0..ZIPLIST_FIELD_COUNT {
            let field = format!("zip-field-{idx:04}");
            let value = format!("zip-value-{idx:04}");
            pipe.cmd("HSET")
                .arg(ZIPLIST_KEY)
                .arg(field)
                .arg(value)
                .ignore();
        }
        pipe.query_async::<()>(&mut *conn).await?;

        Ok(())
    }

    fn assert(&self, _: &Version, items: &[Item]) -> AnyResult<()> {
        let item = items
            .iter()
            .filter(|item| matches!(item, Item::HashRecord { .. }))
            .find(|item| item.key().is_some_and(|key| key == ZIPLIST_KEY))
            .ok_or_else(|| {
                anyhow!(
                    "Expected to find hash record with key '{ZIPLIST_KEY}' but none found. Total items: {}",
                    items.len()
                )
            })?;

        match item {
            Item::HashRecord {
                encoding,
                pair_count,
                ..
            } => {
                ensure!(
                    *pair_count == ZIPLIST_FIELD_COUNT as u64,
                    "unexpected hash pair count {pair_count}, expected {ZIPLIST_FIELD_COUNT}"
                );
                ensure!(
                    matches!(encoding, HashEncoding::ZipList),
                    "unexpected hash encoding {encoding:?}, expected ZipList"
                );
            }
            _ => unreachable!("checked hash record variant"),
        }

        Ok(())
    }
}
