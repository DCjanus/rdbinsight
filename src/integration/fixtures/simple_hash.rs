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
pub struct SimpleHashFixture;

impl SimpleHashFixture {
    pub fn new() -> Self {
        Self
    }
}

const KEY: &str = "integration:hash";
const FIELD_COUNT: usize = 700;
const VALUE_PAD: &str = "hash-value-padding-to-force-raw-encoding--------------------------------";

#[async_trait]
impl TestFixture for SimpleHashFixture {
    fn name(&self) -> &'static str {
        "simple_hash_fixture"
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
