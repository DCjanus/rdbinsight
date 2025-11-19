use anyhow::{anyhow, ensure};
use async_trait::async_trait;
use redis::aio::MultiplexedConnection;
use semver::Version;

use super::TestFixture;
use crate::{
    helper::AnyResult,
    parser::{Item, model::ZSetEncoding},
};

#[derive(Debug, Default)]
pub struct SimpleZSetFixture;

impl SimpleZSetFixture {
    pub fn new() -> Self {
        Self
    }
}

const KEY: &str = "integration:zset";
const MEMBERS: &[(f64, &str)] = &[(1.0, "bronze"), (2.0, "silver"), (3.0, "gold")];

#[async_trait]
impl TestFixture for SimpleZSetFixture {
    fn name(&self) -> &'static str {
        "simple_zset_fixture"
    }

    async fn load(&self, conn: &mut MultiplexedConnection) -> AnyResult<()> {
        redis::cmd("DEL").arg(KEY).query_async::<()>(conn).await?;

        let mut cmd = redis::cmd("ZADD");
        cmd.arg(KEY);
        for (score, member) in MEMBERS.iter().copied() {
            cmd.arg(score).arg(member);
        }
        cmd.query_async::<()>(conn).await?;

        Ok(())
    }

    fn assert(&self, version: &Version, items: &[Item]) -> AnyResult<()> {
        let item = items
            .iter()
            .filter(|item| matches!(item, Item::ZSetRecord { .. } | Item::ZSet2Record { .. }))
            .find(|item| item.key().is_some_and(|key| key == KEY))
            .ok_or_else(|| {
                anyhow!(
                    "Expected to find zset record with key '{}' but none found. Total items: {}",
                    KEY,
                    items.len()
                )
            })?;

        match item {
            Item::ZSetRecord {
                encoding,
                member_count,
                ..
            }
            | Item::ZSet2Record {
                encoding,
                member_count,
                ..
            } => {
                ensure!(
                    *member_count == MEMBERS.len() as u64,
                    "unexpected member count {member_count}, expected {}",
                    MEMBERS.len()
                );
                let expected_encoding = if version.major >= 7 {
                    ZSetEncoding::ListPack
                } else {
                    ZSetEncoding::ZipList
                };
                ensure!(
                    *encoding == expected_encoding,
                    "unexpected zset encoding {encoding:?}, expected {expected_encoding:?} for Redis {version}"
                );
            }
            _ => unreachable!("checked zset variants"),
        }

        Ok(())
    }
}
