use anyhow::{anyhow, ensure};
use async_trait::async_trait;
use redis::{aio::MultiplexedConnection, pipe};
use semver::Version;

use super::TestFixture;
use crate::{
    helper::AnyResult,
    parser::{Item, model::SetEncoding},
};

#[derive(Debug, Default)]
pub struct SetRecordFixture;

impl SetRecordFixture {
    pub fn new() -> Self {
        Self
    }
}

#[derive(Debug, Default)]
pub struct SetIntSetRecordFixture;

impl SetIntSetRecordFixture {
    pub fn new() -> Self {
        Self
    }
}

const KEY: &str = "integration:set";
const MEMBERS: [&str; 3] = [
    "set-member-alpha",
    "set-member-beta",
    "set-member-with-a-slightly-longer-value-to-avoid-intset",
];
const INTSET_KEY: &str = "integration:set:intset";
const INTSET_MEMBER_COUNT: usize = 50;

#[async_trait]
impl TestFixture for SetRecordFixture {
    fn name(&self) -> &'static str {
        "set_record_fixture"
    }

    async fn load(&self, conn: &mut MultiplexedConnection) -> AnyResult<()> {
        let mut pipe = pipe();
        for member in MEMBERS {
            pipe.cmd("SADD").arg(KEY).arg(member).ignore();
        }
        pipe.query_async::<()>(&mut *conn).await?;
        Ok(())
    }

    fn assert(&self, _: &Version, items: &[Item]) -> AnyResult<()> {
        let item = items
            .iter()
            .filter(|item| matches!(item, Item::SetRecord { .. }))
            .find(|item| item.key().is_some_and(|key| key == KEY))
            .ok_or_else(|| {
                anyhow!(
                    "Expected to find set record with key '{}' but none found. Total items: {}",
                    KEY,
                    items.len()
                )
            })?;

        match item {
            Item::SetRecord {
                encoding,
                member_count,
                ..
            } => {
                ensure!(
                    matches!(encoding, SetEncoding::Raw | SetEncoding::ListPack),
                    "unexpected set encoding {encoding:?}"
                );
                ensure!(
                    *member_count == MEMBERS.len() as u64,
                    "unexpected member count {member_count}, expected {}",
                    MEMBERS.len()
                );
            }
            _ => unreachable!("checked set record variant"),
        }

        Ok(())
    }
}

#[async_trait]
impl TestFixture for SetIntSetRecordFixture {
    fn name(&self) -> &'static str {
        "set_intset_record_fixture"
    }

    async fn load(&self, conn: &mut MultiplexedConnection) -> AnyResult<()> {
        let mut pipe = pipe();
        for idx in 0..INTSET_MEMBER_COUNT {
            pipe.cmd("SADD")
                .arg(INTSET_KEY)
                .arg(idx.to_string())
                .ignore();
        }
        pipe.query_async::<()>(&mut *conn).await?;
        Ok(())
    }

    fn supported(&self, version: &Version) -> bool {
        version >= &Version::new(2, 8, 0)
    }

    fn assert(&self, _: &Version, items: &[Item]) -> AnyResult<()> {
        let item = items
            .iter()
            .filter(|item| matches!(item, Item::SetRecord { .. }))
            .find(|item| item.key().is_some_and(|key| key == INTSET_KEY))
            .ok_or_else(|| {
                anyhow!(
                    "Expected to find intset set record with key '{}' but none found. Total items: {}",
                    INTSET_KEY,
                    items.len()
                )
            })?;

        match item {
            Item::SetRecord {
                encoding,
                member_count,
                ..
            } => {
                ensure!(
                    matches!(encoding, SetEncoding::IntSet),
                    "unexpected set encoding {encoding:?}"
                );
                ensure!(
                    *member_count == INTSET_MEMBER_COUNT as u64,
                    "unexpected member count {member_count}, expected {}",
                    INTSET_MEMBER_COUNT
                );
            }
            _ => unreachable!("checked set record variant"),
        }

        Ok(())
    }
}
