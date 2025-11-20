use anyhow::{anyhow, ensure};
use async_trait::async_trait;
use redis::{aio::MultiplexedConnection, pipe};
use semver::Version;

use super::TestFixture;
use crate::{
    helper::AnyResult,
    parser::{Item, model::ListEncoding},
};

#[derive(Debug, Default)]
pub struct ListRecordFixture;

impl ListRecordFixture {
    pub fn new() -> Self {
        Self
    }
}

#[derive(Debug, Default)]
pub struct ListZipListRecordFixture;

impl ListZipListRecordFixture {
    pub fn new() -> Self {
        Self
    }
}

const KEY: &str = "integration:list";
const ELEMENT_COUNT: usize = 2048;
const ZIPLIST_KEY: &str = "integration:list:ziplist";
const ZIPLIST_ELEMENT_COUNT: usize = 300;

#[async_trait]
impl TestFixture for ListRecordFixture {
    fn name(&self) -> &'static str {
        "list_record_fixture"
    }

    async fn load(&self, conn: &mut MultiplexedConnection) -> AnyResult<()> {
        let payloads: Vec<String> = (0..ELEMENT_COUNT)
            .map(|idx| {
                format!(
                    "list-entry-{idx:04}-value-with-padding-to-avoid-ziplist--------------------------------",
                )
            })
            .collect();

        let mut pipe = pipe();
        for payload in &payloads {
            pipe.cmd("RPUSH").arg(KEY).arg(payload).ignore();
        }
        pipe.query_async::<()>(&mut *conn).await?;
        Ok(())
    }

    fn assert(&self, version: &Version, items: &[Item]) -> AnyResult<()> {
        let item = items
            .iter()
            .filter(|item| matches!(item, Item::ListRecord { .. }))
            .find(|item| item.key().is_some_and(|key| key == KEY))
            .ok_or_else(|| {
                anyhow!(
                    "Expected to find list record with key '{KEY}' but none found. Total items: {}",
                    items.len()
                )
            })?;

        match item {
            Item::ListRecord {
                encoding,
                member_count,
                ..
            } => {
                ensure!(
                    *member_count == ELEMENT_COUNT as u64,
                    "unexpected list length {member_count}, expected {ELEMENT_COUNT}"
                );
                let expected_encoding = match version.major {
                    0 | 1 | 2 => ListEncoding::List,
                    3..=6 => ListEncoding::QuickList,
                    7 => ListEncoding::QuickList,
                    _ => ListEncoding::QuickList2,
                };
                if version.major == 7 {
                    ensure!(
                        matches!(encoding, ListEncoding::QuickList | ListEncoding::QuickList2),
                        "unexpected list encoding {encoding:?} for Redis {version}"
                    );
                } else {
                    ensure!(
                        *encoding == expected_encoding,
                        "unexpected list encoding {encoding:?}, expected {expected_encoding:?} for Redis {version}"
                    );
                }
            }
            _ => unreachable!("checked list record variant"),
        }

        Ok(())
    }
}

#[async_trait]
impl TestFixture for ListZipListRecordFixture {
    fn name(&self) -> &'static str {
        "list_ziplist_record_fixture"
    }

    async fn load(&self, conn: &mut MultiplexedConnection) -> AnyResult<()> {
        let mut pipe = pipe();
        for idx in 0..ZIPLIST_ELEMENT_COUNT {
            pipe.cmd("RPUSH")
                .arg(ZIPLIST_KEY)
                .arg(format!("zl-{idx:04}"))
                .ignore();
        }
        pipe.query_async::<()>(&mut *conn).await?;
        Ok(())
    }

    fn supported(&self, version: &Version) -> bool {
        version >= &Version::new(2, 6, 0) && version < &Version::new(3, 2, 0)
    }

    fn assert(&self, _: &Version, items: &[Item]) -> AnyResult<()> {
        let item = items
            .iter()
            .filter(|item| matches!(item, Item::ListRecord { .. }))
            .find(|item| item.key().is_some_and(|key| key == ZIPLIST_KEY))
            .ok_or_else(|| {
                anyhow!(
                    "Expected to find ziplist list record with key '{ZIPLIST_KEY}' but none found. Total items: {}",
                    items.len()
                )
            })?;

        match item {
            Item::ListRecord {
                encoding,
                member_count,
                ..
            } => {
                ensure!(
                    matches!(encoding, ListEncoding::ZipList),
                    "unexpected list encoding {encoding:?}"
                );
                ensure!(
                    *member_count == ZIPLIST_ELEMENT_COUNT as u64,
                    "unexpected member count {member_count}, expected {ZIPLIST_ELEMENT_COUNT}"
                );
            }
            _ => unreachable!("checked list record variant"),
        }

        Ok(())
    }
}
