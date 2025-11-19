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

const KEY: &str = "integration:list";
const ELEMENT_COUNT: usize = 2048;

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
