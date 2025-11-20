use anyhow::{anyhow, ensure};
use async_trait::async_trait;
use redis::aio::MultiplexedConnection;
use semver::Version;

use super::TestFixture;
use crate::{
    helper::AnyResult,
    parser::{Item, model::StringEncoding},
};

const KEY_RAW: &str = "integration:string:raw";
const KEY_INT: &str = "integration:string:int";

fn find_string_record<'items>(items: &'items [Item], key: &str) -> AnyResult<&'items Item> {
    items
        .iter()
        .filter(|item| item.is_string_record())
        .find(|item| item.key().is_some_and(|k| k == key))
        .ok_or_else(|| {
            anyhow!(
                "Expected to find string record with key '{key}', but none found. Total items: {}",
                items.len()
            )
        })
}

#[derive(Debug, Default)]
pub struct RawStringRecordFixture;

impl RawStringRecordFixture {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl TestFixture for RawStringRecordFixture {
    fn name(&self) -> &'static str {
        "raw_string_record_fixture"
    }

    fn supported(&self, _: &Version) -> bool {
        true
    }

    async fn load(&self, conn: &mut MultiplexedConnection) -> AnyResult<()> {
        redis::cmd("SET")
            .arg(KEY_RAW)
            .arg("integration-value")
            .query_async::<()>(conn)
            .await?;
        Ok(())
    }

    fn assert(&self, _: &Version, items: &[Item]) -> AnyResult<()> {
        match find_string_record(items, KEY_RAW)? {
            Item::StringRecord { encoding, .. } => {
                ensure!(
                    matches!(encoding, StringEncoding::Raw),
                    "unexpected encoding {encoding:?} for key {KEY_RAW}"
                );
            }
            _ => unreachable!("checked string record variant"),
        }

        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct IntStringRecordFixture;

impl IntStringRecordFixture {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl TestFixture for IntStringRecordFixture {
    fn name(&self) -> &'static str {
        "int_string_record_fixture"
    }

    fn supported(&self, version: &Version) -> bool {
        version.major >= 1
    }

    async fn load(&self, conn: &mut MultiplexedConnection) -> AnyResult<()> {
        redis::cmd("SET")
            .arg(KEY_INT)
            .arg("42")
            .query_async::<()>(conn)
            .await?;
        Ok(())
    }

    fn assert(&self, _: &Version, items: &[Item]) -> AnyResult<()> {
        match find_string_record(items, KEY_INT)? {
            Item::StringRecord { encoding, .. } => {
                ensure!(
                    matches!(encoding, StringEncoding::Int),
                    "unexpected encoding {encoding:?} for key {KEY_INT}"
                );
            }
            _ => unreachable!("checked string record variant"),
        }

        Ok(())
    }
}
