use anyhow::{anyhow, ensure};
use async_trait::async_trait;
use redis::aio::MultiplexedConnection;
use semver::Version;

use super::TestFixture;
use crate::{
    helper::AnyResult,
    parser::{Item, model::StringEncoding},
};

#[derive(Debug, Default)]
pub struct SimpleStringFixture;

impl SimpleStringFixture {
    pub fn new() -> Self {
        Self
    }
}

const KEY_RAW: &'static str = "integration:string:raw";
const KEY_INT: &'static str = "integration:string:int";

#[async_trait]
impl TestFixture for SimpleStringFixture {
    fn name(&self) -> &'static str {
        "simple_string_fixture"
    }

    async fn load(&self, conn: &mut MultiplexedConnection) -> AnyResult<()> {
        redis::cmd("SET")
            .arg(KEY_RAW)
            .arg("integration-value")
            .query_async::<()>(conn)
            .await?;

        redis::cmd("SET")
            .arg(KEY_INT)
            .arg("42")
            .query_async::<()>(conn)
            .await?;
        Ok(())
    }

    fn assert(&self, _: &Version, items: &[Item]) -> AnyResult<()> {
        let find_string_record = |key: &str| -> AnyResult<&Item> {
            items
                .iter()
                .filter(|item| item.is_string_record())
                .find(|item| item.key().is_some_and(|k| k == key))
                .ok_or_else(|| {
                    anyhow!(
                        "Expected to find string record with key '{}', but none found. Total items: {}",
                        key,
                        items.len()
                    )
                })
        };

        match find_string_record(KEY_RAW)? {
            Item::StringRecord { encoding, .. } => {
                ensure!(
                    matches!(encoding, StringEncoding::Raw),
                    "unexpected encoding {encoding:?} for key {KEY_RAW}"
                );
            }
            _ => unreachable!("checked string record variant"),
        }

        match find_string_record(KEY_INT)? {
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
