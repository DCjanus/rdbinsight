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

const KEY: &'static str = "integration:string";

#[async_trait]
impl TestFixture for SimpleStringFixture {
    fn name(&self) -> &'static str {
        "simple_string_fixture"
    }

    async fn load(&self, conn: &mut MultiplexedConnection) -> AnyResult<()> {
        redis::cmd("SET")
            .arg(KEY)
            .arg("integration-value")
            .query_async::<()>(conn)
            .await?;
        Ok(())
    }

    fn assert(&self, _: &Version, items: &[Item]) -> AnyResult<()> {
        let item = items
            .iter()
            .filter(|item| item.is_string_record())
            .filter(|item| item.key().is_some_and(|k| k == KEY))
            .next()
            .ok_or_else(|| {
                anyhow!(
                    "Expected to find string record with key '{}', but none found. Total items: {}",
                    KEY,
                    items.len()
                )
            })?;

        match item {
            Item::StringRecord { encoding, .. } => {
                ensure!(
                    matches!(encoding, StringEncoding::Raw),
                    "unexpected encoding {encoding:?}"
                );
            }
            _ => unreachable!("checked string record variant"),
        }

        Ok(())
    }
}
