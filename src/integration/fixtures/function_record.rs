use anyhow::{anyhow, ensure};
use async_trait::async_trait;
use redis::aio::MultiplexedConnection;
use semver::Version;

use super::TestFixture;
use crate::{helper::AnyResult, parser::Item};

#[derive(Debug, Default)]
pub struct FunctionRecordFixture;

impl FunctionRecordFixture {
    pub fn new() -> Self {
        Self
    }
}

const LUA_LIBRARY: &str = "#!lua name=integration
redis.register_function('fixture', function(keys, args) return 1 end)";

#[async_trait]
impl TestFixture for FunctionRecordFixture {
    fn name(&self) -> &'static str {
        "function_record_fixture"
    }

    async fn load(&self, conn: &mut MultiplexedConnection) -> AnyResult<()> {
        redis::cmd("FUNCTION")
            .arg("LOAD")
            .arg("REPLACE")
            .arg(LUA_LIBRARY)
            .query_async::<()>(conn)
            .await?;

        Ok(())
    }

    fn supported(&self, version: &Version) -> bool {
        version.major >= 7
    }

    fn assert(&self, _: &Version, items: &[Item]) -> AnyResult<()> {
        let Some(record) = items.iter().find_map(|item| match item {
            Item::FunctionRecord { rdb_size } => Some(*rdb_size),
            _ => None,
        }) else {
            return Err(anyhow!(
                "Expected to find at least one FunctionRecord item; total items: {}",
                items.len()
            ));
        };

        ensure!(
            record > 0,
            "function library size should be positive (got {record})"
        );

        Ok(())
    }
}
