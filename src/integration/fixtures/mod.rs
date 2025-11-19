use async_trait::async_trait;
use redis::aio::MultiplexedConnection;
use semver::Version;

use crate::{helper::AnyResult, parser::Item};

#[async_trait]
pub trait TestFixture: Send + Sync {
    fn name(&self) -> &'static str;

    async fn load(&self, conn: &mut MultiplexedConnection) -> AnyResult<()>;

    fn supported(&self, _: &Version) -> bool {
        true
    }

    fn assert(&self, version: &Version, items: &[Item]) -> AnyResult<()>;
}

pub mod simple_expiry;
pub mod simple_hash;
pub mod simple_list;
pub mod simple_set;
pub mod simple_stream;
pub mod simple_string;
pub mod simple_zset;
