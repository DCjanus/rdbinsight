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

pub mod simple_set;
pub mod simple_string;
pub mod simple_zset;
pub use simple_set::SimpleSetFixture;
pub use simple_string::SimpleStringFixture;
pub use simple_zset::SimpleZSetFixture;
