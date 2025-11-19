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

    fn assert(&self, items: &[Item]) -> AnyResult<()>;
}

pub mod simple_string;
pub use simple_string::SimpleStringFixture;
