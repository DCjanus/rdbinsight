use async_trait::async_trait;
use redis::aio::MultiplexedConnection;
use semver::Version;

use super::artifacts::ParsedRdbArtifacts;
use crate::helper::AnyResult;

#[async_trait]
pub trait TestFixture: Send + Sync {
    fn name(&self) -> &'static str;

    async fn load(&self, conn: &mut MultiplexedConnection) -> AnyResult<()>;

    fn supported(&self, _: &Version) -> bool {
        true
    }

    fn assert(&self, artifacts: &ParsedRdbArtifacts) -> AnyResult<()>;
}

pub mod simple_string;
pub use simple_string::SimpleStringFixture;
