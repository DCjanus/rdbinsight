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

pub mod function_record;
pub mod record_expiry;
pub mod record_hash;
pub mod record_list;
pub mod record_set;
pub mod record_stream;
pub mod record_string;
pub mod record_zset;
