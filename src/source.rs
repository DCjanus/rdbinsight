pub mod rdb_file;

use async_trait::async_trait;
use bytes::BytesMut;

use crate::helper::AnyResult;

#[async_trait]
pub trait Source {
    async fn prepare(&mut self) -> AnyResult<()>;
    async fn read(&mut self, out: &mut BytesMut) -> AnyResult<usize>;
}
