use std::pin::Pin;

use async_trait::async_trait;
use tokio::io::AsyncRead;

use crate::helper::AnyResult;

pub mod cluster;
pub mod codis;
pub mod file;
pub mod redis_stream;
pub mod standalone;

/// A stream of RDB data that can be read asynchronously.
/// It might require an asynchronous preparation step before reading.
#[async_trait]
pub trait RDBStream: AsyncRead + Send + Unpin {
    /// Prepares the stream for reading. This could involve actions like
    /// connecting to a remote server, authenticating, and initiating
    // the data transfer.
    async fn prepare(&mut self) -> AnyResult<()>;

    /// Returns the instance identifier for this stream.
    /// For standalone Redis, this is typically the address.
    /// For cluster, this is the address of the specific node.
    fn instance(&self) -> String;
}

#[async_trait]
pub trait RdbSourceConfig {
    /// Gets a list of RDB streams to be processed.
    /// This method is responsible for discovering all the RDB sources,
    /// for example, all master nodes in a Redis Cluster.
    async fn get_rdb_streams(&self) -> AnyResult<Vec<Pin<Box<dyn RDBStream>>>>;

    /// Preprocesses the source configuration to handle dynamic values.
    /// This method should be called before using the configuration.
    /// For example, it can fetch cluster_name from external APIs.
    async fn preprocess(&mut self) -> AnyResult<()> {
        // Default implementation does nothing
        Ok(())
    }
}
