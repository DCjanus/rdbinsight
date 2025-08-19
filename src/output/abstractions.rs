use async_trait::async_trait;

use crate::{helper::AnyResult, output::types::Chunk};

#[async_trait]
pub trait Output: Send + Sync {
    /// Prepare batch-level resources (e.g., ensure tables exist or create batch directories).
    /// Must be idempotent and safe to call once before writers are created.
    async fn prepare_batch(&self) -> AnyResult<()>;

    /// Create a writer bound to a specific instance.
    async fn create_writer(&self, instance: &str) -> AnyResult<Box<dyn ChunkWriter + Send>>;

    /// Finalize the batch after all instance writers have completed.
    /// Consumes the output to guarantee it is called at most once.
    async fn finalize_batch(self: Box<Self>) -> AnyResult<()>;
}

#[async_trait]
pub trait ChunkWriter: Send {
    /// Write a chunk of records. Caller may wrap with retry policy if needed.
    async fn write_chunk(&mut self, chunk: Chunk) -> AnyResult<()>;

    /// Finalize this instance writer (no-op for some backends).
    async fn finalize_instance(&mut self) -> AnyResult<()>;
}
