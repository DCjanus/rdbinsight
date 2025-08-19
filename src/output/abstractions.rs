use async_trait::async_trait;

use crate::{helper::AnyResult, output::types::Chunk};

/// Batch-scoped output for the direct-writer pipeline.
///
/// Lifecycle:
/// - Per batch: `prepare_batch()` once → `create_writer(instance)` many → `finalize_batch()` once.
/// - `create_writer()` may be called upfront for many instances; keep it lightweight.
///   Defer heavy per-instance setup to `ChunkWriter::prepare_instance()` or the first `write_chunk()`.
/// - `prepare_batch()`/`finalize_batch()` should be idempotent and safe under retries.
#[async_trait]
pub trait Output: Send + Sync {
    /// Prepare batch-level resources. Idempotent.
    async fn prepare_batch(&self) -> AnyResult<()>;

    /// Create a writer bound to `instance`.
    /// May be called upfront for many instances; avoid costly work here.
    async fn create_writer(&self, instance: &str) -> AnyResult<Box<dyn ChunkWriter + Send>>;

    /// Finalize the batch after all writers complete. Called once and consumes `self`.
    async fn finalize_batch(self: Box<Self>) -> AnyResult<()>;
}

/// Per-instance writer used by a single async task.
///
/// Lifecycle:
/// - Per instance: optional `prepare_instance()` → `write_chunk()` many → `finalize_instance()` once.
#[async_trait]
pub trait ChunkWriter: Send {
    /// Optional per-instance initialization (default: no-op).
    async fn prepare_instance(&mut self) -> AnyResult<()> {
        Ok(())
    }

    /// Write a chunk of records (caller may add retries).
    async fn write_chunk(&mut self, chunk: Chunk) -> AnyResult<()>;

    /// Finalize this instance writer (no-op for some backends).
    async fn finalize_instance(&mut self) -> AnyResult<()>;
}
