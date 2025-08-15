use crate::{
    helper::AnyResult,
    output::{clickhouse::ClickHouseOutput, parquet::ParquetOutput, types::Chunk},
};

// TODO: try to remove this
#[allow(clippy::large_enum_variant)]
pub enum Output {
    ClickHouse(ClickHouseOutput),
    Parquet(ParquetOutput),
}

impl Output {
    pub async fn write_chunk(&mut self, chunk: Chunk) -> AnyResult<()> {
        let _ = chunk;
        todo!("Implement in later phase");
    }

    pub async fn finalize_instance(&mut self, instance: &str) -> AnyResult<()> {
        let _ = instance;
        todo!("Implement in later phase");
    }

    pub async fn finalize_batch(self) -> AnyResult<()> {
        todo!("Implement in later phase");
    }
}
