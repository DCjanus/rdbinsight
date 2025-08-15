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
        match self {
            Output::ClickHouse(ch_output) => ch_output.write_chunk(chunk).await,
            Output::Parquet(parquet_output) => {
                parquet_output.write_chunk(chunk).await?;
                Ok(())
            }
        }
    }

    pub async fn finalize_instance(&mut self, instance: &str) -> AnyResult<()> {
        match self {
            Output::ClickHouse(ch_output) => ch_output.finalize_instance(instance).await,
            Output::Parquet(parquet_output) => {
                parquet_output.finalize_instance(instance).await?;
                Ok(())
            }
        }
    }

    pub async fn finalize_batch(self) -> AnyResult<()> {
        match self {
            Output::ClickHouse(ch_output) => ch_output.finalize_batch().await,
            Output::Parquet(parquet_output) => {
                parquet_output.finalize_batch().await?;
                Ok(())
            }
        }
    }
}
