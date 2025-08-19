use anyhow::Context;
use bytes::Bytes;
use clickhouse::{Client, Row, insert::Insert};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tracing::info;

use crate::{config::ClickHouseConfig, helper::AnyResult, record::Record};

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct RedisRecordRow {
    cluster: String,
    #[serde(with = "clickhouse::serde::time::datetime64::nanos")]
    batch: OffsetDateTime,
    instance: String,
    db: u64,
    key: Bytes,
    r#type: String,
    member_count: u64,
    rdb_size: u64,
    encoding: String,
    #[serde(with = "clickhouse::serde::time::datetime64::millis::option")]
    expire_at: Option<OffsetDateTime>,
    idle_seconds: Option<u64>,
    freq: Option<u8>,
    codis_slot: Option<u16>,
    redis_slot: Option<u16>,
}

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct BatchCompletedRow {
    cluster: String,
    #[serde(with = "clickhouse::serde::time::datetime64::nanos")]
    batch: OffsetDateTime,
}

pub struct ClickHouseOutput {
    config: ClickHouseConfig,
    cluster: String,
    batch_ts: OffsetDateTime,
}

impl ClickHouseOutput {
    pub fn new(config: ClickHouseConfig, cluster: String, batch_ts: OffsetDateTime) -> Self {
        Self {
            config,
            cluster,
            batch_ts,
        }
    }

    async fn ensure_tables(&self, client: &Client) -> AnyResult<()> {
        use tracing::debug;

        debug!(
            operation = "clickhouse_table_existence_check_start",
            "Checking existence of required ClickHouse tables..."
        );

        let raw_table_exists: u64 = client
            .query("SELECT count() FROM system.tables WHERE name = 'redis_records_raw'")
            .fetch_one()
            .await?;

        let batch_table_exists: u64 = client
            .query("SELECT count() FROM system.tables WHERE name = 'import_batches_completed'")
            .fetch_one()
            .await?;

        let view_exists: u64 = client
            .query("SELECT count() FROM system.tables WHERE name = 'redis_records_view' AND engine = 'View'")
            .fetch_one()
            .await?;

        debug!(
            operation = "clickhouse_table_existence_check",
            raw_table_exists = raw_table_exists > 0,
            batch_table_exists = batch_table_exists > 0,
            view_exists = view_exists > 0,
            "Table existence check"
        );

        let tables_exist = [
            raw_table_exists > 0,
            batch_table_exists > 0,
            view_exists > 0,
        ];
        let all_exist = tables_exist.iter().all(|&exists| exists);
        let none_exist = tables_exist.iter().all(|&exists| !exists);

        if all_exist {
            debug!(
                operation = "clickhouse_tables_exist",
                "All required ClickHouse tables exist"
            );
            return Ok(());
        }

        if none_exist {
            debug!(
                operation = "clickhouse_tables_missing",
                "No ClickHouse tables exist"
            );
            if self.config.auto_create_tables {
                debug!(
                    operation = "clickhouse_auto_create_tables_enabled",
                    "auto_create_tables is enabled, creating tables..."
                );
                self.create_all_tables(client).await?;
                return Ok(());
            } else {
                return Err(anyhow::anyhow!(
                    "Required ClickHouse tables do not exist. Please run 'rdbinsight misc clickhouse-schema' to get the DDL statements and create the required tables, or set 'auto_create_tables = true' in your configuration."
                ));
            }
        }

        let missing_tables: Vec<&str> = [
            ("redis_records_raw", raw_table_exists == 0),
            ("import_batches_completed", batch_table_exists == 0),
            ("redis_records_view", view_exists == 0),
        ]
        .iter()
        .filter_map(|(name, missing)| if *missing { Some(*name) } else { None })
        .collect();

        Err(anyhow::anyhow!(
            "Inconsistent ClickHouse schema state. Missing tables/views: {}. Please manually fix the schema by running 'rdbinsight misc clickhouse-schema' and creating the missing objects, or drop all existing tables and set 'auto_create_tables = true' to recreate everything.",
            missing_tables.join(", ")
        ))
    }

    async fn create_all_tables(&self, client: &Client) -> AnyResult<()> {
        info!(
            operation = "clickhouse_tables_auto_create_start",
            "Auto-creating ClickHouse tables and views..."
        );

        let sql_files = [
            (
                "redis_records_raw table",
                include_str!("../../sql/01_create_redis_records_raw.sql"),
            ),
            (
                "import_batches_completed table",
                include_str!("../../sql/02_create_import_batches_completed.sql"),
            ),
            (
                "redis_records_view",
                include_str!("../../sql/03_create_redis_records_view.sql"),
            ),
        ];

        for (description, sql) in sql_files {
            info!(
                operation = "clickhouse_table_create",
                description = %description,
                "Creating ClickHouse table/view"
            );
            client.query(sql).execute().await?;
        }

        info!(
            operation = "clickhouse_tables_created",
            "Successfully created all ClickHouse tables and views"
        );
        Ok(())
    }

    fn record_to_row_from_chunk(
        record: &Record,
        chunk: &crate::output::types::Chunk,
    ) -> RedisRecordRow {
        let key_bytes = match &record.key {
            crate::parser::core::raw::RDBStr::Str(bytes) => bytes.clone(),
            crate::parser::core::raw::RDBStr::Int(i) => Bytes::from(i.to_string()),
        };

        RedisRecordRow {
            cluster: chunk.cluster.clone(),
            batch: chunk.batch_ts,
            instance: chunk.instance.clone(),
            db: record.db,
            key: key_bytes,
            r#type: record.type_name().to_string(),
            member_count: record.member_count.unwrap_or(0),
            rdb_size: record.rdb_size,
            encoding: record.encoding_name(),
            expire_at: record.expire_at_ms.map(|ms| {
                OffsetDateTime::from_unix_timestamp_nanos((ms as i128) * 1_000_000).unwrap()
            }),
            idle_seconds: record.idle_seconds,
            freq: record.freq,
            codis_slot: record.codis_slot,
            redis_slot: record.redis_slot,
        }
    }
}

#[async_trait::async_trait]
impl crate::output::abstractions::Output for ClickHouseOutput {
    async fn prepare_batch(&self) -> AnyResult<()> {
        use tracing::debug;
        debug!(
            operation = "clickhouse_prepare_batch",
            cluster = %self.cluster,
            "Preparing ClickHouse batch (idempotent)"
        );
        let client = self
            .config
            .create_client()
            .context("Failed to create ClickHouse client")?;
        self.ensure_tables(&client).await
    }

    async fn create_writer(
        &self,
        _instance: &str,
    ) -> AnyResult<Box<dyn crate::output::abstractions::ChunkWriter + Send>> {
        let client = self
            .config
            .create_client()
            .context("Failed to create ClickHouse client for writer")?;
        Ok(Box::new(ClickHouseChunkWriter { client }))
    }

    async fn finalize_batch(self: Box<Self>) -> AnyResult<()> {
        use tracing::info;
        let client = self
            .config
            .create_client()
            .context("Failed to create ClickHouse client for finalize_batch")?;

        let completion_row = BatchCompletedRow {
            cluster: self.cluster,
            batch: self.batch_ts,
        };

        info!(
            operation = "clickhouse_finalize_batch",
            "Writing batch completion row"
        );
        let mut insert: Insert<BatchCompletedRow> = client.insert("import_batches_completed")?;
        insert.write(&completion_row).await?;
        insert.end().await?;
        Ok(())
    }
}

pub struct ClickHouseChunkWriter {
    client: Client,
}

#[async_trait::async_trait]
impl crate::output::abstractions::ChunkWriter for ClickHouseChunkWriter {
    async fn write_chunk(&mut self, chunk: crate::output::types::Chunk) -> AnyResult<()> {
        if chunk.records.is_empty() {
            return Ok(());
        }
        let mut insert: Insert<RedisRecordRow> = self
            .client
            .insert("redis_records_raw")?;
        for record in &chunk.records {
            let row = ClickHouseOutput::record_to_row_from_chunk(record, &chunk);
            insert.write(&row).await?;
        }
        insert.end().await?;
        Ok(())
    }

    async fn finalize_instance(&mut self) -> AnyResult<()> {
        Ok(())
    }
}
