use anyhow::Context;
use bytes::Bytes;
use clickhouse::{Client, Row, insert::Insert};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tracing::info;

use crate::{config::ClickHouseConfig, helper::AnyResult, record::Record};

#[derive(Clone)]
pub struct ClickHouseOutput {
    client: Client,
    config: ClickHouseConfig,
    cluster: String,
    batch_ts: OffsetDateTime,
}

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

impl ClickHouseOutput {
    pub async fn new(
        config: ClickHouseConfig,
        cluster: String,
        batch_ts: OffsetDateTime,
    ) -> AnyResult<Self> {
        use tracing::debug;

        debug!(
            operation = "clickhouse_client_init",
            address = %config.address,
            cluster = %cluster,
            "Initializing ClickHouse client with batch info"
        );

        let client = config
            .create_client()
            .context("Failed to create ClickHouse client")?;
        let output = Self {
            client,
            config: config.clone(),
            cluster,
            batch_ts,
        };

        debug!("Validating ClickHouse tables and schema...");
        output.validate_or_create_tables().await?;
        debug!("ClickHouse initialization completed successfully");

        Ok(output)
    }

    async fn validate_or_create_tables(&self) -> AnyResult<()> {
        use tracing::debug;

        debug!("Checking existence of required ClickHouse tables...");

        // Check existence of all required tables/views
        let raw_table_exists: u64 = self
            .client
            .query("SELECT count() FROM system.tables WHERE name = 'redis_records_raw'")
            .fetch_one()
            .await?;

        let batch_table_exists: u64 = self
            .client
            .query("SELECT count() FROM system.tables WHERE name = 'import_batches_completed'")
            .fetch_one()
            .await?;

        let view_exists: u64 = self
            .client
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
                self.create_all_tables().await?;
                return Ok(());
            } else {
                debug!(
                    operation = "clickhouse_auto_create_tables_disabled",
                    "auto_create_tables is disabled, failing with missing tables error"
                );
                return Err(anyhow::anyhow!(
                    "Required ClickHouse tables do not exist. Please run 'rdbinsight misc clickhouse-schema' to get the DDL statements and create the required tables, or set 'auto_create_tables = true' in your configuration."
                ));
            }
        }

        // Partial state - some tables exist, others don't
        let missing_tables: Vec<&str> = [
            ("redis_records_raw", raw_table_exists == 0),
            ("import_batches_completed", batch_table_exists == 0),
            ("redis_records_view", view_exists == 0),
        ]
        .iter()
        .filter_map(|(name, missing)| if *missing { Some(*name) } else { None })
        .collect();

        debug!(
            operation = "clickhouse_partial_schema_state",
            missing_tables = %missing_tables.join(", "),
            "Partial ClickHouse schema state detected"
        );

        Err(anyhow::anyhow!(
            "Inconsistent ClickHouse schema state. Missing tables/views: {}. Please manually fix the schema by running 'rdbinsight misc clickhouse-schema' and creating the missing objects, or drop all existing tables and set 'auto_create_tables = true' to recreate everything.",
            missing_tables.join(", ")
        ))
    }

    async fn create_all_tables(&self) -> AnyResult<()> {
        use tracing::info;

        info!(
            operation = "clickhouse_tables_auto_create_start",
            "Auto-creating ClickHouse tables and views..."
        );

        // Execute SQL files in order
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
            self.client.query(sql).execute().await?;
        }

        info!(
            operation = "clickhouse_tables_created",
            "Successfully created all ClickHouse tables and views"
        );
        Ok(())
    }

    pub async fn write(&self, records: &[Record], instance: &str) -> AnyResult<()> {
        if records.is_empty() {
            return Ok(());
        }

        let mut insert: Insert<RedisRecordRow> = self.client.insert("redis_records_raw")?;
        for record in records {
            let row = self.record_to_row(record, instance);
            insert.write(&row).await?;
        }
        insert.end().await?;

        Ok(())
    }

    pub async fn write_chunk(&mut self, chunk: crate::output::types::Chunk) -> AnyResult<()> {
        if chunk.records.is_empty() {
            return Ok(());
        }

        let mut insert: Insert<RedisRecordRow> = self.client.insert("redis_records_raw")?;
        for record in &chunk.records {
            let row = self.record_to_row_from_chunk(record, &chunk);
            insert.write(&row).await?;
        }
        insert.end().await?;

        Ok(())
    }

    pub async fn finalize_instance(&mut self, _instance: &str) -> AnyResult<()> {
        // No-op for ClickHouse - instances don't need individual finalization
        Ok(())
    }

    pub async fn finalize_batch(self) -> AnyResult<()> {
        let completion_row = BatchCompletedRow {
            cluster: self.cluster,
            batch: self.batch_ts,
        };

        let mut insert: Insert<BatchCompletedRow> =
            self.client.insert("import_batches_completed")?;
        insert.write(&completion_row).await?;
        insert.end().await?;

        Ok(())
    }

    fn record_to_row(&self, record: &Record, instance: &str) -> RedisRecordRow {
        let key_bytes = match &record.key {
            crate::parser::core::raw::RDBStr::Str(bytes) => bytes.clone(),
            crate::parser::core::raw::RDBStr::Int(i) => Bytes::from(i.to_string()),
        };

        RedisRecordRow {
            cluster: self.cluster.clone(),
            batch: self.batch_ts,
            instance: instance.to_string(),
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

    fn record_to_row_from_chunk(
        &self,
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

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use url::Url;

    use super::*;
    use crate::{
        parser::{core::raw::RDBStr, model::StringEncoding},
        record::{RecordEncoding, RecordType},
    };

    #[test]
    fn test_record_to_row_conversion() {
        let client = Client::default();
        let config =
            ClickHouseConfig::new(Url::parse("http://localhost:8123").unwrap(), false, None)
                .unwrap();
        let output = ClickHouseOutput {
            client,
            config,
            cluster: "test-cluster".to_string(),
            batch_ts: OffsetDateTime::from_unix_timestamp(1640995200).unwrap(),
        };

        let record = Record::builder()
            .db(0)
            .key(RDBStr::Str(Bytes::from("test_key")))
            .r#type(RecordType::String)
            .encoding(RecordEncoding::String(StringEncoding::Raw))
            .rdb_size(100)
            .member_count(Some(1))
            .expire_at_ms(Some(1640995260000))
            .idle_seconds(Some(300))
            .freq(Some(5))
            .build();

        let row = output.record_to_row(&record, "127.0.0.1:6379");

        assert_eq!(row.cluster, "test-cluster");
        assert_eq!(row.batch, output.batch_ts);
        assert_eq!(row.instance, "127.0.0.1:6379");
        assert_eq!(row.db, 0);
        assert_eq!(row.key, Bytes::from("test_key"));
        assert_eq!(row.r#type, "string");
        assert_eq!(row.member_count, 1);
        assert_eq!(row.rdb_size, 100);
        assert_eq!(row.encoding, "raw");
        assert_eq!(
            row.expire_at,
            Some(OffsetDateTime::from_unix_timestamp_nanos(1640995260000 * 1_000_000).unwrap())
        );
    }
}
