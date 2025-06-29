use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::{config::ClickHouseConfig, helper::AnyResult, record::Record};

#[derive(Debug, Clone)]
pub struct BatchInfo {
    pub cluster: String,
    pub batch: OffsetDateTime,
    pub instance: String,
}

pub struct ClickHouseOutput {
    client: Client,
    config: ClickHouseConfig,
}

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct RedisRecordRow {
    cluster: String,
    #[serde(with = "clickhouse::serde::time::datetime64::nanos")]
    batch: OffsetDateTime,
    instance: String,
    db: u64,
    key: String,
    r#type: String,
    member_count: u64,
    rdb_size: u64,
    encoding: String,
    #[serde(with = "clickhouse::serde::time::datetime64::millis::option")]
    expire_at: Option<OffsetDateTime>,
    idle_seconds: Option<u64>,
    freq: Option<u8>,
}

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct BatchCompletedRow {
    cluster: String,
    #[serde(with = "clickhouse::serde::time::datetime64::nanos")]
    batch: OffsetDateTime,
}

impl ClickHouseOutput {
    pub async fn new(config: ClickHouseConfig) -> AnyResult<Self> {
        let mut client_builder = Client::default().with_url(&config.address);

        if let Some(username) = &config.username {
            client_builder = client_builder.with_user(username);
        }

        if let Some(password) = &config.password {
            client_builder = client_builder.with_password(password);
        }

        if let Some(database) = &config.database {
            client_builder = client_builder.with_database(database);
        }

        let client = client_builder;
        let output = Self {
            client,
            config: config.clone(),
        };
        output.validate_or_create_tables().await?;

        Ok(output)
    }

    async fn validate_or_create_tables(&self) -> AnyResult<()> {
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

        let tables_exist = [
            raw_table_exists > 0,
            batch_table_exists > 0,
            view_exists > 0,
        ];
        let all_exist = tables_exist.iter().all(|&exists| exists);
        let none_exist = tables_exist.iter().all(|&exists| !exists);

        if all_exist {
            // All tables exist, we're good to go
            return Ok(());
        }

        if none_exist {
            // No tables exist
            if self.config.auto_create_tables {
                // Create all tables at once
                self.create_all_tables().await?;
                return Ok(());
            } else {
                return Err(anyhow::anyhow!(
                    "Required ClickHouse tables do not exist. Please run 'rdbinsight misc print-clickhouse-schema' to get the DDL statements and create the required tables, or set 'auto_create_tables = true' in your configuration."
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

        Err(anyhow::anyhow!(
            "Inconsistent ClickHouse schema state. Missing tables/views: {}. Please manually fix the schema by running 'rdbinsight misc print-clickhouse-schema' and creating the missing objects, or drop all existing tables and set 'auto_create_tables = true' to recreate everything.",
            missing_tables.join(", ")
        ))
    }

    async fn create_all_tables(&self) -> AnyResult<()> {
        use tracing::info;

        info!("Auto-creating ClickHouse tables and views...");

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
            info!("Creating {}", description);
            self.client.query(sql).execute().await?;
        }

        info!("Successfully created all ClickHouse tables and views");
        Ok(())
    }

    pub async fn write(&self, records: &[Record], batch_info: &BatchInfo) -> AnyResult<()> {
        if records.is_empty() {
            return Ok(());
        }

        let rows: Vec<RedisRecordRow> = records
            .iter()
            .map(|record| self.record_to_row(record, batch_info))
            .collect();

        let mut insert = self.client.insert("redis_records_raw")?;
        for row in rows {
            insert.write(&row).await?;
        }
        insert.end().await?;

        Ok(())
    }

    pub async fn commit_batch(&self, batch_info: &BatchInfo) -> AnyResult<()> {
        let completion_row = BatchCompletedRow {
            cluster: batch_info.cluster.clone(),
            batch: batch_info.batch,
        };

        let mut insert = self.client.insert("import_batches_completed")?;
        insert.write(&completion_row).await?;
        insert.end().await?;

        Ok(())
    }

    fn record_to_row(&self, record: &Record, batch_info: &BatchInfo) -> RedisRecordRow {
        let key_str = match &record.key {
            crate::parser::core::raw::RDBStr::Str(bytes) => {
                String::from_utf8_lossy(bytes).into_owned()
            }
            crate::parser::core::raw::RDBStr::Int(i) => i.to_string(),
        };

        RedisRecordRow {
            cluster: batch_info.cluster.clone(),
            batch: batch_info.batch,
            instance: batch_info.instance.clone(),
            db: record.db,
            key: key_str,
            r#type: record.type_name().to_string(),
            member_count: record.member_count.unwrap_or(0),
            rdb_size: record.rdb_size,
            encoding: record.encoding_name(),
            expire_at: record.expire_at_ms.map(|ms| {
                OffsetDateTime::from_unix_timestamp_nanos((ms as i128) * 1_000_000).unwrap()
            }),
            idle_seconds: record.idle_seconds,
            freq: record.freq,
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::{
        parser::{core::raw::RDBStr, model::StringEncoding},
        record::{RecordEncoding, RecordType},
    };

    #[test]
    fn test_record_to_row_conversion() {
        let client = Client::default();
        let config = ClickHouseConfig {
            address: "http://localhost:8123".to_string(),
            username: None,
            password: None,
            database: None,
            auto_create_tables: false,
        };
        let output = ClickHouseOutput { client, config };

        let batch_info = BatchInfo {
            cluster: "test-cluster".to_string(),
            batch: OffsetDateTime::from_unix_timestamp(1640995200).unwrap(), // 2022-01-01
            instance: "127.0.0.1:6379".to_string(),
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

        let row = output.record_to_row(&record, &batch_info);

        assert_eq!(row.cluster, "test-cluster");
        assert_eq!(row.batch, batch_info.batch);
        assert_eq!(row.instance, "127.0.0.1:6379");
        assert_eq!(row.db, 0);
        assert_eq!(row.key, "test_key");
        assert_eq!(row.r#type, "string");
        assert_eq!(row.member_count, 1);
        assert_eq!(row.rdb_size, 100);
        assert_eq!(row.encoding, "string:raw");
        assert_eq!(
            row.expire_at,
            Some(OffsetDateTime::from_unix_timestamp_nanos(1640995260000 * 1_000_000).unwrap())
        );
        assert_eq!(row.idle_seconds, Some(300));
        assert_eq!(row.freq, Some(5));

        let int_record = Record::builder()
            .db(1)
            .key(RDBStr::Int(42))
            .r#type(RecordType::Hash)
            .encoding(RecordEncoding::String(StringEncoding::Int))
            .rdb_size(50)
            .member_count(Some(10))
            .build();

        let int_row = output.record_to_row(&int_record, &batch_info);
        assert_eq!(int_row.key, "42");
        assert_eq!(int_row.db, 1);
        assert_eq!(int_row.r#type, "hash");
        assert_eq!(int_row.member_count, 10);
        assert_eq!(int_row.expire_at, None);
    }
}
