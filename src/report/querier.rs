use anyhow::Result as AnyResult;
use bytes::Bytes;
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use serde_with::{base64::Base64, serde_as};
use time::OffsetDateTime;

use crate::config::ClickHouseConfig;

#[serde_as]
#[derive(Debug, Clone, Serialize)]
pub struct PrefixRecord {
    #[serde_as(as = "Base64")]
    pub prefix_base64: Bytes,
    pub instance: String,
    pub db: u64,
    pub r#type: String,
    pub rdb_size: u64,
    pub key_count: u64,
}

#[serde_as]
#[derive(Debug, Clone, Serialize)]
pub struct TopKeyRecord {
    #[serde_as(as = "Base64")]
    #[serde(rename = "key_base64")]
    pub key: Bytes,
    pub rdb_size: u64,
    pub member_count: Option<u64>,
    pub r#type: String,
    pub instance: String,
    pub db: u64,
    pub encoding: String,
    pub expire_at: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PrefixNode {
    pub prefix: String, // Complete prefix path from root to this node
    pub value: u64,
    pub children: Vec<PrefixNode>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DbAggregate {
    pub db: u64,
    pub key_count: u64,
    pub total_size: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TypeAggregate {
    pub data_type: String,
    pub key_count: u64,
    pub total_size: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct InstanceAggregate {
    pub instance: String,
    pub key_count: u64,
    pub total_size: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PrefixAggregate {
    pub prefix: String,
    pub total_size: u64,
    pub key_count: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReportData {
    pub cluster: String,
    pub batch: String,
    pub db_aggregates: Vec<DbAggregate>,
    pub type_aggregates: Vec<TypeAggregate>,
    pub instance_aggregates: Vec<InstanceAggregate>,
    pub top_keys: Vec<TopKeyRecord>,
    pub top_prefixes: Vec<PrefixAggregate>,
}

pub struct ClickHouseQuerier {
    client: Client,
    cluster: String,
    batch: String,
}

impl ClickHouseQuerier {
    pub async fn new(config: ClickHouseConfig, cluster: String, batch: String) -> AnyResult<Self> {
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

        Ok(Self {
            client,
            cluster,
            batch,
        })
    }

    pub async fn get_db_aggregates(&self) -> AnyResult<Vec<DbAggregate>> {
        #[derive(Debug, Row, Deserialize)]
        struct DbAggregateRow {
            db: u64,
            key_count: u64,
            total_size: u64,
        }

        let query = "
            SELECT 
                db,
                COUNT(*) as key_count,
                SUM(rdb_size) as total_size
            FROM redis_records_view
            WHERE cluster = ? AND batch = parseDateTime64BestEffort(?, 9, 'UTC')
            GROUP BY db
            ORDER BY db
        ";

        tracing::info!(
            operation = "db_aggregates_query_start",
            cluster = %self.cluster,
            batch = %self.batch,
            "Executing DB aggregates query"
        );

        let rows: Vec<DbAggregateRow> = self
            .client
            .query(query)
            .bind(&self.cluster)
            .bind(&self.batch)
            .fetch_all()
            .await?;

        let result: Vec<_> = rows
            .into_iter()
            .map(|row| DbAggregate {
                db: row.db,
                key_count: row.key_count,
                total_size: row.total_size,
            })
            .collect();

        tracing::info!(
            operation = "db_aggregates_query_complete",
            cluster = %self.cluster,
            batch = %self.batch,
            result_count = result.len(),
            "DB aggregates query completed"
        );

        Ok(result)
    }

    pub async fn get_type_aggregates(&self) -> AnyResult<Vec<TypeAggregate>> {
        #[derive(Debug, Row, Deserialize)]
        struct TypeAggregateRow {
            r#type: String,
            key_count: u64,
            total_size: u64,
        }

        let query = "
            SELECT 
                type,
                COUNT(*) as key_count,
                SUM(rdb_size) as total_size
            FROM redis_records_view
            WHERE cluster = ? AND batch = parseDateTime64BestEffort(?, 9, 'UTC')
            GROUP BY type
            ORDER BY total_size DESC
        ";

        tracing::info!(
            operation = "type_aggregates_query_start",
            cluster = %self.cluster,
            batch = %self.batch,
            "Executing type aggregates query"
        );

        let rows: Vec<TypeAggregateRow> = self
            .client
            .query(query)
            .bind(&self.cluster)
            .bind(&self.batch)
            .fetch_all()
            .await?;

        let result: Vec<_> = rows
            .into_iter()
            .map(|row| TypeAggregate {
                data_type: row.r#type,
                key_count: row.key_count,
                total_size: row.total_size,
            })
            .collect();

        tracing::info!(
            operation = "type_aggregates_query_complete",
            cluster = %self.cluster,
            batch = %self.batch,
            result_count = result.len(),
            "Type aggregates query completed"
        );

        Ok(result)
    }

    pub async fn get_instance_aggregates(&self) -> AnyResult<Vec<InstanceAggregate>> {
        #[derive(Debug, Row, Deserialize)]
        struct InstanceAggregateRow {
            instance: String,
            key_count: u64,
            total_size: u64,
        }

        let query = "
            SELECT 
                instance,
                COUNT(*) as key_count,
                SUM(rdb_size) as total_size
            FROM redis_records_view
            WHERE cluster = ? AND batch = parseDateTime64BestEffort(?, 9, 'UTC')
            GROUP BY instance
            ORDER BY total_size DESC
        ";

        tracing::info!(
            operation = "instance_aggregates_query_start",
            cluster = %self.cluster,
            batch = %self.batch,
            "Executing instance aggregates query"
        );

        let rows: Vec<InstanceAggregateRow> = self
            .client
            .query(query)
            .bind(&self.cluster)
            .bind(&self.batch)
            .fetch_all()
            .await?;

        let result: Vec<_> = rows
            .into_iter()
            .map(|row| InstanceAggregate {
                instance: row.instance,
                key_count: row.key_count,
                total_size: row.total_size,
            })
            .collect();

        tracing::info!(
            operation = "instance_aggregates_query_complete",
            cluster = %self.cluster,
            batch = %self.batch,
            result_count = result.len(),
            "Instance aggregates query completed"
        );

        Ok(result)
    }

    pub async fn get_top_prefixes(&self) -> AnyResult<Vec<PrefixAggregate>> {
        // TODO: correctly calculate top prefixes based on 1% of total size
        #[derive(Debug, Row, Deserialize)]
        struct TotalSizeRow {
            total_size: u64,
        }

        let total_query = "
            SELECT SUM(rdb_size) as total_size
            FROM redis_records_view
            WHERE cluster = ? AND batch = parseDateTime64BestEffort(?, 9, 'UTC')
        ";

        let total_result: Vec<TotalSizeRow> = self
            .client
            .query(total_query)
            .bind(&self.cluster)
            .bind(&self.batch)
            .fetch_all()
            .await?;

        let total_size = total_result.first().map(|r| r.total_size).unwrap_or(0);
        let threshold = total_size / 100; // 1% threshold

        tracing::info!(
            operation = "top_prefixes_calculation",
            cluster = %self.cluster,
            batch = %self.batch,
            total_size = total_size,
            threshold = threshold,
            "Calculated 1% threshold for top prefixes"
        );

        #[derive(Debug, Row, Deserialize)]
        struct PrefixAggregateRow {
            prefix: String,
            key_count: u64,
            total_size: u64,
        }

        let prefix_query = "
            WITH prefix_extraction AS (
                SELECT 
                    CASE 
                        WHEN positionUTF8(toString(key), ':') > 0 THEN 
                            substringUTF8(toString(key), 1, positionUTF8(toString(key), ':'))
                        WHEN positionUTF8(toString(key), '_') > 0 THEN 
                            substringUTF8(toString(key), 1, positionUTF8(toString(key), '_'))
                        WHEN positionUTF8(toString(key), '/') > 0 THEN 
                            substringUTF8(toString(key), 1, positionUTF8(toString(key), '/'))
                        ELSE toString(key)
                    END as prefix,
                    rdb_size
                FROM redis_records_view
                WHERE cluster = ? AND batch = parseDateTime64BestEffort(?, 9, 'UTC')
            )
            SELECT 
                prefix,
                COUNT(*) as key_count,
                SUM(rdb_size) as total_size
            FROM prefix_extraction
            GROUP BY prefix
            HAVING total_size >= ?
            ORDER BY total_size DESC
        ";

        tracing::info!(
            operation = "top_prefixes_query_start",
            cluster = %self.cluster,
            batch = %self.batch,
            threshold = threshold,
            "Executing top prefixes query"
        );

        let rows: Vec<PrefixAggregateRow> = self
            .client
            .query(prefix_query)
            .bind(&self.cluster)
            .bind(&self.batch)
            .bind(threshold)
            .fetch_all()
            .await?;

        let result: Vec<_> = rows
            .into_iter()
            .map(|row| PrefixAggregate {
                prefix: row.prefix,
                total_size: row.total_size,
                key_count: row.key_count,
            })
            .collect();

        tracing::info!(
            operation = "top_prefixes_query_complete",
            cluster = %self.cluster,
            batch = %self.batch,
            result_count = result.len(),
            threshold = threshold,
            "Top prefixes query completed"
        );

        Ok(result)
    }

    pub async fn get_top_keys(&self) -> AnyResult<Vec<TopKeyRecord>> {
        #[derive(Debug, Row, Deserialize)]
        struct TopKeyRow {
            key: Bytes,
            rdb_size: u64,
            member_count: u64,
            r#type: String,
            instance: String,
            db: u64,
            encoding: String,
            #[serde(with = "clickhouse::serde::time::datetime64::millis::option")]
            expire_at: Option<OffsetDateTime>,
        }

        let query = "
            SELECT 
                key,
                rdb_size,
                member_count,
                type,
                instance,
                db,
                encoding,
                expire_at
            FROM redis_records_view
            WHERE cluster = ? AND batch = parseDateTime64BestEffort(?, 9, 'UTC')
            ORDER BY rdb_size DESC
            LIMIT 100
        ";

        tracing::info!(
            operation = "top_keys_query_start",
            cluster = %self.cluster,
            batch = %self.batch,
            "Executing top 100 keys query"
        );

        let rows: Vec<TopKeyRow> = self
            .client
            .query(query)
            .bind(&self.cluster)
            .bind(&self.batch)
            .fetch_all()
            .await?;

        let result: Vec<_> = rows
            .into_iter()
            .map(|row| TopKeyRecord {
                key: row.key,
                rdb_size: row.rdb_size,
                member_count: Some(row.member_count),
                r#type: row.r#type,
                instance: row.instance,
                db: row.db,
                encoding: row.encoding,
                expire_at: row.expire_at.map(|dt| {
                    dt.format(&time::format_description::well_known::Rfc3339)
                        .unwrap_or_default()
                }),
            })
            .collect();

        tracing::info!(
            operation = "top_keys_query_complete",
            cluster = %self.cluster,
            batch = %self.batch,
            result_count = result.len(),
            "Top keys query completed"
        );

        Ok(result)
    }

    pub async fn generate_report_data(&self) -> AnyResult<ReportData> {
        tracing::info!(
            operation = "report_generation_start",
            cluster = %self.cluster,
            batch = %self.batch,
            "Starting report generation"
        );

        let (db_aggregates, type_aggregates, instance_aggregates, top_keys, top_prefixes) = tokio::try_join!(
            self.get_db_aggregates(),
            self.get_type_aggregates(),
            self.get_instance_aggregates(),
            self.get_top_keys(),
            self.get_top_prefixes()
        )?;

        let report_data = ReportData {
            cluster: self.cluster.clone(),
            batch: self.batch.clone(),
            db_aggregates,
            type_aggregates,
            instance_aggregates,
            top_keys,
            top_prefixes,
        };

        tracing::info!(
            operation = "report_generation_complete",
            cluster = %self.cluster,
            batch = %self.batch,
            db_count = report_data.db_aggregates.len(),
            type_count = report_data.type_aggregates.len(),
            instance_count = report_data.instance_aggregates.len(),
            top_keys_count = report_data.top_keys.len(),
            top_prefixes_count = report_data.top_prefixes.len(),
            "Report generation completed"
        );

        Ok(report_data)
    }
}
