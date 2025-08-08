use std::collections::HashSet;

use anyhow::{Context, Result as AnyResult};
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

#[serde_as]
#[derive(Debug, Clone, Serialize)]
pub struct PrefixAggregate {
    #[serde_as(as = "Base64")]
    #[serde(rename = "prefix_base64")]
    pub prefix: Bytes,
    pub total_size: u64,
    pub key_count: u64,
}

#[derive(Debug)]
struct PrefixPartition {
    total_size: u64,
    key_count: u64,
    min_key: Bytes,
    max_key: Bytes,
}

#[serde_as]
#[derive(Debug, Clone, Serialize)]
pub struct BigKey {
    #[serde_as(as = "Base64")]
    #[serde(rename = "key_base64")]
    pub key: Bytes,
    pub instance: String,
    pub db: u64,
    pub r#type: String,
    pub rdb_size: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClusterIssues {
    pub big_keys: Vec<BigKey>,
    pub codis_slot_skew: bool,
    pub redis_cluster_slot_skew: bool,
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
    pub cluster_issues: ClusterIssues,
}

pub struct ClickHouseQuerier {
    client: Client,
    cluster: String,
    batch: String,
}

impl ClickHouseQuerier {
    pub async fn new(config: ClickHouseConfig, cluster: String, batch: String) -> AnyResult<Self> {
        let client = config
            .create_client()
            .context("Failed to create ClickHouse client")?;

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
        // Step 1: Initialize discovery - calculate threshold and get initial partitions
        let (threshold, initial_partitions) = self.initialize_prefix_discovery().await?;

        // Step 2: Explore prefix tree using depth-first search with LCP optimization
        let mut significant_prefixes = self
            .explore_prefix_tree(threshold, initial_partitions)
            .await?;

        // Step 3: Finalize results - sort and log completion
        self.finalize_discovery_results(&mut significant_prefixes, threshold);

        Ok(significant_prefixes)
    }

    async fn initialize_prefix_discovery(&self) -> AnyResult<(u64, Vec<PrefixPartition>)> {
        let initial_partitions = self.get_prefix_partitions(&Bytes::new()).await?;
        let total_size: u64 = initial_partitions.iter().map(|p| p.total_size).sum();
        // 1% threshold with a minimum of 1 to avoid zero threshold on tiny datasets
        let threshold = (total_size / 100).max(1);

        tracing::info!(
            operation = "dynamic_prefix_discovery_start",
            cluster = %self.cluster,
            batch = %self.batch,
            total_size = total_size,
            threshold = threshold,
            "Starting dynamic prefix discovery with LCP optimization"
        );

        Ok((threshold, initial_partitions))
    }

    async fn explore_prefix_tree(
        &self,
        threshold: u64,
        initial_partitions: Vec<PrefixPartition>,
    ) -> AnyResult<Vec<PrefixAggregate>> {
        let mut exploration_stack: Vec<Bytes> = Vec::new();
        let mut significant_prefixes: Vec<PrefixAggregate> = Vec::new();
        let mut visited_prefixes: HashSet<Bytes> = HashSet::new();

        // Process initial partitions directly
        self.process_partitions(
            &Bytes::new(),
            initial_partitions,
            threshold,
            &mut significant_prefixes,
            &mut exploration_stack,
            &mut visited_prefixes,
        )
        .await;

        // Continue exploring deeper prefixes
        while let Some(current_prefix) = exploration_stack.pop() {
            tracing::debug!(
                operation = "prefix_exploration",
                current_prefix = %String::from_utf8_lossy(&current_prefix),
                stack_size = exploration_stack.len(),
                "Exploring prefix"
            );

            let partitions = self.get_prefix_partitions(&current_prefix).await?;
            self.process_partitions(
                &current_prefix,
                partitions,
                threshold,
                &mut significant_prefixes,
                &mut exploration_stack,
                &mut visited_prefixes,
            )
            .await;
        }

        Ok(significant_prefixes)
    }

    async fn process_partitions(
        &self,
        parent_prefix: &Bytes,
        partitions: Vec<PrefixPartition>,
        threshold: u64,
        significant_prefixes: &mut Vec<PrefixAggregate>,
        exploration_stack: &mut Vec<Bytes>,
        visited_prefixes: &mut HashSet<Bytes>,
    ) {
        for partition in partitions {
            if partition.total_size >= threshold {
                let lcp = longest_common_prefix(&partition.min_key, &partition.max_key);

                // Only consider deeper prefixes; skip if LCP does not extend the parent
                if lcp.len() <= parent_prefix.len() {
                    continue;
                }

                // Deduplicate to avoid re-exploring the same prefix endlessly
                if visited_prefixes.insert(lcp.clone()) {
                    tracing::debug!(
                        operation = "significant_prefix_found",
                        prefix = %String::from_utf8_lossy(&lcp),
                        total_size = partition.total_size,
                        key_count = partition.key_count,
                        "Found significant prefix using LCP"
                    );

                    significant_prefixes.push(PrefixAggregate {
                        prefix: lcp.clone(),
                        total_size: partition.total_size,
                        key_count: partition.key_count,
                    });

                    exploration_stack.push(lcp);
                }
            }
        }
    }

    fn finalize_discovery_results(
        &self,
        significant_prefixes: &mut [PrefixAggregate],
        threshold: u64,
    ) {
        significant_prefixes.sort_by_key(|x| x.prefix.clone());

        tracing::info!(
            operation = "dynamic_prefix_discovery_complete",
            cluster = %self.cluster,
            batch = %self.batch,
            result_count = significant_prefixes.len(),
            threshold = threshold,
            "Dynamic prefix discovery completed"
        );
    }

    async fn get_prefix_partitions(
        &self,
        current_prefix: &Bytes,
    ) -> AnyResult<Vec<PrefixPartition>> {
        #[derive(Debug, Row, Deserialize)]
        struct PartitionRow {
            total_size: u64,
            key_count: u64,
            min_key: Bytes,
            max_key: Bytes,
        }

        let prefix_len = current_prefix.len();
        let query = format!(
            "SELECT
                sum(rdb_size) AS total_size,
                count(*) AS key_count,
                min(key) AS min_key,
                max(key) AS max_key
            FROM redis_records_view
            WHERE
                cluster = ? AND
                batch = parseDateTime64BestEffort(?, 9, 'UTC') AND
                startsWith(key, ?)
            GROUP BY
                substring(key, 1, {})",
            prefix_len + 1
        );

        let rows: Vec<PartitionRow> = self
            .client
            .query(&query)
            .bind(&self.cluster)
            .bind(&self.batch)
            .bind(serde_bytes::Bytes::new(current_prefix.as_ref()))
            .fetch_all()
            .await?;

        let partitions = rows
            .into_iter()
            .map(|row| PrefixPartition {
                total_size: row.total_size,
                key_count: row.key_count,
                min_key: row.min_key,
                max_key: row.max_key,
            })
            .collect();

        Ok(partitions)
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

    pub async fn query_big_keys(&self) -> AnyResult<Vec<BigKey>> {
        #[derive(Debug, Row, Deserialize)]
        struct BigKeyRow {
            r#type: String,
            key: Bytes,
            instance: String,
            db: u64,
            rdb_size: u64,
        }

        let query = "
            SELECT
                type,
                key,
                instance,
                db,
                rdb_size
            FROM (
                SELECT
                    type,
                    key,
                    instance,
                    db,
                    rdb_size,
                    ROW_NUMBER() OVER (PARTITION BY type ORDER BY rdb_size DESC) as rn
                FROM redis_records_view
                WHERE 
                    cluster = ? AND 
                    batch = parseDateTime64BestEffort(?, 9, 'UTC') AND
                    ((type = 'string' AND rdb_size > 1048576) OR (type != 'string' AND rdb_size > 1073741824))
            )
            WHERE rn = 1
            ORDER BY rdb_size DESC
        ";

        tracing::info!(
            operation = "big_keys_query_start",
            cluster = %self.cluster,
            batch = %self.batch,
            "Executing big keys query"
        );

        let rows: Vec<BigKeyRow> = self
            .client
            .query(query)
            .bind(&self.cluster)
            .bind(&self.batch)
            .fetch_all()
            .await?;

        let result: Vec<_> = rows
            .into_iter()
            .map(|row| BigKey {
                key: row.key,
                instance: row.instance,
                db: row.db,
                r#type: row.r#type,
                rdb_size: row.rdb_size,
            })
            .collect();

        tracing::info!(
            operation = "big_keys_query_complete",
            cluster = %self.cluster,
            batch = %self.batch,
            result_count = result.len(),
            "Big keys query completed"
        );

        Ok(result)
    }

    pub async fn query_codis_slot_skew(&self) -> AnyResult<bool> {
        #[derive(Debug, Row, Deserialize)]
        struct SlotSkewRow {
            skew_count: u64,
        }

        let query = "
            SELECT 
                count(*) as skew_count
            FROM (
                SELECT 
                    codis_slot,
                    count(DISTINCT instance) as instance_count
                FROM redis_records_view
                WHERE 
                    cluster = ? AND 
                    batch = parseDateTime64BestEffort(?, 9, 'UTC') AND
                    codis_slot IS NOT NULL
                GROUP BY codis_slot
                HAVING instance_count > 1
            )
        ";

        tracing::info!(
            operation = "codis_slot_skew_query_start",
            cluster = %self.cluster,
            batch = %self.batch,
            "Executing Codis slot skew query"
        );

        let rows: Vec<SlotSkewRow> = self
            .client
            .query(query)
            .bind(&self.cluster)
            .bind(&self.batch)
            .fetch_all()
            .await?;

        let has_skew = rows.first().map(|row| row.skew_count > 0).unwrap_or(false);

        tracing::info!(
            operation = "codis_slot_skew_query_complete",
            cluster = %self.cluster,
            batch = %self.batch,
            has_skew = has_skew,
            "Codis slot skew query completed"
        );

        Ok(has_skew)
    }

    pub async fn query_redis_cluster_slot_skew(&self) -> AnyResult<bool> {
        #[derive(Debug, Row, Deserialize)]
        struct SlotSkewRow {
            skew_count: u64,
        }

        let query = "
            SELECT 
                count(*) as skew_count
            FROM (
                SELECT 
                    redis_slot,
                    count(DISTINCT instance) as instance_count
                FROM redis_records_view
                WHERE 
                    cluster = ? AND 
                    batch = parseDateTime64BestEffort(?, 9, 'UTC') AND
                    redis_slot IS NOT NULL
                GROUP BY redis_slot
                HAVING instance_count > 1
            )
        ";

        tracing::info!(
            operation = "redis_cluster_slot_skew_query_start",
            cluster = %self.cluster,
            batch = %self.batch,
            "Executing Redis Cluster slot skew query"
        );

        let rows: Vec<SlotSkewRow> = self
            .client
            .query(query)
            .bind(&self.cluster)
            .bind(&self.batch)
            .fetch_all()
            .await?;

        let has_skew = rows.first().map(|row| row.skew_count > 0).unwrap_or(false);

        tracing::info!(
            operation = "redis_cluster_slot_skew_query_complete",
            cluster = %self.cluster,
            batch = %self.batch,
            has_skew = has_skew,
            "Redis Cluster slot skew query completed"
        );

        Ok(has_skew)
    }

    pub async fn generate_report_data(&self) -> AnyResult<ReportData> {
        tracing::info!(
            operation = "report_generation_start",
            cluster = %self.cluster,
            batch = %self.batch,
            "Starting report generation"
        );

        let (
            db_aggregates,
            type_aggregates,
            instance_aggregates,
            top_keys,
            top_prefixes,
            big_keys,
            codis_slot_skew,
            redis_cluster_slot_skew,
        ) = tokio::try_join!(
            self.get_db_aggregates(),
            self.get_type_aggregates(),
            self.get_instance_aggregates(),
            self.get_top_keys(),
            self.get_top_prefixes(),
            self.query_big_keys(),
            self.query_codis_slot_skew(),
            self.query_redis_cluster_slot_skew()
        )?;

        let cluster_issues = ClusterIssues {
            big_keys,
            codis_slot_skew,
            redis_cluster_slot_skew,
        };

        let report_data = ReportData {
            cluster: self.cluster.clone(),
            batch: self.batch.clone(),
            db_aggregates,
            type_aggregates,
            instance_aggregates,
            top_keys,
            top_prefixes,
            cluster_issues,
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
            big_keys_count = report_data.cluster_issues.big_keys.len(),
            codis_slot_skew = report_data.cluster_issues.codis_slot_skew,
            redis_cluster_slot_skew = report_data.cluster_issues.redis_cluster_slot_skew,
            "Report generation completed"
        );

        Ok(report_data)
    }
}

/// Calculate the longest common prefix (LCP) between two byte sequences
/// This is the core optimization for dynamic prefix discovery
fn longest_common_prefix(bytes1: &Bytes, bytes2: &Bytes) -> Bytes {
    let min_len = bytes1.len().min(bytes2.len());
    let mut common_len = 0;

    for i in 0..min_len {
        if bytes1[i] == bytes2[i] {
            common_len += 1;
        } else {
            break;
        }
    }

    bytes1.slice(0..common_len)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_longest_common_prefix() {
        assert_eq!(
            longest_common_prefix(
                &Bytes::from("user:profile:active:1"),
                &Bytes::from("user:profile:active:2")
            ),
            Bytes::from("user:profile:active:")
        );
        assert_eq!(
            longest_common_prefix(
                &Bytes::from("user:session:web:123"),
                &Bytes::from("user:session:api:456")
            ),
            Bytes::from("user:session:")
        );
        assert_eq!(
            longest_common_prefix(
                &Bytes::from("cache:page:desktop"),
                &Bytes::from("cache:page:mobile")
            ),
            Bytes::from("cache:page:")
        );
        assert_eq!(
            longest_common_prefix(&Bytes::from("user123"), &Bytes::from("user456")),
            Bytes::from("user")
        );
        assert_eq!(
            longest_common_prefix(&Bytes::from("abc"), &Bytes::from("def")),
            Bytes::new()
        );
        assert_eq!(
            longest_common_prefix(&Bytes::new(), &Bytes::from("abc")),
            Bytes::new()
        );
        assert_eq!(
            longest_common_prefix(&Bytes::from("abc"), &Bytes::new()),
            Bytes::new()
        );
        assert_eq!(
            longest_common_prefix(&Bytes::new(), &Bytes::new()),
            Bytes::new()
        );
        assert_eq!(
            longest_common_prefix(&Bytes::from("same"), &Bytes::from("same")),
            Bytes::from("same")
        );

        // Test with binary data
        let binary1 = Bytes::from(vec![0x00, 0x01, 0x02, 0x03, 0x04]);
        let binary2 = Bytes::from(vec![0x00, 0x01, 0x02, 0xFF, 0xFE]);
        let expected = Bytes::from(vec![0x00, 0x01, 0x02]);
        assert_eq!(longest_common_prefix(&binary1, &binary2), expected);
    }
}
