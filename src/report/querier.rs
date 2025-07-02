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

#[derive(Debug, Clone, Serialize)]
pub struct Classification {
    pub r#type: String,
    pub instance: String,
    pub db: u64,
}

#[serde_as]
#[derive(Debug, Clone, Serialize)]
pub struct TopKeyRecord {
    #[serde_as(as = "Base64")]
    pub key_base64: Bytes,
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

    pub async fn get_all_classifications(&self) -> AnyResult<Vec<(Classification, u64)>> {
        #[derive(Debug, Row, Deserialize)]
        struct ClassificationRow {
            r#type: String,
            instance: String,
            db: u64,
            total_size: u64,
        }

        let query = "
            SELECT 
                type, 
                instance, 
                db,
                SUM(rdb_size) as total_size
            FROM redis_records_view
            WHERE cluster = ? AND batch = parseDateTime64BestEffort(?, 9, 'UTC')
            GROUP BY type, instance, db
            ORDER BY type, instance, db
        ";

        let rows: Vec<ClassificationRow> = self
            .client
            .query(query)
            .bind(&self.cluster)
            .bind(&self.batch)
            .fetch_all()
            .await?;

        let classifications = rows
            .into_iter()
            .map(|row| {
                (
                    Classification {
                        r#type: row.r#type,
                        instance: row.instance,
                        db: row.db,
                    },
                    row.total_size,
                )
            })
            .collect();

        Ok(classifications)
    }

    /// Generate unified prefix report data using two-stage aggregation
    /// Stage 1: Get empty prefix records for all classifications (cluster totals)
    /// Stage 2: Discover important prefixes and get their cross-classification details
    pub async fn generate_prefix_report_data(&self) -> AnyResult<Vec<PrefixRecord>> {
        // Stage 1: Get empty prefix records for all classifications
        // This provides cluster total data and can be reused in the aggregation process
        let mut final_records = self
            .get_prefix_cross_classification_details(&Bytes::new())
            .await
            .context("Failed to get bootstrapping records")?;

        // Configurable threshold: prefix is important if it's > 1% of classification total
        let significance_threshold_ratio = 0.01;

        // Stage 2: Discover important prefixes and get their cross-classification details
        let mut discovered_prefixes = HashSet::new();
        let mut classification_prefix_records = Vec::new();

        // First, collect all the classification total records to avoid borrowing issues
        let classification_totals: Vec<_> = final_records.clone();

        for record in &classification_totals {
            let classification = Classification {
                instance: record.instance.clone(),
                db: record.db,
                r#type: record.r#type.clone(),
            };

            let total_size = record.rdb_size;
            let significance_threshold = (total_size as f64 * significance_threshold_ratio) as u64;

            // Discover important prefixes for this classification
            let important_prefix_records = self
                .discover_important_prefixes_bfs(&classification, significance_threshold)
                .await?;

            // Collect unique prefixes and store the records
            for prefix_record in important_prefix_records {
                discovered_prefixes.insert(prefix_record.prefix_base64.clone());
                classification_prefix_records.push(prefix_record);
            }
        }

        // Add all the classification-specific prefix records
        final_records.extend(classification_prefix_records);

        // Get cross-classification details for all discovered prefixes
        // Note: We already have data for the classification where each prefix was discovered,
        // now we need to get data for other classifications
        for prefix in discovered_prefixes {
            let cross_classification_records = self
                .get_prefix_cross_classification_details(&prefix)
                .await?;

            // Add all records, we'll deduplicate at the end
            final_records.extend(cross_classification_records);
        }

        // Deduplicate records based on unique combination of prefix, instance, db, and type
        final_records.sort_by(|a, b| {
            a.prefix_base64
                .cmp(&b.prefix_base64)
                .then_with(|| a.instance.cmp(&b.instance))
                .then_with(|| a.db.cmp(&b.db))
                .then_with(|| a.r#type.cmp(&b.r#type))
        });
        final_records.dedup_by(|a, b| {
            a.prefix_base64 == b.prefix_base64
                && a.instance == b.instance
                && a.db == b.db
                && a.r#type == b.r#type
        });

        Ok(final_records)
    }

    /// Discover important prefixes within a single classification using BFS
    async fn discover_important_prefixes_bfs(
        &self,
        classification: &Classification,
        significance_threshold: u64,
    ) -> AnyResult<Vec<PrefixRecord>> {
        let mut important_prefixes = Vec::new();
        let mut prefixes_to_explore = vec![Bytes::new()]; // Start with empty prefix (root level)

        while !prefixes_to_explore.is_empty() {
            let mut next_level_prefixes = Vec::new();

            for current_prefix in prefixes_to_explore {
                let child_prefixes = self
                    .get_child_prefixes_with_count(classification, &current_prefix)
                    .await?;

                for (prefix, rdb_size, key_count) in child_prefixes {
                    if rdb_size >= significance_threshold {
                        // This prefix is important, record it with full statistics
                        important_prefixes.push(PrefixRecord {
                            prefix_base64: prefix.clone(),
                            instance: classification.instance.clone(),
                            db: classification.db,
                            r#type: classification.r#type.clone(),
                            rdb_size,
                            key_count,
                        });

                        // Add to next level for further exploration
                        next_level_prefixes.push(prefix);
                    }
                    // If not significant, we prune this branch (don't explore further)
                }
            }

            prefixes_to_explore = next_level_prefixes;
        }

        Ok(important_prefixes)
    }

    /// Get child prefixes with both size and count for a classification
    /// Also includes min/max keys for path compression optimization
    async fn get_child_prefixes_with_count(
        &self,
        classification: &Classification,
        current_prefix: &[u8],
    ) -> AnyResult<Vec<(Bytes, u64, u64)>> {
        #[derive(Debug, Row, Deserialize)]
        struct PrefixAggregateWithCountRow {
            prefix: Bytes,
            total_size: u64,
            key_count: u64,
            min_key: Bytes,
            max_key: Bytes,
        }

        let prefix_length = if current_prefix.is_empty() {
            1
        } else {
            current_prefix.len() + 1
        };

        let query = "
            SELECT 
                substring(key, 1, ?) as prefix,
                SUM(rdb_size) as total_size,
                COUNT(*) as key_count,
                MIN(key) as min_key,
                MAX(key) as max_key
            FROM redis_records_view
            WHERE cluster = ? AND batch = parseDateTime64BestEffort(?, 9, 'UTC') AND type = ? AND instance = ? AND db = ?
              AND startsWith(hex(key), ?)
              AND length(key) > ?
            GROUP BY prefix
            ORDER BY total_size DESC
        ";

        let rows: Vec<PrefixAggregateWithCountRow> = self
            .client
            .query(query)
            .bind(prefix_length)
            .bind(&self.cluster)
            .bind(&self.batch)
            .bind(&classification.r#type)
            .bind(&classification.instance)
            .bind(classification.db)
            // Use hex encoding to work around ClickHouse driver issue with empty &[u8]
            // which would be bound as [] instead of "" without encoding
            .bind(hex::encode(current_prefix))
            .bind(current_prefix.len())
            .fetch_all()
            .await?;

        // Apply path compression optimization
        Ok(rows
            .into_iter()
            .map(|row| {
                let common_prefix_len =
                    self.longest_common_prefix_length(&row.min_key, &row.max_key);
                let compressed_prefix = row.min_key.slice(0..common_prefix_len);
                assert!(compressed_prefix.len() >= row.prefix.len());

                (compressed_prefix, row.total_size, row.key_count)
            })
            .collect())
    }

    /// Find the length of the longest common prefix between two byte sequences
    /// More efficient than creating intermediate Bytes objects
    fn longest_common_prefix_length(&self, s1: &Bytes, s2: &Bytes) -> usize {
        let bytes1 = s1.as_ref();
        let bytes2 = s2.as_ref();
        let min_len = bytes1.len().min(bytes2.len());

        // Use iterator's position method for better performance
        bytes1[..min_len]
            .iter()
            .zip(&bytes2[..min_len])
            .position(|(a, b)| a != b)
            .unwrap_or(min_len)
    }

    /// Get cross-classification details for a specific prefix
    async fn get_prefix_cross_classification_details(
        &self,
        prefix: &Bytes,
    ) -> AnyResult<Vec<PrefixRecord>> {
        #[derive(Debug, Row, Deserialize)]
        struct CrossClassificationRow {
            instance: String,
            db: u64,
            r#type: String,
            key_count: u64,
            rdb_size: u64,
        }

        let query = "
            SELECT
                instance,
                db,
                type,
                count(*) AS key_count,
                sum(rdb_size) AS rdb_size
            FROM redis_records_view
            WHERE cluster = ? AND batch = parseDateTime64BestEffort(?, 9, 'UTC') AND startsWith(hex(key), ?)
            GROUP BY instance, db, type
        ";

        let rows: Vec<CrossClassificationRow> = self
            .client
            .query(query)
            .bind(&self.cluster)
            .bind(&self.batch)
            // Use hex encoding to work around ClickHouse driver issue with empty &[u8]
            // which would be bound as [] instead of "" without encoding
            .bind(hex::encode(prefix))
            .fetch_all()
            .await
            .context("Failed to get cross-classification details")?;

        Ok(rows
            .into_iter()
            .map(|row| PrefixRecord {
                prefix_base64: prefix.clone(),
                instance: row.instance,
                db: row.db,
                r#type: row.r#type,
                rdb_size: row.rdb_size,
                key_count: row.key_count,
            })
            .collect())
    }

    /// Get top 100 big keys for all classifications using a simpler approach
    pub async fn get_top_keys_for_all_classifications(&self) -> AnyResult<Vec<TopKeyRecord>> {
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

        // Simplified query without window function to avoid potential network issues
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
            LIMIT 1000
        ";

        let rows: Vec<TopKeyRow> = self
            .client
            .query(query)
            .bind(&self.cluster)
            .bind(&self.batch)
            .fetch_all()
            .await?;

        Ok(rows
            .into_iter()
            .map(|row| TopKeyRecord {
                key_base64: row.key,
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
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_querier() -> ClickHouseQuerier {
        ClickHouseQuerier {
            client: Client::default(),
            cluster: "test".to_string(),
            batch: "test".to_string(),
        }
    }

    #[test]
    fn test_longest_common_prefix_length() {
        let querier = create_test_querier();

        // Normal case with common prefix
        assert_eq!(
            querier.longest_common_prefix_length(
                &Bytes::from("hello_alice"),
                &Bytes::from("hello_zarah")
            ),
            6 // "hello_"
        );

        // No common prefix
        assert_eq!(
            querier.longest_common_prefix_length(&Bytes::from("apple"), &Bytes::from("banana")),
            0
        );

        // One string is prefix of another
        assert_eq!(
            querier
                .longest_common_prefix_length(&Bytes::from("user"), &Bytes::from("user_profile")),
            4 // "user"
        );

        // Identical strings
        assert_eq!(
            querier
                .longest_common_prefix_length(&Bytes::from("cache:key"), &Bytes::from("cache:key")),
            9 // "cache:key"
        );

        // Empty strings
        assert_eq!(
            querier.longest_common_prefix_length(&Bytes::from(""), &Bytes::from("")),
            0
        );

        // One empty string
        assert_eq!(
            querier.longest_common_prefix_length(&Bytes::from(""), &Bytes::from("test")),
            0
        );
    }
}
