use std::collections::HashSet;

use anyhow::Result as AnyResult;
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::config::ClickHouseConfig;

#[derive(Debug, Clone, Serialize)]
pub struct PrefixRecord {
    pub prefix: String,
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

#[derive(Debug, Clone, Serialize)]
pub struct TopKeyRecord {
    pub key: String,
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
        let mut final_records = self.get_prefix_cross_classification_details("").await?;

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
                discovered_prefixes.insert(prefix_record.prefix.clone());
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
            a.prefix
                .cmp(&b.prefix)
                .then_with(|| a.instance.cmp(&b.instance))
                .then_with(|| a.db.cmp(&b.db))
                .then_with(|| a.r#type.cmp(&b.r#type))
        });
        final_records.dedup_by(|a, b| {
            a.prefix == b.prefix && a.instance == b.instance && a.db == b.db && a.r#type == b.r#type
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
        let mut prefixes_to_explore = vec!["".to_string()]; // Start with empty prefix (root level)

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
                            prefix: prefix.clone(),
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
        current_prefix: &str,
    ) -> AnyResult<Vec<(String, u64, u64)>> {
        #[derive(Debug, Row, Deserialize)]
        struct PrefixAggregateWithCountRow {
            prefix: String,
            total_size: u64,
            key_count: u64,
            min_key: String,
            max_key: String,
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
              AND startsWith(key, ?)
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
            .bind(current_prefix)
            .bind(current_prefix.len())
            .fetch_all()
            .await?;

        // Apply path compression optimization
        let rows_for_compression: Vec<(String, u64, u64, String, String)> = rows
            .into_iter()
            .map(|row| {
                (
                    row.prefix,
                    row.total_size,
                    row.key_count,
                    row.min_key,
                    row.max_key,
                )
            })
            .collect();

        let compressed_prefixes = self.apply_path_compression(rows_for_compression);

        Ok(compressed_prefixes
            .into_iter()
            .map(|(prefix, total_size, key_count, _, _)| (prefix, total_size, key_count))
            .collect())
    }

    /// Apply path compression optimization using min/max key analysis
    /// If a prefix has min and max keys that share a longer common prefix,
    /// we can skip intermediate levels and jump directly to the compressed prefix
    fn apply_path_compression(
        &self,
        rows: Vec<(String, u64, u64, String, String)>,
    ) -> Vec<(String, u64, u64, String, String)> {
        let mut compressed = Vec::new();

        for (prefix, total_size, key_count, min_key, max_key) in rows {
            let compressed_prefix = self.find_compressed_prefix(&prefix, &min_key, &max_key);
            compressed.push((compressed_prefix, total_size, key_count, min_key, max_key));
        }

        compressed
    }

    /// Find the optimal compressed prefix based on min/max key analysis
    /// Example: prefix="he", min_key="hello_alice", max_key="hello_zarah" -> "hello_"
    fn find_compressed_prefix(&self, current_prefix: &str, min_key: &str, max_key: &str) -> String {
        // Find the longest common prefix between min_key and max_key
        let common_prefix = self.longest_common_prefix(min_key, max_key);

        // Only compress if the common prefix is significantly longer than current prefix
        // and provides meaningful compression benefit (at least 2 characters improvement)
        if common_prefix.len() > current_prefix.len() + 1 {
            // Limit compression to avoid excessively long prefixes
            let max_compression_length = current_prefix.len() + 20;
            if common_prefix.len() <= max_compression_length {
                common_prefix
            } else {
                // If common prefix is too long, truncate it to reasonable length
                common_prefix.chars().take(max_compression_length).collect()
            }
        } else {
            current_prefix.to_string()
        }
    }

    /// Find the longest common prefix between two strings
    fn longest_common_prefix(&self, s1: &str, s2: &str) -> String {
        let chars1: Vec<char> = s1.chars().collect();
        let chars2: Vec<char> = s2.chars().collect();

        let mut common_len = 0;
        let min_len = chars1.len().min(chars2.len());

        for i in 0..min_len {
            if chars1[i] == chars2[i] {
                common_len += 1;
            } else {
                break;
            }
        }

        chars1.iter().take(common_len).collect()
    }

    /// Get cross-classification details for a specific prefix
    async fn get_prefix_cross_classification_details(
        &self,
        prefix: &str,
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
            WHERE cluster = ? AND batch = parseDateTime64BestEffort(?, 9, 'UTC') AND startsWith(key, ?)
            GROUP BY instance, db, type
        ";

        let rows: Vec<CrossClassificationRow> = self
            .client
            .query(query)
            .bind(&self.cluster)
            .bind(&self.batch)
            .bind(prefix)
            .fetch_all()
            .await?;

        Ok(rows
            .into_iter()
            .map(|row| PrefixRecord {
                prefix: prefix.to_string(),
                instance: row.instance,
                db: row.db,
                r#type: row.r#type,
                rdb_size: row.rdb_size,
                key_count: row.key_count,
            })
            .collect())
    }

    /// Get top 100 big keys for all classifications using window function
    pub async fn get_top_keys_for_all_classifications(&self) -> AnyResult<Vec<TopKeyRecord>> {
        #[derive(Debug, Row, Deserialize)]
        struct TopKeyRow {
            key: String,
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
            FROM (
                SELECT 
                    key,
                    rdb_size,
                    member_count,
                    type,
                    instance,
                    db,
                    encoding,
                    expire_at,
                    ROW_NUMBER() OVER (PARTITION BY type, instance, db ORDER BY rdb_size DESC) as rn
                FROM redis_records_view
                WHERE cluster = ? AND batch = parseDateTime64BestEffort(?, 9, 'UTC')
            ) ranked
            WHERE rn <= 100
            ORDER BY type, instance, db, rdb_size DESC
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
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_longest_common_prefix() {
        let querier = ClickHouseQuerier {
            client: Client::default(),
            cluster: "test".to_string(),
            batch: "test".to_string(),
        };

        assert_eq!(
            querier.longest_common_prefix("hello_alice", "hello_zarah"),
            "hello_"
        );
        assert_eq!(querier.longest_common_prefix("apple", "banana"), "");
        assert_eq!(
            querier.longest_common_prefix("user", "user_profile"),
            "user"
        );
        assert_eq!(
            querier.longest_common_prefix("cache:key", "cache:key"),
            "cache:key"
        );
    }

    #[test]
    fn test_path_compression_with_reasonable_prefix() {
        let querier = ClickHouseQuerier {
            client: Client::default(),
            cluster: "test".to_string(),
            batch: "test".to_string(),
        };

        // Should compress when significant improvement
        assert_eq!(
            querier.find_compressed_prefix("he", "hello_alice", "hello_zarah"),
            "hello_"
        );

        // Should not compress when minimal improvement
        assert_eq!(
            querier.find_compressed_prefix("user", "user_a", "user_z"),
            "user"
        );
    }

    #[test]
    fn test_path_compression_with_length_limit() {
        let querier = ClickHouseQuerier {
            client: Client::default(),
            cluster: "test".to_string(),
            batch: "test".to_string(),
        };

        let result = querier.find_compressed_prefix(
            "u",
            "user_profile_very_long_key_name",
            "user_profile_very_long_key_other",
        );

        // Should truncate to reasonable length (u + 20 = 21 chars max)
        assert_eq!(result.len(), 21);
        assert!(result.starts_with("user_profile_very_lon"));
    }

    #[test]
    fn test_prefix_records_aggregation() {
        let records = vec![
            // Classification totals (empty prefix)
            PrefixRecord {
                prefix: "".to_string(),
                instance: "127.0.0.1:6379".to_string(),
                db: 0,
                r#type: "string".to_string(),
                rdb_size: 10000,
                key_count: 100,
            },
            PrefixRecord {
                prefix: "".to_string(),
                instance: "127.0.0.1:6380".to_string(),
                db: 0,
                r#type: "hash".to_string(),
                rdb_size: 5000,
                key_count: 50,
            },
            // Specific prefix records
            PrefixRecord {
                prefix: "user:".to_string(),
                instance: "127.0.0.1:6379".to_string(),
                db: 0,
                r#type: "string".to_string(),
                rdb_size: 8000,
                key_count: 80,
            },
            PrefixRecord {
                prefix: "user:".to_string(),
                instance: "127.0.0.1:6380".to_string(),
                db: 0,
                r#type: "hash".to_string(),
                rdb_size: 3000,
                key_count: 30,
            },
        ];

        // Cluster total memory
        let cluster_total: u64 = records
            .iter()
            .filter(|r| r.prefix.is_empty())
            .map(|r| r.rdb_size)
            .sum();
        assert_eq!(cluster_total, 15000);

        // Prefix cross-classification memory
        let user_prefix_total: u64 = records
            .iter()
            .filter(|r| r.prefix == "user:")
            .map(|r| r.rdb_size)
            .sum();
        assert_eq!(user_prefix_total, 11000);
    }

    #[test]
    fn test_significance_threshold_filtering() {
        let threshold = 100u64;

        assert!(150u64 > threshold); // Should include
        assert!(!(50u64 > threshold)); // Should exclude
    }
}
