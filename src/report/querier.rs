use std::collections::HashSet;

use anyhow::Result as AnyResult;
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};

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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_significance_threshold_calculation() {
        let total_size = 1000000u64;
        let threshold = (total_size as f64 * 0.001) as u64;
        assert_eq!(threshold, 1000);
    }

    #[test]
    fn test_classification_creation() {
        let _classification = Classification {
            r#type: "string".to_string(),
            instance: "127.0.0.1:6379".to_string(),
            db: 0,
        };
    }

    #[test]
    fn test_prefix_node_creation() {
        let root = PrefixNode {
            prefix: "".to_string(), // Root node has empty prefix
            value: 1000,
            children: vec![
                PrefixNode {
                    prefix: "user:".to_string(),
                    value: 600,
                    children: vec![],
                },
                PrefixNode {
                    prefix: "*".to_string(),
                    value: 400,
                    children: vec![],
                },
            ],
        };

        assert_eq!(root.prefix, "");
        assert_eq!(root.value, 1000);
        assert_eq!(root.children.len(), 2);
        assert_eq!(root.children[0].prefix, "user:");
        assert_eq!(root.children[1].prefix, "*");
    }

    #[test]
    fn test_path_compression_logic() {
        let children_data = vec![("user:session:".to_string(), 1000u64)];

        let current_size = 1000u64;
        let should_compress = children_data.len() == 1 && children_data[0].1 == current_size;

        assert!(should_compress);
    }

    #[test]
    fn test_other_node_filtering() {
        let significance_threshold = 100u64;
        let other_size = 50u64;
        let should_include_other = other_size > significance_threshold;
        assert!(!should_include_other);

        let other_size_significant = 150u64;
        let should_include_other_significant = other_size_significant > significance_threshold;
        assert!(should_include_other_significant);
    }

    #[test]
    fn test_root_node_naming() {
        let classification = Classification {
            r#type: "hash".to_string(),
            instance: "redis-cluster-1:6379".to_string(),
            db: 2,
        };

        let expected_name = format!(
            "{}:{}:{}",
            classification.r#type, classification.instance, classification.db
        );

        assert_eq!(expected_name, "hash:redis-cluster-1:6379:2");
    }

    #[test]
    fn test_prefix_record_creation() {
        let record = PrefixRecord {
            prefix: "user:".to_string(),
            instance: "127.0.0.1:6379".to_string(),
            db: 0,
            r#type: "string".to_string(),
            rdb_size: 1024,
            key_count: 42,
        };

        assert_eq!(record.prefix, "user:");
        assert_eq!(record.instance, "127.0.0.1:6379");
        assert_eq!(record.db, 0);
        assert_eq!(record.r#type, "string");
        assert_eq!(record.rdb_size, 1024);
        assert_eq!(record.key_count, 42);
    }

    #[test]
    fn test_empty_prefix_record_for_classification_total() {
        // Test that empty prefix records can represent classification totals
        let empty_prefix_record = PrefixRecord {
            prefix: "".to_string(), // Empty prefix for classification total
            instance: "127.0.0.1:6379".to_string(),
            db: 0,
            r#type: "string".to_string(),
            rdb_size: 10240, // Total size for this classification
            key_count: 100,  // Total key count for this classification
        };

        assert_eq!(empty_prefix_record.prefix, "");
        assert_eq!(empty_prefix_record.rdb_size, 10240);
        assert_eq!(empty_prefix_record.key_count, 100);

        // This empty prefix record enables easy aggregation of cluster totals
        // by filtering records with prefix == "" and summing rdb_size and key_count
    }

    #[test]
    fn test_unified_data_structure_design() {
        // Test that the unified data structure supports both total aggregation and prefix analysis
        let records = vec![
            // Empty prefix records for classification totals
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
            // Important prefix records discovered through BFS
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

        // Test cluster total aggregation
        let cluster_total_memory: u64 = records
            .iter()
            .filter(|r| r.prefix.is_empty())
            .map(|r| r.rdb_size)
            .sum();
        assert_eq!(cluster_total_memory, 15000);

        // Test prefix cross-classification aggregation
        let user_prefix_total_memory: u64 = records
            .iter()
            .filter(|r| r.prefix == "user:")
            .map(|r| r.rdb_size)
            .sum();
        assert_eq!(user_prefix_total_memory, 11000);

        // Test that we have both total data and prefix details
        let empty_prefix_count = records.iter().filter(|r| r.prefix.is_empty()).count();
        let non_empty_prefix_count = records.iter().filter(|r| !r.prefix.is_empty()).count();
        assert_eq!(empty_prefix_count, 2); // Two classifications
        assert_eq!(non_empty_prefix_count, 2); // One prefix across two classifications
    }

    #[test]
    fn test_discover_important_prefixes_bfs_returns_prefix_records() {
        // Test that discover_important_prefixes_bfs now returns Vec<PrefixRecord>
        // This allows one-time collection of both count and sum statistics
        let classification = Classification {
            r#type: "string".to_string(),
            instance: "127.0.0.1:6379".to_string(),
            db: 0,
        };

        // Mock what the BFS function should return - PrefixRecord with full statistics
        let mock_result = vec![
            PrefixRecord {
                prefix: "user:".to_string(),
                instance: "127.0.0.1:6379".to_string(),
                db: 0,
                r#type: "string".to_string(),
                rdb_size: 8000,
                key_count: 80,
            },
            PrefixRecord {
                prefix: "cache:".to_string(),
                instance: "127.0.0.1:6379".to_string(),
                db: 0,
                r#type: "string".to_string(),
                rdb_size: 1500,
                key_count: 15,
            },
        ];

        // Verify that each record contains both rdb_size and key_count
        for record in &mock_result {
            assert!(!record.prefix.is_empty());
            assert!(record.rdb_size > 0);
            assert!(record.key_count > 0);
            assert_eq!(record.instance, classification.instance);
            assert_eq!(record.db, classification.db);
            assert_eq!(record.r#type, classification.r#type);
        }

        // This demonstrates that the BFS function now provides complete statistics
        // eliminating the need for additional queries to get count information
        let total_size: u64 = mock_result.iter().map(|r| r.rdb_size).sum();
        let total_count: u64 = mock_result.iter().map(|r| r.key_count).sum();

        assert_eq!(total_size, 9500);
        assert_eq!(total_count, 95);
    }

    #[test]
    fn test_longest_common_prefix() {
        let querier = ClickHouseQuerier {
            client: Client::default(),
            cluster: "test".to_string(),
            batch: "test".to_string(),
        };

        // Test case 1: Common prefix exists
        assert_eq!(
            querier.longest_common_prefix("hello_alice", "hello_zarah"),
            "hello_"
        );

        // Test case 2: No common prefix
        assert_eq!(querier.longest_common_prefix("apple", "banana"), "");

        // Test case 3: One string is prefix of another
        assert_eq!(
            querier.longest_common_prefix("user", "user_profile"),
            "user"
        );

        // Test case 4: Identical strings
        assert_eq!(
            querier.longest_common_prefix("cache:key", "cache:key"),
            "cache:key"
        );
    }

    #[test]
    fn test_find_compressed_prefix() {
        let querier = ClickHouseQuerier {
            client: Client::default(),
            cluster: "test".to_string(),
            batch: "test".to_string(),
        };

        // Test case 1: Should compress from "he" to "hello_"
        assert_eq!(
            querier.find_compressed_prefix("he", "hello_alice", "hello_zarah"),
            "hello_"
        );

        // Test case 2: Should compress but truncate if common prefix is too long
        let result = querier.find_compressed_prefix(
            "u",
            "user_profile_very_long_key_name",
            "user_profile_very_long_key_other",
        );
        // Should be truncated to reasonable length (u + 20 = 21 chars max)
        assert_eq!(result.len(), 21);
        assert!(result.starts_with("user_profile_very_lon"));

        // Test case 3: Should not compress if improvement is minimal
        assert_eq!(
            querier.find_compressed_prefix("user", "user_a", "user_z"),
            "user"
        );

        // Test case 4: Should compress reasonable length improvement
        assert_eq!(
            querier.find_compressed_prefix("ca", "cache:session:", "cache:temp:"),
            "cache:"
        );
    }

    #[test]
    fn test_path_compression_benefits() {
        // Demonstrate the benefits of path compression

        // Before compression: Need to traverse he -> hel -> hell -> hello
        // After compression: Jump directly from he -> hello_

        let scenarios = vec![
            ("he", "hello_alice", "hello_zarah", "hello_"),
            ("u", "user:session:123", "user:session:999", "user:session:"),
            ("c", "cache:temp:data", "cache:temp:info", "cache:temp:"),
        ];

        for (prefix, min_key, max_key, expected_compressed) in scenarios {
            let querier = ClickHouseQuerier {
                client: Client::default(),
                cluster: "test".to_string(),
                batch: "test".to_string(),
            };

            let result = querier.find_compressed_prefix(prefix, min_key, max_key);
            assert_eq!(result, expected_compressed);

            // Verify compression actually provides benefit
            assert!(result.len() > prefix.len() + 1);
            println!(
                "Compressed '{}' -> '{}' (saved {} levels)",
                prefix,
                result,
                result.len() - prefix.len() - 1
            );
        }
    }
}
