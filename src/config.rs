use std::{path::PathBuf, pin::Pin};

use anyhow::Context;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    helper::AnyResult,
    source::{RDBStream, RdbSourceConfig},
};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
    pub source: SourceConfig,
    pub output: OutputConfig,
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
}

impl Config {
    /// Get the effective concurrency based on configuration and source type
    pub async fn effective_concurrency(&self) -> usize {
        // If explicitly set to a non-default value, respect it
        if self.concurrency != default_concurrency() {
            return self.concurrency;
        }

        // For cluster sources, try to get recommendation
        match &self.source {
            SourceConfig::RedisCluster { addrs, .. } => {
                // For clusters, use number of expected shards as hint, but cap it
                addrs.len().min(num_cpus::get()).max(1)
            }
            _ => self.concurrency,
        }
    }

    /// Validate the entire configuration
    pub fn validate(&self) -> AnyResult<()> {
        use anyhow::ensure;

        // Validate concurrency
        ensure!(
            self.concurrency > 0,
            "Concurrency must be greater than 0, got: {}",
            self.concurrency
        );

        ensure!(
            self.concurrency <= 1000,
            "Concurrency too high: {}. Maximum recommended is 1000",
            self.concurrency
        );

        // Validate source configuration
        match &self.source {
            SourceConfig::RedisStandalone {
                cluster_name,
                address,
                ..
            } => {
                ensure!(!cluster_name.is_empty(), "Cluster name cannot be empty");
                ensure!(!address.is_empty(), "Redis address cannot be empty");
            }
            SourceConfig::RedisCluster {
                cluster_name,
                addrs,
                ..
            } => {
                ensure!(!cluster_name.is_empty(), "Cluster name cannot be empty");
                ensure!(!addrs.is_empty(), "Redis cluster addresses cannot be empty");
            }
            SourceConfig::RDBFile {
                cluster_name,
                path,
                instance,
                ..
            } => {
                ensure!(!cluster_name.is_empty(), "Cluster name cannot be empty");
                ensure!(!path.is_empty(), "RDB file path cannot be empty");
                ensure!(!instance.is_empty(), "Instance name cannot be empty");
            }
            SourceConfig::Codis { dashboard_addr, .. } => {
                ensure!(
                    !dashboard_addr.is_empty(),
                    "Dashboard address cannot be empty"
                );
            }
        }

        // Validate output configuration
        match &self.output {
            OutputConfig::Clickhouse(clickhouse_config) => {
                clickhouse_config.validate()?;
            }
        }

        Ok(())
    }
}

fn default_concurrency() -> usize {
    // Conservative default that works well for both single instances and small clusters
    (num_cpus::get() / 2).clamp(1, 8)
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceConfig {
    RedisStandalone {
        cluster_name: String,
        batch_id: Option<String>,
        address: String,
        username: Option<String>,
        password: Option<String>,
    },
    RedisCluster {
        cluster_name: String,
        batch_id: Option<String>,
        addrs: Vec<String>,
        username: Option<String>,
        password: Option<String>,
        #[serde(default)]
        require_slave: bool,
    },
    #[serde(rename = "rdb_file")]
    RDBFile {
        cluster_name: String,
        batch_id: Option<String>,
        path: String,
        instance: String,
    },
    Codis {
        cluster_name: Option<String>,
        batch_id: Option<String>,
        dashboard_addr: String,
        password: Option<String>,
        #[serde(default)]
        require_slave: bool,
    },
}

impl SourceConfig {
    /// Get the cluster name for this source configuration.
    /// For Codis sources, this should be called after preprocess().
    pub fn cluster_name(&self) -> &str {
        match self {
            SourceConfig::RedisStandalone { cluster_name, .. } => cluster_name,
            SourceConfig::RedisCluster { cluster_name, .. } => cluster_name,
            SourceConfig::RDBFile { cluster_name, .. } => cluster_name,
            SourceConfig::Codis { cluster_name, .. } => cluster_name
                .as_ref()
                .expect("Codis cluster_name should be populated by preprocess()"),
        }
    }
}

#[async_trait]
impl RdbSourceConfig for SourceConfig {
    async fn get_rdb_streams(&self) -> AnyResult<Vec<Pin<Box<dyn RDBStream>>>> {
        match self {
            SourceConfig::RedisStandalone {
                address,
                username,
                password,
                ..
            } => {
                let source = crate::source::standalone::Config {
                    address: address.clone(),
                    username: username.clone(),
                    password: password.clone(),
                };
                let streams = source.get_rdb_streams().await?;
                Ok(streams)
            }
            SourceConfig::RedisCluster {
                addrs,
                username,
                password,
                require_slave,
                ..
            } => {
                let source = crate::source::cluster::Config {
                    addrs: addrs.clone(),
                    username: username.clone(),
                    password: password.clone(),
                    require_slave: *require_slave,
                };
                let streams = source.get_rdb_streams().await?;
                Ok(streams)
            }
            SourceConfig::RDBFile { path, instance, .. } => {
                let source = crate::source::file::Config {
                    path: PathBuf::from(path),
                    instance: instance.clone(),
                };
                let streams = source.get_rdb_streams().await?;
                Ok(streams)
            }
            SourceConfig::Codis {
                dashboard_addr,
                password,
                require_slave,
                ..
            } => {
                let client = crate::source::codis::CodisClient::new(dashboard_addr);
                let redis_addrs = client.fetch_redis_instances(*require_slave).await?;

                let mut streams = Vec::new();
                for addr in redis_addrs {
                    let source = crate::source::standalone::Config {
                        address: addr,
                        username: None, // Codis doesn't support username
                        password: password.clone(),
                    };
                    let standalone_streams = source.get_rdb_streams().await?;
                    streams.extend(standalone_streams);
                }
                Ok(streams)
            }
        }
    }

    async fn preprocess(&mut self) -> AnyResult<()> {
        if let SourceConfig::Codis {
            cluster_name,
            dashboard_addr,
            ..
        } = self
            && cluster_name.is_none()
        {
            let client = crate::source::codis::CodisClient::new(dashboard_addr);
            let product_name = client
                .fetch_product_name()
                .await
                .context("failed to fetch codis product name")?;
            *cluster_name = Some(product_name);
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutputConfig {
    #[serde(rename = "clickhouse")]
    Clickhouse(ClickHouseConfig),
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ClickHouseConfig {
    pub address: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub database: Option<String>,
    #[serde(default)]
    pub auto_create_tables: bool,
}

impl ClickHouseConfig {
    /// Validate the ClickHouse configuration
    pub fn validate(&self) -> AnyResult<()> {
        use anyhow::ensure;

        ensure!(
            !self.address.is_empty(),
            "ClickHouse address cannot be empty"
        );

        // Basic URL format validation
        ensure!(
            self.address.starts_with("http://") || self.address.starts_with("https://"),
            "ClickHouse address must start with http:// or https://, got: '{}'",
            self.address
        );

        // Validate database name if provided
        if let Some(database) = &self.database {
            ensure!(
                !database.is_empty(),
                "ClickHouse database name cannot be empty string"
            );

            // Basic database name validation (alphanumeric, underscore, hyphen)
            ensure!(
                database
                    .chars()
                    .all(|c| c.is_alphanumeric() || c == '_' || c == '-'),
                "ClickHouse database name '{}' contains invalid characters. Only alphanumeric, underscore, and hyphen are allowed",
                database
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_standalone_config() {
        let toml_str = include_str!("../examples/standalone.toml");
        let config: Config = toml::from_str(toml_str).unwrap();

        assert_eq!(config.concurrency, 10);

        match config.source {
            SourceConfig::RedisStandalone {
                cluster_name,
                batch_id,
                address,
                username,
                password,
            } => {
                assert_eq!(cluster_name, "my-standalone-cluster");
                assert_eq!(batch_id, None);
                assert_eq!(address, "127.0.0.1:6379");
                assert_eq!(username, Some("default".to_string()));
                assert_eq!(password, Some("123456".to_string()));
            }
            _ => panic!("Incorrect source type"),
        }

        match &config.output {
            OutputConfig::Clickhouse(clickhouse_config) => {
                assert_eq!(clickhouse_config.address, "http://127.0.0.1:8124");
                assert_eq!(clickhouse_config.username, Some("rdbinsight".to_string()));
                assert_eq!(clickhouse_config.password, Some("rdbinsight".to_string()));
                assert_eq!(clickhouse_config.auto_create_tables, true);
            }
        }
    }

    #[test]
    fn test_deserialize_rdb_file_config() {
        let toml_str = include_str!("../examples/rdb_file.toml");
        let config: Config = toml::from_str(toml_str).unwrap();

        assert_eq!(config.concurrency, 1);

        match config.source {
            SourceConfig::RDBFile {
                cluster_name,
                batch_id,
                path,
                instance,
            } => {
                assert_eq!(cluster_name, "my-rdb-file");
                assert_eq!(batch_id, Some("batch-456".to_string()));
                assert_eq!(path, "/path/to/dump.rdb");
                assert_eq!(instance, "192.168.1.100:6379");
            }
            _ => panic!("Incorrect source type"),
        }

        match &config.output {
            OutputConfig::Clickhouse(clickhouse_config) => {
                assert_eq!(clickhouse_config.address, "http://127.0.0.1:8124");
                assert_eq!(clickhouse_config.username, Some("rdbinsight".to_string()));
                assert_eq!(clickhouse_config.password, Some("rdbinsight".to_string()));
                assert_eq!(clickhouse_config.auto_create_tables, true);
            }
        }
    }

    #[test]
    fn test_auto_create_tables_config() {
        let toml_str = r#"
concurrency = 1

[source]
type = "rdb_file"
cluster_name = "test-cluster"
path = "/test/path.rdb"
instance = "test:6379"

[output]
type = "clickhouse"
address = "127.0.0.1:9000"
username = "test"
password = "test"
auto_create_tables = true
        "#;

        let config: Config = toml::from_str(toml_str).unwrap();

        match &config.output {
            OutputConfig::Clickhouse(clickhouse_config) => {
                assert_eq!(clickhouse_config.auto_create_tables, true);
            }
        }
    }

    #[test]
    fn test_deserialize_cluster_config() {
        let toml_str = include_str!("../examples/cluster.toml");
        let config: Config = toml::from_str(toml_str).unwrap();

        assert_eq!(config.concurrency, 8);

        match config.source {
            SourceConfig::RedisCluster {
                cluster_name,
                batch_id,
                addrs,
                username,
                password,
                require_slave: _,
            } => {
                assert_eq!(cluster_name, "my-redis-cluster");
                assert_eq!(batch_id, None);
                assert_eq!(addrs, vec![
                    "127.0.0.1:7000".to_string(),
                    "127.0.0.1:7001".to_string(),
                    "127.0.0.1:7002".to_string()
                ]);
                assert_eq!(username, Some("default".to_string()));
                assert_eq!(password, Some("123456".to_string()));
            }
            _ => panic!("Incorrect source type"),
        }

        match &config.output {
            OutputConfig::Clickhouse(clickhouse_config) => {
                assert_eq!(clickhouse_config.address, "http://127.0.0.1:8124");
                assert_eq!(clickhouse_config.username, Some("rdbinsight".to_string()));
                assert_eq!(clickhouse_config.password, Some("rdbinsight".to_string()));
                assert_eq!(clickhouse_config.auto_create_tables, true);
            }
        }
    }

    #[test]
    fn test_deserialize_codis_config() {
        let toml_str = include_str!("../examples/codis.toml");
        let config: Config = toml::from_str(toml_str).unwrap();

        assert_eq!(config.concurrency, 8);

        match config.source {
            SourceConfig::Codis {
                cluster_name,
                batch_id,
                dashboard_addr,
                password,
                require_slave,
            } => {
                assert_eq!(cluster_name, None); // Should be None as it will be fetched from API
                assert_eq!(batch_id, None);
                assert_eq!(dashboard_addr, "http://127.0.0.1:11080");
                assert_eq!(password, Some("your_redis_password".to_string()));
                assert_eq!(require_slave, false); // Default value
            }
            _ => panic!("Incorrect source type"),
        }

        match &config.output {
            OutputConfig::Clickhouse(clickhouse_config) => {
                assert_eq!(clickhouse_config.address, "http://127.0.0.1:8124");
                assert_eq!(clickhouse_config.username, Some("rdbinsight".to_string()));
                assert_eq!(clickhouse_config.password, Some("rdbinsight".to_string()));
                assert_eq!(clickhouse_config.auto_create_tables, true);
            }
        }
    }
}
