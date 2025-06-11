use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
    pub source: SourceConfig,
    pub output: OutputConfig,
    pub concurrency: u32,
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
    RedisSentinel {
        cluster_name: String,
        batch_id: Option<String>,
        master_name: String,
        sentinels: Vec<String>,
        username: Option<String>,
        password: Option<String>,
    },
    RedisCluster {
        cluster_name: String,
        batch_id: Option<String>,
        instances: Vec<String>,
        username: Option<String>,
        password: Option<String>,
    },
    Codis {
        cluster_name: Option<String>,
        batch_id: Option<String>,
        dashboard_address: String,
        password: Option<String>,
    },
    #[serde(rename = "rdb_file")]
    RDBFile {
        cluster_name: String,
        batch_id: Option<String>,
        path: String,
    },
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutputConfig {
    Clickhouse {
        address: String,
        username: Option<String>,
        password: Option<String>,
    },
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

        match config.output {
            OutputConfig::Clickhouse {
                address,
                username,
                password,
            } => {
                assert_eq!(address, "127.0.0.1:9000");
                assert_eq!(username, Some("default".to_string()));
                assert_eq!(password, Some("123456".to_string()));
            }
        }
    }

    #[test]
    fn test_deserialize_sentinel_config() {
        let toml_str = include_str!("../examples/sentinel.toml");
        let config: Config = toml::from_str(toml_str).unwrap();

        assert_eq!(config.concurrency, 10);

        match config.source {
            SourceConfig::RedisSentinel {
                cluster_name,
                batch_id,
                master_name,
                sentinels,
                username,
                password,
            } => {
                assert_eq!(cluster_name, "my-sentinel-cluster");
                assert_eq!(batch_id, None);
                assert_eq!(master_name, "mymaster");
                assert_eq!(sentinels, vec![
                    "127.0.0.1:26379",
                    "127.0.0.1:26380",
                    "127.0.0.1:26381"
                ]);
                assert_eq!(username, Some("default".to_string()));
                assert_eq!(password, Some("123456".to_string()));
            }
            _ => panic!("Incorrect source type"),
        }
    }

    #[test]
    fn test_deserialize_cluster_config() {
        let toml_str = include_str!("../examples/cluster.toml");
        let config: Config = toml::from_str(toml_str).unwrap();

        assert_eq!(config.concurrency, 10);

        match config.source {
            SourceConfig::RedisCluster {
                cluster_name,
                batch_id,
                instances,
                username,
                password,
            } => {
                assert_eq!(cluster_name, "my-cluster");
                assert_eq!(batch_id, Some("batch-123".to_string()));
                assert_eq!(instances, vec![
                    "127.0.0.1:6379",
                    "127.0.0.1:6380",
                    "127.0.0.1:6381"
                ]);
                assert_eq!(username, Some("default".to_string()));
                assert_eq!(password, Some("123456".to_string()));
            }
            _ => panic!("Incorrect source type"),
        }
    }

    #[test]
    fn test_deserialize_codis_config() {
        let toml_str = include_str!("../examples/codis.toml");
        let config: Config = toml::from_str(toml_str).unwrap();

        assert_eq!(config.concurrency, 10);

        match config.source {
            SourceConfig::Codis {
                cluster_name,
                batch_id,
                dashboard_address,
                password,
            } => {
                assert_eq!(cluster_name, None);
                assert_eq!(batch_id, None);
                assert_eq!(dashboard_address, "127.0.0.1:18080");
                assert_eq!(password, Some("123456".to_string()));
            }
            _ => panic!("Incorrect source type"),
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
            } => {
                assert_eq!(cluster_name, "my-rdb-file");
                assert_eq!(batch_id, Some("batch-456".to_string()));
                assert_eq!(path, "/path/to/dump.rdb");
            }
            _ => panic!("Incorrect source type"),
        }

        match config.output {
            OutputConfig::Clickhouse {
                address,
                username,
                password,
            } => {
                assert_eq!(address, "127.0.0.1:9000");
                assert_eq!(username, Some("default".to_string()));
                assert_eq!(password, Some("123456".to_string()));
            }
        }
    }
}
