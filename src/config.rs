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
    #[serde(rename = "rdb_file")]
    RDBFile {
        cluster_name: String,
        batch_id: Option<String>,
        path: String,
        instance: String,
    },
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
                assert_eq!(clickhouse_config.address, "127.0.0.1:9000");
                assert_eq!(clickhouse_config.username, Some("default".to_string()));
                assert_eq!(clickhouse_config.password, Some("123456".to_string()));
                assert_eq!(clickhouse_config.auto_create_tables, false); // Default value
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
                assert_eq!(clickhouse_config.address, "127.0.0.1:9000");
                assert_eq!(clickhouse_config.username, Some("default".to_string()));
                assert_eq!(clickhouse_config.password, Some("123456".to_string()));
                assert_eq!(clickhouse_config.auto_create_tables, false);
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
}
