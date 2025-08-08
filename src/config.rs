use std::{path::PathBuf, pin::Pin};

use anyhow::Context;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    helper::AnyResult,
    source::{RDBStream, RdbSourceConfig},
};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DumpConfig {
    pub source: SourceConfig,
    pub output: OutputConfig,
    pub concurrency: usize,
}

impl DumpConfig {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clickhouse_config_default_database() {
        let config =
            ClickHouseConfig::new("http://localhost:8123".to_string(), false, None).unwrap();

        assert_eq!(config.database, "rdbinsight");
    }

    #[test]
    fn test_clickhouse_config_custom_database() {
        let config =
            ClickHouseConfig::new("http://localhost:8123/mydb".to_string(), false, None).unwrap();

        assert_eq!(config.database, "mydb");
    }

    #[test]
    fn test_clickhouse_config_validation() {
        let config =
            ClickHouseConfig::new("http://localhost:8123".to_string(), false, None).unwrap();

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_clickhouse_config_invalid_url() {
        let config = ClickHouseConfig::new("invalid-url".to_string(), false, None);

        assert!(config.is_err());
    }

    #[test]
    fn test_clickhouse_config_invalid_database_name() {
        let config = ClickHouseConfig::new(
            "http://localhost:8123/invalid-db-name!".to_string(),
            false,
            None,
        );

        assert!(config.is_err());
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceConfig {
    RedisStandalone {
        cluster_name: String,
        address: String,
        username: String,
        password: Option<String>,
    },
    RedisCluster {
        cluster_name: String,
        addrs: Vec<String>,
        username: String,
        password: Option<String>,
        #[serde(default)]
        require_slave: bool,
    },
    #[serde(rename = "rdb_file")]
    RDBFile {
        cluster_name: String,
        path: String,
        instance: String,
    },
    Codis {
        cluster_name: Option<String>,
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
                let source = crate::source::standalone::Config::new(
                    address.clone(),
                    username.clone(),
                    password.clone(),
                );
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
                    let source = crate::source::standalone::Config::new(
                        addr,
                        String::new(),
                        password.clone(),
                    );
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
    #[serde(default)]
    pub auto_create_tables: bool,
    pub proxy_url: Option<String>,

    /// Address of ClickHouse server (e.g., http[s]://<host>:[port])
    pub address: String,
    /// Username for authentication (empty string means no username)
    pub username: String,
    /// Password for authentication (optional)
    pub password: Option<String>,
    /// Database name (defaults to "rdbinsight")
    #[serde(default = "default_database")]
    pub database: String,
}

impl ClickHouseConfig {
    /// Create a new ClickHouseConfig from URL
    pub fn new(
        url: String,
        auto_create_tables: bool,
        proxy_url: Option<String>,
    ) -> AnyResult<Self> {
        use anyhow::ensure;

        ensure!(!url.is_empty(), "ClickHouse URL cannot be empty");

        // Basic URL format validation
        ensure!(
            url.starts_with("http://") || url.starts_with("https://"),
            "ClickHouse URL must start with http:// or https://, got: '{}'",
            url
        );

        // Parse URL to validate format and extract components
        let parsed_url = url::Url::parse(&url)
            .map_err(|e| anyhow::anyhow!("Invalid ClickHouse URL format: {}", e))?;

        // Validate scheme
        ensure!(
            parsed_url.scheme() == "http" || parsed_url.scheme() == "https",
            "ClickHouse URL must use http or https scheme"
        );

        // Validate host
        ensure!(
            parsed_url.host_str().is_some(),
            "ClickHouse URL must contain a host"
        );

        // Extract database name from path, default to "rdbinsight"
        let database = parsed_url
            .path_segments()
            .and_then(|mut segments| segments.next())
            .filter(|db| !db.is_empty())
            .map(|db| db.to_string())
            .unwrap_or_else(|| "rdbinsight".to_string());

        // Validate database name
        ensure!(
            database
                .chars()
                .all(|c| c.is_alphanumeric() || c == '_' || c == '-'),
            "ClickHouse database name '{}' contains invalid characters. Only alphanumeric, underscore, and hyphen are allowed",
            database
        );

        // Extract username and password
        let username = parsed_url.username().to_string();

        let password = parsed_url.password().map(|p| p.to_string());

        // Build address
        let mut address = parsed_url.clone();
        address.set_username("").ok();
        address.set_password(None).ok();
        address.set_path("");
        address.set_query(None);

        Ok(Self {
            auto_create_tables,
            proxy_url,
            address: address.to_string(),
            username,
            password,
            database,
        })
    }

    /// Validate the ClickHouse configuration
    pub fn validate(&self) -> AnyResult<()> {
        use anyhow::ensure;

        ensure!(
            !self.address.is_empty(),
            "ClickHouse base URL cannot be empty"
        );

        ensure!(
            self.address.starts_with("http://") || self.address.starts_with("https://"),
            "ClickHouse base URL must start with http:// or https://"
        );

        ensure!(
            self.database
                .chars()
                .all(|c| c.is_alphanumeric() || c == '_' || c == '-'),
            "ClickHouse database name '{}' contains invalid characters. Only alphanumeric, underscore, and hyphen are allowed",
            self.database
        );

        Ok(())
    }

    /// Create a ClickHouse client based on this configuration
    pub fn create_client(&self) -> AnyResult<clickhouse::Client> {
        use std::time::Duration;

        use clickhouse::Client;
        use hyper_util::{client::legacy::Client as LegacyClient, rt::TokioExecutor};
        use tracing::info;

        use crate::helper::{proxy_connector::ProxyConnector, sanitize_url};

        if let Some(proxy_url) = self.proxy_url.as_ref() {
            let safe_url = sanitize_url(proxy_url);
            info!(operation = "create_clickhouse_client", proxy_host = %safe_url, "Creating ClickHouse client with proxy");
        } else {
            info!(
                operation = "create_clickhouse_client",
                "Creating ClickHouse client without proxy"
            );
        }

        let proxy_connector = ProxyConnector::new(self.proxy_url.as_deref())
            .map_err(|e| anyhow::anyhow!("Failed to create proxy connector: {e}"))?;
        let http_client = LegacyClient::builder(TokioExecutor::new())
            .pool_idle_timeout(Duration::from_secs(90))
            .build(proxy_connector);

        let mut client = Client::with_http_client(http_client).with_url(&self.address);

        let username = self.username.clone();
        if !username.is_empty() {
            client = client.with_user(username);
        }

        if let Some(password) = self.password.as_deref() {
            client = client.with_password(password);
        }

        client = client.with_database(&self.database);

        Ok(client)
    }
}

fn default_database() -> String {
    "rdbinsight".to_string()
}
