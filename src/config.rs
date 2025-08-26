use std::{path::PathBuf, pin::Pin};

use anyhow::{Context, ensure};
use async_trait::async_trait;
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use url::Url;

use crate::{
    helper::AnyResult,
    output::OutputEnum,
    source::{RDBStream, RdbSourceConfig},
};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DumpConfig {
    pub source: SourceConfig,
    pub output: OutputConfig,
    /// Number of parallel connections for dump operations
    ///
    /// Default: CPU cores / 2 (min 1, max 8)
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
            OutputConfig::Clickhouse(_clickhouse_config) => {}
            OutputConfig::Parquet(parquet_config) => {
                parquet_config.validate()?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use url::Url;

    use super::*;

    #[test]
    fn test_clickhouse_config_requires_database_and_port() {
        // Missing database
        assert!(
            ClickHouseConfig::new(Url::parse("http://localhost:8123").unwrap(), false, None)
                .is_err()
        );
        // Missing port
        assert!(
            ClickHouseConfig::new(
                Url::parse("http://localhost?database=db").unwrap(),
                false,
                None
            )
            .is_err()
        );
        // OK
        assert!(
            ClickHouseConfig::new(
                Url::parse("http://localhost:8123?database=db").unwrap(),
                false,
                None
            )
            .is_ok()
        );
    }

    #[test]
    fn test_clickhouse_config_validation() {
        // Construction performs validation; this should succeed
        let cfg_ok = ClickHouseConfig::new(
            Url::parse("http://localhost:8123?database=db").unwrap(),
            false,
            None,
        );
        assert!(cfg_ok.is_ok());
    }

    #[test]
    fn test_clickhouse_config_invalid_url() {
        // Invalid scheme should be rejected by ClickHouseConfig::new
        let config =
            ClickHouseConfig::new(Url::parse("tcp://localhost:8123").unwrap(), false, None);

        assert!(config.is_err());
    }

    #[test]
    fn test_clickhouse_config_invalid_database_name() {
        let config = ClickHouseConfig::new(
            Url::parse("http://localhost:8123?database=invalid-db-name!").unwrap(),
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
    #[serde(rename = "parquet")]
    Parquet(ParquetConfig),
}

impl OutputConfig {
    pub fn create_output(
        &self,
        cluster: String,
        batch_ts: OffsetDateTime,
    ) -> AnyResult<OutputEnum> {
        match self {
            OutputConfig::Clickhouse(clickhouse_config) => {
                let output = crate::output::clickhouse::ClickHouseOutput::new(
                    clickhouse_config.clone(),
                    cluster,
                    batch_ts,
                );
                Ok(OutputEnum::ClickHouse(output))
            }
            OutputConfig::Parquet(parquet_config) => {
                let output = crate::output::parquet::output::ParquetOutput::new(
                    parquet_config.dir.clone(),
                    parquet_config.compression,
                    cluster,
                    batch_ts,
                );
                Ok(OutputEnum::Parquet(output))
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ParquetConfig {
    pub dir: PathBuf,
    pub compression: ParquetCompression,
}

impl ParquetConfig {
    /// Create a new ParquetConfig
    pub fn new(dir: PathBuf, compression: ParquetCompression) -> AnyResult<Self> {
        Ok(Self { dir, compression })
    }

    /// Validate the Parquet configuration
    pub fn validate(&self) -> AnyResult<()> {
        ensure!(!self.dir.is_file(), "Parquet directory cannot be a file");

        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum ParquetCompression {
    Zstd,
    Snappy,
    None,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ClickHouseConfig {
    #[serde(default)]
    pub auto_create_tables: bool,
    pub proxy_url: Option<String>,
    pub url: Url,
}

impl ClickHouseConfig {
    /// Create a new ClickHouseConfig from URL
    pub fn new(url: Url, auto_create_tables: bool, proxy_url: Option<String>) -> AnyResult<Self> {
        use anyhow::ensure;

        ensure!(
            url.scheme() == "http" || url.scheme() == "https",
            "ClickHouse URL must start with http:// or https://, got: '{}'",
            url
        );

        ensure!(
            url.host_str().is_some(),
            "ClickHouse URL must contain a host"
        );

        ensure!(
            url.port().is_some(),
            "ClickHouse URL must include an explicit port"
        );

        let database = url
            .query_pairs()
            .find(|(k, _)| k == "database")
            .map(|(_, v)| v.into_owned())
            .unwrap_or_default();

        ensure!(
            !database.is_empty(),
            "ClickHouse URL must include '?database=' query parameter"
        );

        ensure!(
            database
                .chars()
                .all(|c| c.is_alphanumeric() || c == '_' || c == '-'),
            "ClickHouse database name '{}' contains invalid characters. Only alphanumeric, underscore, and hyphen are allowed",
            database
        );

        // Log sanitized URLs for observability
        let ch_url_sanitized = crate::helper::sanitize_url(url.as_str());
        let proxy_url_sanitized = proxy_url
            .as_deref()
            .map(crate::helper::sanitize_url)
            .unwrap_or_else(|| "<none>".to_string());
        tracing::info!(
            operation = "clickhouse_config_new",
            clickhouse_url = %ch_url_sanitized,
            proxy_url = %proxy_url_sanitized,
            "Creating ClickHouseConfig"
        );

        Ok(Self {
            auto_create_tables,
            proxy_url,
            url,
        })
    }

    // validate() removed; validation happens in new()

    /// Create a ClickHouse client based on this configuration
    pub fn create_client(&self) -> AnyResult<clickhouse::Client> {
        use std::time::Duration;

        use clickhouse::Client;
        use hyper_util::{client::legacy::Client as LegacyClient, rt::TokioExecutor};

        use crate::helper::proxy_connector::ProxyConnector;

        let proxy_connector = ProxyConnector::new(self.proxy_url.as_deref())
            .map_err(|e| anyhow::anyhow!("Failed to create proxy connector: {e}"))?;
        let http_client = LegacyClient::builder(TokioExecutor::new())
            .pool_idle_timeout(Duration::from_secs(90))
            .build(proxy_connector);

        let mut client = Client::with_http_client(http_client).with_url(self.url.to_string());

        if !self.url.username().is_empty() {
            client = client.with_user(self.url.username());
        }
        if let Some(password) = self.url.password() {
            client = client.with_password(password);
        }

        Ok(client)
    }
}
