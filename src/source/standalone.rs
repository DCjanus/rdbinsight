use std::{pin::Pin, vec};

use anyhow::{Context as AnyhowContext, ensure};
use async_trait::async_trait;

use crate::{
    helper::AnyResult,
    source::{RDBStream, RdbSourceConfig, SourceType, redis_stream::RedisRdbStream},
};

#[derive(Clone)]
pub struct Config {
    pub address: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub source_type: Option<SourceType>,
}

impl Config {
    /// Create a new standalone config with default source type
    pub fn new(address: String, username: Option<String>, password: Option<String>) -> Self {
        Self {
            address,
            username,
            password,
            source_type: Some(SourceType::Standalone),
        }
    }

    /// Create a new standalone config with specific source type (useful for Codis)
    pub fn with_source_type(
        address: String,
        username: Option<String>,
        password: Option<String>,
        source_type: SourceType,
    ) -> Self {
        Self {
            address,
            username,
            password,
            source_type: Some(source_type),
        }
    }

    /// Validate the standalone configuration
    pub fn validate(&self) -> AnyResult<()> {
        ensure!(!self.address.is_empty(), "Redis address cannot be empty");

        ensure!(
            self.address.contains(':'),
            "Invalid address format: '{}', expected format 'host:port'",
            self.address
        );

        // Basic port validation
        if let Some(port_str) = self.address.split(':').next_back() {
            port_str.parse::<u16>().with_context(|| {
                format!(
                    "Invalid port in address '{}': port must be a number between 1-65535",
                    self.address
                )
            })?;
        }

        Ok(())
    }
}

#[async_trait]
impl RdbSourceConfig for Config {
    async fn get_rdb_streams(&self) -> AnyResult<Vec<Pin<Box<dyn RDBStream>>>> {
        // Validate configuration first
        self.validate()
            .context("Invalid Redis standalone configuration")?;

        let stream = RedisRdbStream::new(
            self.address.clone(),
            self.username.clone(),
            self.password.clone(),
            self.source_type.unwrap_or(SourceType::Standalone),
        );
        Ok(vec![Box::pin(stream)])
    }
}
