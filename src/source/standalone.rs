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
    pub username: String,
    pub password: Option<String>,
}

impl Config {
    /// Create a new standalone config
    pub fn new(address: String, username: String, password: Option<String>) -> Self {
        Self {
            address,
            username,
            password,
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
            SourceType::Standalone,
        );
        Ok(vec![Box::pin(stream)])
    }
}
