use std::{
    env,
    time::{Duration, Instant},
};

use anyhow::{Context as AnyhowContext, anyhow};
use redis::{Client, aio::MultiplexedConnection};
use semver::Version;
use testcontainers::{
    ContainerAsync, GenericImage,
    core::{ImageExt, IntoContainerPort},
    runners::AsyncRunner,
};
use tokio::time::sleep;

use super::helpers;
use crate::{
    helper::AnyResult,
    parser::Item,
    source::{RDBStream, SourceType, redis_stream::RedisRdbStream},
};

const DEFAULT_REPO: &str = "ghcr.io/dcjanus/rdbinsight/redis";

fn default_image_repo() -> String {
    env::var("RDBINSIGHT_TEST_REDIS_IMAGE_REPO").unwrap_or_else(|_| DEFAULT_REPO.to_string())
}

fn supports_protected_mode(version: &Version) -> bool {
    (version.major > 3) || (version.major == 3 && version.minor >= 2)
}

fn supports_diskless_replication(version: &Version) -> bool {
    (version.major > 2) || (version.major == 2 && version.minor >= 8)
}

fn supports_inline_config_args(version: &Version) -> bool {
    version.major >= 2
}

fn is_legacy_info_args_error(err: &redis::RedisError) -> bool {
    err.kind() == redis::ErrorKind::ResponseError
        && err
            .to_string()
            .to_lowercase()
            .contains("wrong number of arguments")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RedisPreset {
    Redis8_4_0,
    Redis7_4_7,
    Redis6_2_21,
    Redis5_0_14,
    Redis4_0_14,
    Redis3_2_13,
    Redis2_8_24,
    Redis2_4_18,
    Redis1_2_6,
}

struct PresetMeta {
    tag: &'static str,
    version: Version,
}

impl RedisPreset {
    fn meta(&self) -> PresetMeta {
        match self {
            RedisPreset::Redis8_4_0 => PresetMeta {
                tag: "8.4.0",
                version: Version::new(8, 4, 0),
            },
            RedisPreset::Redis7_4_7 => PresetMeta {
                tag: "7.4.7",
                version: Version::new(7, 4, 7),
            },
            RedisPreset::Redis6_2_21 => PresetMeta {
                tag: "6.2.21",
                version: Version::new(6, 2, 21),
            },
            RedisPreset::Redis5_0_14 => PresetMeta {
                tag: "5.0.14",
                version: Version::new(5, 0, 14),
            },
            RedisPreset::Redis4_0_14 => PresetMeta {
                tag: "4.0.14",
                version: Version::new(4, 0, 14),
            },
            RedisPreset::Redis3_2_13 => PresetMeta {
                tag: "3.2.13",
                version: Version::new(3, 2, 13),
            },
            RedisPreset::Redis2_8_24 => PresetMeta {
                tag: "2.8.24",
                version: Version::new(2, 8, 24),
            },
            RedisPreset::Redis2_4_18 => PresetMeta {
                tag: "2.4.18",
                version: Version::new(2, 4, 18),
            },
            RedisPreset::Redis1_2_6 => PresetMeta {
                tag: "1.2.6",
                version: Version::new(1, 2, 6),
            },
        }
    }
}

#[derive(Clone, Debug)]
pub struct RedisConfig {
    image: String,
    tag: String,
    version: Version,
    diskless: bool,
    snapshot: bool,
    username: String,
    password: Option<String>,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self::from_preset(RedisPreset::Redis8_4_0)
    }
}

impl RedisConfig {
    pub fn from_preset(preset: RedisPreset) -> Self {
        let meta = preset.meta();
        Self {
            image: default_image_repo(),
            tag: meta.tag.to_string(),
            version: meta.version,
            diskless: true,
            snapshot: true,
            username: String::new(),
            password: None,
        }
    }

    pub async fn build(self) -> AnyResult<RedisTestEnv> {
        RedisTestEnv::start(self).await
    }
}

pub struct RedisTestEnv {
    _container: ContainerAsync<GenericImage>,
    address: String,
    username: String,
    password: Option<String>,
    redis_version: Version,
}

impl RedisTestEnv {
    async fn start(cfg: RedisConfig) -> AnyResult<Self> {
        if cfg.username.is_empty() && cfg.password.is_some() {
            // Allow default user auth via password only.
        } else if !cfg.username.is_empty() && cfg.password.is_none() {
            anyhow::bail!("username requires password to be set");
        }

        let mut args: Vec<String> = Vec::new();
        if supports_inline_config_args(&cfg.version) {
            args.push("--appendonly".into());
            args.push("no".into());

            if supports_protected_mode(&cfg.version) {
                args.push("--protected-mode".into());
                args.push("no".into());
            }

            if supports_diskless_replication(&cfg.version) {
                if cfg.diskless {
                    args.push("--repl-diskless-sync".into());
                    args.push("yes".into());
                    args.push("--repl-diskless-sync-delay".into());
                    args.push("0".into());
                } else {
                    args.push("--repl-diskless-sync".into());
                    args.push("no".into());
                }
            }

            if !cfg.snapshot {
                args.push("--save".into());
                args.push(String::new());
            }

            if let Some(password) = &cfg.password {
                args.push("--requirepass".into());
                args.push(password.clone());
            }
        }

        let redis_image = {
            let base_image =
                GenericImage::new(cfg.image.clone(), cfg.tag.clone()).with_exposed_port(6379.tcp());
            let full_cmd: Vec<String> = if args.is_empty() {
                vec![]
            } else {
                let mut v = Vec::with_capacity(args.len() + 1);
                v.push("redis-server".to_string());
                v.extend(args);
                v
            };
            base_image.with_cmd(&full_cmd)
        };

        let container = redis_image.start().await.context("start redis container")?;
        let host = container.get_host().await.context("get container host")?;
        let port = container
            .get_host_port_ipv4(6379)
            .await
            .context("get mapped redis port")?;
        let address = format!("{host}:{port}");

        wait_for_ready(&address, "", cfg.password.as_deref()).await?;

        if !cfg.username.is_empty() {
            create_acl_user(
                &address,
                &cfg.username,
                cfg.password
                    .as_deref()
                    .expect("password must exist when username is set"),
            )
            .await?;
        }

        let redis_version =
            fetch_semver(&address, cfg.username.as_str(), cfg.password.as_deref()).await?;

        Ok(Self {
            _container: container,
            address,
            username: cfg.username,
            password: cfg.password,
            redis_version,
        })
    }

    fn connection_url(&self) -> String {
        connection_url(&self.address, &self.username, self.password.as_deref())
    }

    pub fn version(&self) -> &Version {
        &self.redis_version
    }

    pub async fn connection(&self) -> AnyResult<MultiplexedConnection> {
        let client = Client::open(self.connection_url().as_str())
            .with_context(|| "create redis client for test env")?;
        Ok(client
            .get_multiplexed_tokio_connection()
            .await
            .context("connect to redis test env")?)
    }

    pub async fn collect_items(&self) -> AnyResult<Vec<Item>> {
        let mut stream = RedisRdbStream::new(
            self.address.clone(),
            self.username.clone(),
            self.password.clone(),
            SourceType::Standalone,
        );
        stream.prepare().await?;
        let items = helpers::collect_items(stream).await?;
        Ok(items)
    }
}

fn connection_url(address: &str, username: &str, password: Option<&str>) -> String {
    match (username.is_empty(), password) {
        (true, None) => format!("redis://{address}"),
        (true, Some(password)) => format!("redis://:{password}@{address}"),
        (false, Some(password)) => format!("redis://{username}:{password}@{address}"),
        (false, None) => format!("redis://{username}@{address}"),
    }
}

async fn wait_for_ready(address: &str, username: &str, password: Option<&str>) -> AnyResult<()> {
    let timeout = Duration::from_secs(60);
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            anyhow::bail!("timeout waiting for redis container at {address}");
        }

        let url = connection_url(address, username, password);
        if let Ok(client) = Client::open(url.as_str())
            && let Ok(mut conn) = client.get_multiplexed_tokio_connection().await
        {
            match redis::cmd("PING").query_async::<()>(&mut conn).await {
                Ok(_) => return Ok(()),
                Err(err) => {
                    if err.to_string().contains("NOAUTH") {
                        return Ok(());
                    }
                }
            }
        }

        sleep(Duration::from_millis(100)).await;
    }
}

async fn fetch_semver(address: &str, username: &str, password: Option<&str>) -> AnyResult<Version> {
    let url = connection_url(address, username, password);
    let client = Client::open(url.as_str()).context("create redis client for version probe")?;
    let mut conn = client
        .get_multiplexed_tokio_connection()
        .await
        .context("connect to redis for version probe")?;
    let info: String = match redis::cmd("INFO")
        .arg("SERVER")
        .query_async(&mut conn)
        .await
    {
        Ok(info) => info,
        Err(err) => {
            if is_legacy_info_args_error(&err) {
                redis::cmd("INFO")
                    .query_async(&mut conn)
                    .await
                    .context("fetch legacy INFO")?
            } else {
                return Err(err).context("fetch INFO SERVER");
            }
        }
    };
    let version_line = info
        .lines()
        .find(|line| line.starts_with("redis_version:"))
        .ok_or_else(|| anyhow!("redis_version not found in INFO output"))?;
    let raw = version_line.trim_start_matches("redis_version:").trim();
    Version::parse(raw).context("parse redis version")
}

async fn create_acl_user(address: &str, username: &str, password: &str) -> AnyResult<()> {
    let url = connection_url(address, "", None);
    let client = Client::open(url.as_str()).context("create redis client for acl user")?;
    let mut conn = client
        .get_multiplexed_tokio_connection()
        .await
        .context("connect to redis for acl user")?;
    redis::cmd("AUTH")
        .arg(password)
        .query_async::<()>(&mut conn)
        .await
        .context("auth as default user for acl setup")?;
    redis::cmd("ACL")
        .arg("SETUSER")
        .arg(username)
        .arg("on")
        .arg(format!(">{}", password))
        .arg("allcommands")
        .arg("allkeys")
        .query_async::<()>(&mut conn)
        .await
        .context("create acl user")?;
    Ok(())
}
