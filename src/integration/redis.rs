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

use super::{artifacts::ParsedRdbArtifacts, fixtures::TestFixture, helpers};
use crate::{
    helper::AnyResult,
    source::{RDBStream, SourceType, redis_stream::RedisRdbStream},
};

const DEFAULT_REPO: &str = "ghcr.io/dcjanus/rdbinsight/redis";

fn default_image_repo() -> String {
    env::var("RDBINSIGHT_TEST_REDIS_IMAGE_REPO").unwrap_or_else(|_| DEFAULT_REPO.to_string())
}

fn is_redis_2_8(version: &Version) -> bool {
    version.major == 2 && version.minor == 8
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RedisPreset {
    Redis8_0_5,
    Redis7_0_15,
    Redis6_0_20,
    Redis2_8_24,
}

struct PresetMeta {
    tag: &'static str,
    version: Version,
}

impl RedisPreset {
    fn meta(&self) -> PresetMeta {
        match self {
            RedisPreset::Redis8_0_5 => PresetMeta {
                tag: "8.0.5",
                version: Version::new(8, 0, 5),
            },
            RedisPreset::Redis7_0_15 => PresetMeta {
                tag: "7.0.15",
                version: Version::new(7, 0, 15),
            },
            RedisPreset::Redis6_0_20 => PresetMeta {
                tag: "6.0.20",
                version: Version::new(6, 0, 20),
            },
            RedisPreset::Redis2_8_24 => PresetMeta {
                tag: "2.8.24",
                version: Version::new(2, 8, 24),
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
        Self::from_preset(RedisPreset::Redis8_0_5)
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

    pub fn with_image(mut self, image: impl Into<String>) -> Self {
        self.image = image.into();
        self
    }

    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tag = tag.into();
        self
    }

    pub fn with_version(mut self, version: Version) -> Self {
        self.version = version;
        self
    }

    pub fn with_version_and_tag(self, version: Version, tag: impl Into<String>) -> Self {
        self.with_version(version).with_tag(tag)
    }

    pub fn with_preset(mut self, preset: RedisPreset) -> Self {
        let meta = preset.meta();
        self.image = default_image_repo();
        self.tag = meta.tag.to_string();
        self.version = meta.version;
        self
    }

    pub fn with_diskless(mut self, diskless: bool) -> Self {
        self.diskless = diskless;
        self
    }

    pub fn without_snapshot(mut self) -> Self {
        self.snapshot = false;
        self
    }

    pub fn with_username(mut self, username: impl Into<String>) -> Self {
        self.username = username.into();
        self
    }

    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
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

        args.push("--appendonly".into());
        args.push("no".into());

        if !is_redis_2_8(&cfg.version) {
            args.push("--protected-mode".into());
            args.push("no".into());
        }

        if cfg.diskless {
            args.push("--repl-diskless-sync".into());
            args.push("yes".into());
            args.push("--repl-diskless-sync-delay".into());
            args.push("0".into());
        } else {
            args.push("--repl-diskless-sync".into());
            args.push("no".into());
        }

        if !cfg.snapshot {
            args.push("--save".into());
            args.push(String::new());
        }

        if let Some(password) = &cfg.password {
            args.push("--requirepass".into());
            args.push(password.clone());
        }

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
        let redis_image = base_image.with_cmd(&full_cmd);

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

    pub async fn collect_artifacts(&self) -> AnyResult<ParsedRdbArtifacts> {
        let mut stream = RedisRdbStream::new(
            self.address.clone(),
            self.username.clone(),
            self.password.clone(),
            SourceType::Standalone,
        );
        stream.prepare().await?;
        let items = helpers::collect_items(stream).await?;
        Ok(ParsedRdbArtifacts::new(self.redis_version.clone(), items))
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
    let info: String = redis::cmd("INFO")
        .arg("SERVER")
        .query_async(&mut conn)
        .await
        .context("fetch INFO SERVER")?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redis_config_builder_methods_chain() {
        let version = Version::new(7, 0, 15);
        let cfg = RedisConfig::from_preset(RedisPreset::Redis7_0_15)
            .with_image("example.com/custom/redis")
            .with_version_and_tag(version.clone(), "7.0.15")
            .with_diskless(false)
            .without_snapshot()
            .with_username("tester")
            .with_password("secret");
        assert_eq!(cfg.image, "example.com/custom/redis");
        assert_eq!(cfg.tag, "7.0.15");
        assert_eq!(cfg.version, version);
        assert!(!cfg.diskless);
        assert!(!cfg.snapshot);
        assert_eq!(cfg.username, "tester");
        assert_eq!(cfg.password.as_deref(), Some("secret"));
    }

    #[test]
    fn redis_2_8_detection_matches_semver() {
        assert!(is_redis_2_8(&Version::new(2, 8, 24)));
        assert!(!is_redis_2_8(&Version::new(7, 0, 15)));
    }

    #[test]
    fn redis_presets_align_with_publish_workflow() {
        let expected_repo = default_image_repo();
        let cases = [
            (RedisPreset::Redis8_0_5, "8.0.5", Version::new(8, 0, 5)),
            (RedisPreset::Redis7_0_15, "7.0.15", Version::new(7, 0, 15)),
            (RedisPreset::Redis6_0_20, "6.0.20", Version::new(6, 0, 20)),
            (RedisPreset::Redis2_8_24, "2.8.24", Version::new(2, 8, 24)),
        ];
        for (preset, tag, version) in cases {
            let from_preset = RedisConfig::from_preset(preset);
            assert_eq!(from_preset.tag, tag);
            assert_eq!(from_preset.version, version);
            assert_eq!(from_preset.image, expected_repo);

            let updated = RedisConfig::default().with_preset(preset);
            assert_eq!(updated.tag, tag);
            assert_eq!(updated.version, version);
        }
    }
}
