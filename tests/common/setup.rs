use std::{
    env,
    future::Future,
    path::PathBuf,
    pin::Pin,
    time::{Duration, Instant},
};

use anyhow::{Result, anyhow};
use redis::{Client, aio::MultiplexedConnection as AsyncConnection};
use testcontainers::{
    ContainerAsync, GenericImage,
    core::{ImageExt, IntoContainerPort},
    runners::AsyncRunner,
};

const LOG_OUTPUT_LIMIT: usize = 4_096;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RedisVariant {
    Redis8_0,
    Redis7_0,
    Redis6_0,
    Redis2_8,
    StackLatest,
}

impl RedisVariant {
    fn image(&self) -> (String, String) {
        match self {
            RedisVariant::StackLatest => {
                ("redis/redis-stack-server".to_string(), "latest".to_string())
            }
            RedisVariant::Redis8_0 => {
                let repo = env::var("RDBINSIGHT_TEST_REDIS_IMAGE_REPO")
                    .unwrap_or_else(|_| "ghcr.io/dcjanus/rdbinsight/redis".to_string());
                (repo, "8.0.5".to_string())
            }
            RedisVariant::Redis7_0 => {
                let repo = env::var("RDBINSIGHT_TEST_REDIS_IMAGE_REPO")
                    .unwrap_or_else(|_| "ghcr.io/dcjanus/rdbinsight/redis".to_string());
                (repo, "7.0.15".to_string())
            }
            RedisVariant::Redis6_0 => {
                let repo = env::var("RDBINSIGHT_TEST_REDIS_IMAGE_REPO")
                    .unwrap_or_else(|_| "ghcr.io/dcjanus/rdbinsight/redis".to_string());
                (repo, "6.0.20".to_string())
            }
            RedisVariant::Redis2_8 => {
                let repo = env::var("RDBINSIGHT_TEST_REDIS_IMAGE_REPO")
                    .unwrap_or_else(|_| "ghcr.io/dcjanus/rdbinsight/redis".to_string());
                (repo, "2.8.24".to_string())
            }
        }
    }

    fn server_command(&self) -> &'static str {
        match self {
            RedisVariant::Redis8_0
            | RedisVariant::Redis7_0
            | RedisVariant::Redis6_0
            | RedisVariant::Redis2_8 => "redis-server",
            RedisVariant::StackLatest => "redis-stack-server",
        }
    }
}

fn truncate_to_bytes(input: &str, max_bytes: usize) -> String {
    if input.len() <= max_bytes {
        return input.to_string();
    }

    let mut acc = String::with_capacity(max_bytes + 20);
    let mut used = 0usize;
    for ch in input.chars() {
        let len = ch.len_utf8();
        if used + len > max_bytes {
            break;
        }
        acc.push(ch);
        used += len;
    }
    acc.push_str("\n...<truncated>...");
    acc
}

pub struct RedisConfig {
    variant: RedisVariant,
    diskless: bool,
    snapshot: bool,
    cluster_config_file: Option<String>,
    username: String,
    password: Option<String>,
}

impl RedisConfig {
    /// Configure Redis with username for authentication
    pub fn with_username(mut self, username: impl Into<String>) -> Self {
        self.username = username.into();
        self
    }

    /// Configure Redis with password authentication
    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Configure Redis for diskless replication
    pub fn with_diskless(mut self, diskless: bool) -> Self {
        self.diskless = diskless;
        self
    }

    /// Set Redis version
    pub fn with_version(mut self, version: RedisVariant) -> Self {
        self.variant = version;
        self
    }

    /// Configure snapshot behavior
    pub fn with_snapshot(mut self, snapshot: bool) -> Self {
        self.snapshot = snapshot;
        self
    }

    pub fn with_cluster_enabled<T>(mut self, config_file: impl Into<Option<T>>) -> Self
    where T: Into<String> {
        self.cluster_config_file = config_file.into().map(|s| s.into());
        self
    }

    /// Build and start the Redis instance
    pub async fn build(self) -> Result<RedisInstance> {
        RedisInstance::from_config(self).await
    }
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            variant: RedisVariant::Redis8_0,
            diskless: false,
            snapshot: true,
            cluster_config_file: None,
            username: String::new(),
            password: None,
        }
    }
}

pub struct RedisInstance {
    pub container: ContainerAsync<GenericImage>,
    pub connection_string: String,
    pub redis_version: String,
}

impl RedisInstance {
    /// Get the container logs (stdout and stderr)
    pub async fn get_logs(&self) -> Result<String> {
        Self::collect_logs(&self.container).await
    }

    async fn from_config(cfg: RedisConfig) -> Result<Self> {
        let (repo, tag) = cfg.variant.image();
        let mut cmd: Vec<String> = Vec::new();

        cmd.push("--appendonly".into());
        cmd.push("no".into());
        // Disable protected mode so test instances accept external connections (unsupported on Redis 2.8)
        if cfg.variant != RedisVariant::Redis2_8 {
            cmd.push("--protected-mode".into());
            cmd.push("no".into());
        }

        if cfg.diskless {
            cmd.push("--repl-diskless-sync".into());
            cmd.push("yes".into());
            cmd.push("--repl-diskless-sync-delay".into());
            cmd.push("0".into());
        } else {
            cmd.push("--repl-diskless-sync".into());
            cmd.push("no".into());
        }

        if !cfg.snapshot {
            cmd.push("--save".into());
            // Empty string disables snapshots
            cmd.push(String::new());
        }

        // Auto-configure password if provided
        if let Some(ref password) = cfg.password {
            cmd.push("--requirepass".into());
            cmd.push(password.clone());
        }

        // Configure cluster mode if enabled
        if let Some(config_file) = cfg.cluster_config_file {
            cmd.push("--cluster-enabled".into());
            cmd.push("yes".into());
            cmd.push("--cluster-config-file".into());
            cmd.push(config_file);
        }

        // Build container with appropriate entrypoint when arguments are present
        let redis_image = {
            // testcontainers does not automatically read EXPOSE from the image; add 6379 explicitly
            let base = GenericImage::new(repo.clone(), tag.clone()).with_exposed_port(6379.tcp());
            let full_cmd: Vec<String> = if cmd.is_empty() {
                vec![]
            } else {
                let mut v = Vec::with_capacity(cmd.len() + 1);
                v.push(cfg.variant.server_command().to_string());
                v.extend(cmd);
                v
            };
            base.with_cmd(&full_cmd)
        };

        let container: ContainerAsync<GenericImage> = redis_image.start().await?;

        let host = match container.get_host().await {
            Ok(host) => host,
            Err(err) => {
                return Err(
                    Self::error_with_logs(&container, "Failed to get container host", err).await,
                );
            }
        };
        let port = match container.get_host_port_ipv4(6379).await {
            Ok(port) => port,
            Err(err) => {
                return Err(Self::error_with_logs(
                    &container,
                    "Failed to get mapped port 6379/tcp",
                    err,
                )
                .await);
            }
        };
        let connection_string = format!("redis://{}:{}", host, port);

        if let Err(err) = Self::wait_for_redis_ready(&connection_string).await {
            return Err(
                Self::error_with_logs(&container, "Redis failed to become ready", err).await,
            );
        }

        // If both username and password are configured, create ACL user after startup
        if !cfg.username.is_empty() && cfg.password.is_some() {
            let password = cfg.password.as_ref().unwrap();
            if let Err(err) = Self::create_acl_user(
                &connection_string,
                &cfg.username,
                password,
                cfg.password.as_deref(),
            )
            .await
            {
                return Err(
                    Self::error_with_logs(&container, "Failed to create ACL user", err).await,
                );
            }
        }

        Ok(Self {
            container,
            connection_string,
            redis_version: format!("{repo}:{tag}").replace([':', '/'], "_"),
        })
    }

    async fn collect_logs(container: &ContainerAsync<GenericImage>) -> Result<String> {
        let container_id = container.id();
        let logs_cmd = format!("docker logs {}", container_id);

        let output = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(&logs_cmd)
            .output()
            .await?;

        let stdout = String::from_utf8(output.stdout)
            .map_err(|e| anyhow!("Invalid UTF-8 in stdout: {}", e))?;
        let stderr = String::from_utf8(output.stderr)
            .map_err(|e| anyhow!("Invalid UTF-8 in stderr: {}", e))?;

        Ok(format!(
            "=== STDOUT ===\n{}\n=== STDERR ===\n{}",
            stdout, stderr
        ))
    }

    async fn error_with_logs(
        container: &ContainerAsync<GenericImage>,
        context: &str,
        err: impl std::fmt::Display,
    ) -> anyhow::Error {
        let logs = Self::collect_logs(container)
            .await
            .unwrap_or_else(|e| format!("<failed to fetch logs: {e}>"));
        let truncated_logs = truncate_to_bytes(&logs, LOG_OUTPUT_LIMIT);
        anyhow!(
            "{context}: {err}\n--- container logs (truncated to {LOG_OUTPUT_LIMIT} bytes) ---\n{}",
            truncated_logs
        )
    }

    pub async fn generate_rdb<F>(&self, test_case_name: &str, data_seeder: F) -> Result<PathBuf>
    where F: for<'c> FnOnce(
            &'c mut AsyncConnection,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'c>> {
        let client = Client::open(self.connection_string.as_str())?;
        let mut conn = client.get_multiplexed_tokio_connection().await?;

        data_seeder(&mut conn).await?;

        redis::cmd("SAVE").query_async::<()>(&mut conn).await?;

        // Ensure dump file is flushed (modules may take longer)
        tokio::time::sleep(Duration::from_secs(1)).await;
        let dir_resp: Vec<String> = redis::cmd("CONFIG")
            .arg("GET")
            .arg("dir")
            .query_async(&mut conn)
            .await
            .unwrap_or_else(|_| vec!["dir".into(), "/data".into()]);
        let dump_dir_in_container = dir_resp.get(1).map(|s| s.as_str()).unwrap_or("/data");

        let local_dumps_dir = PathBuf::from("tests/dumps");
        tokio::fs::create_dir_all(&local_dumps_dir).await?;

        let filename = format!("{}_{}.rdb", test_case_name, self.redis_version);
        let local_rdb_path = local_dumps_dir.join(&filename);

        let container_id = self.container.id();
        let copy_cmd = format!(
            "docker cp {}:{}/dump.rdb {}",
            container_id,
            dump_dir_in_container,
            local_rdb_path
                .to_str()
                .expect("Given path is not a valid UTF-8 string")
        );

        let output = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(&copy_cmd)
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8(output.stderr)
                .unwrap_or_else(|_| "<invalid UTF-8 in stderr>".to_string());
            return Err(anyhow!(
                "Failed to copy RDB file from container: {}",
                stderr
            ));
        }

        Ok(local_rdb_path)
    }

    async fn wait_for_redis_ready(connection_string: &str) -> Result<()> {
        let timeout = Duration::from_secs(60);
        let start = Instant::now();

        loop {
            if start.elapsed() > timeout {
                return Err(anyhow!("Timeout waiting for Redis to be ready"));
            }

            if let Ok(client) = Client::open(connection_string)
                && let Ok(mut conn) = client.get_multiplexed_tokio_connection().await
            {
                match redis::cmd("PING").query_async::<()>(&mut conn).await {
                    Ok(_) => return Ok(()),
                    Err(err) => {
                        let s = err.to_string();
                        if s.contains("NOAUTH") {
                            // Redis is up but requires authentication
                            return Ok(());
                        }
                    }
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn create_acl_user(
        connection_string: &str,
        username: &str,
        password: &str,
        default_password: Option<&str>,
    ) -> Result<()> {
        let client = Client::open(connection_string)?;
        let mut conn = client.get_multiplexed_tokio_connection().await?;

        // First authenticate with the default password if it exists
        // Since we configured --requirepass, we need to auth first
        if let Some(default_password) = default_password {
            redis::cmd("AUTH")
                .arg(default_password)
                .query_async::<()>(&mut conn)
                .await?;
        }

        // Create ACL user with provided username and password
        redis::cmd("ACL")
            .arg("SETUSER")
            .arg(username)
            .arg("on")
            .arg(format!(">{}", password))
            .arg("allcommands")
            .arg("allkeys")
            .query_async::<()>(&mut conn)
            .await?;

        Ok(())
    }
}
