use std::{path::Path, time::Duration};

use anyhow::{Context, Result, bail};
use redis::Client;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{Healthcheck, IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::process::Command;

use super::{RedisLaunchOptions, RedisVersion, image};

const REDIS_PORT: u16 = 6379;

/// Represents a Redis container launched via the new infrastructure.
pub struct RedisContainer {
    options: RedisLaunchOptions,
    container: ContainerAsync<GenericImage>,
    connection_string: String,
}

impl RedisContainer {
    /// Build the target image if necessary and start a container.
    pub async fn start(options: RedisLaunchOptions) -> Result<Self> {
        let image = image::build_image(options.version, true).await?;

        let mut request = image
            .with_wait_for(WaitFor::healthcheck())
            .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
            .with_exposed_port(REDIS_PORT.tcp())
            .with_health_check(build_health_check(&options));

        let cmd = build_server_command(&options);
        if !cmd.is_empty() {
            request = request.with_cmd(cmd);
        }

        let container = request
            .start()
            .await
            .context("failed to start redis container")?;

        let connection_string = build_connection_string(&container).await?;

        let mut handle = Self {
            options,
            container,
            connection_string,
        };

        handle.ensure_acl_user().await?;

        Ok(handle)
    }

    pub fn connection_string(&self) -> &str {
        &self.connection_string
    }

    pub fn version(&self) -> RedisVersion {
        self.options.version
    }

    pub fn container(&self) -> &ContainerAsync<GenericImage> {
        &self.container
    }

    /// Collect stdout/stderr logs via the Testcontainers API without invoking `docker logs`.
    pub async fn collect_logs(&self) -> Result<String> {
        let stdout = self
            .container
            .stdout_to_vec()
            .await
            .context("failed to read container stdout")?;
        let stderr = self
            .container
            .stderr_to_vec()
            .await
            .context("failed to read container stderr")?;

        let stdout = String::from_utf8_lossy(&stdout);
        let stderr = String::from_utf8_lossy(&stderr);
        Ok(format!(
            "=== STDOUT ===\n{stdout}\n=== STDERR ===\n{stderr}"
        ))
    }

    /// Copy a file from inside the container using the Docker CLI.
    pub async fn copy_file(&self, container_path: &str, host_path: impl AsRef<Path>) -> Result<()> {
        // TODO: Replace this docker-cli based implementation with Testcontainers support once
        // upstream exposes a native file-copy helper.
        // ref: https://github.com/testcontainers/testcontainers-rs/issues/867
        let host_path = host_path.as_ref().to_path_buf();
        if let Some(parent) = host_path.parent() {
            tokio::fs::create_dir_all(parent).await.with_context(|| {
                format!("failed to create parent directory {}", parent.display())
            })?;
        }

        let container_id = self.container.id().to_string();
        let output = Command::new("docker")
            .arg("cp")
            .arg(format!("{container_id}:{container_path}"))
            .arg(&host_path)
            .kill_on_drop(true)
            .output()
            .await
            .with_context(|| {
                format!(
                    "failed to execute docker cp {}:{} {}",
                    container_id,
                    container_path,
                    host_path.display()
                )
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!("docker cp exited with {}: {}", output.status, stderr.trim());
        }

        Ok(())
    }
}

fn build_server_command(options: &RedisLaunchOptions) -> Vec<String> {
    // TODO: Verify each server flag across supported Redis versions to ensure behavior matches expectations.
    let mut cmd = Vec::new();
    cmd.push("--appendonly".into());
    cmd.push("no".into());

    if options.diskless {
        cmd.push("--repl-diskless-sync".into());
        cmd.push("yes".into());
        cmd.push("--repl-diskless-sync-delay".into());
        cmd.push("0".into());
    } else {
        cmd.push("--repl-diskless-sync".into());
        cmd.push("no".into());
    }

    if !options.snapshot {
        cmd.push("--save".into());
        cmd.push(String::new());
    }

    if let Some(ref password) = options.password {
        cmd.push("--requirepass".into());
        cmd.push(password.clone());
    }

    if let Some(ref file) = options.cluster_config_file {
        cmd.push("--cluster-enabled".into());
        cmd.push("yes".into());
        cmd.push("--cluster-config-file".into());
        cmd.push(file.clone());
    }

    cmd
}

async fn build_connection_string(container: &ContainerAsync<GenericImage>) -> Result<String> {
    let host = container
        .get_host()
        .await
        .context("failed to resolve host")?;
    let port = container
        .get_host_port_ipv4(REDIS_PORT)
        .await
        .context("failed to resolve port")?;
    Ok(format!("redis://{}:{}", host, port))
}

impl RedisContainer {
    async fn ensure_acl_user(&mut self) -> Result<()> {
        match (&self.options.username, &self.options.password) {
            (Some(username), Some(password)) => {
                let client = Client::open(self.connection_string.as_str())?;
                let mut conn = client.get_multiplexed_tokio_connection().await?;
                redis::cmd("AUTH")
                    .arg(password)
                    .query_async::<()>(&mut conn)
                    .await
                    .context("authenticating with default password")?;

                redis::cmd("ACL")
                    .arg("SETUSER")
                    .arg(username)
                    .arg("on")
                    .arg(format!(">{password}"))
                    .arg("allcommands")
                    .arg("allkeys")
                    .query_async::<()>(&mut conn)
                    .await
                    .context("creating acl user")?;

                Ok(())
            }
            _ => Ok(()),
        }
    }
}

fn build_health_check(options: &RedisLaunchOptions) -> Healthcheck {
    let mut cmd = vec![
        "redis-cli".to_string(),
        "-h".to_string(),
        "127.0.0.1".to_string(),
        "-p".to_string(),
        REDIS_PORT.to_string(),
    ];

    if let Some(password) = options.password.as_ref() {
        cmd.push("-a".to_string());
        cmd.push(password.clone());
    }

    cmd.push("ping".to_string());

    Healthcheck::cmd(cmd)
        .with_interval(Duration::from_secs(2))
        .with_timeout(Duration::from_secs(1))
        .with_retries(30)
}
