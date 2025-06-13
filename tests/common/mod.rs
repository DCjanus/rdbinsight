use std::{future::Future, path::PathBuf, pin::Pin, str::FromStr};

use anyhow::{Result, anyhow};
use redis::{AsyncCommands, Client, aio::MultiplexedConnection as AsyncConnection};
use testcontainers::{ContainerAsync, GenericImage, core::WaitFor, runners::AsyncRunner};
use tracing_subscriber::{EnvFilter, util::SubscriberInitExt};

/// Creates a logging guard for tests with info/debug level filtering.
///
/// **Important**: Assign to a variable (e.g., `let _guard = ...`) to keep the guard alive.
/// Do NOT use `let _ = ...` as it will immediately drop the guard.
///
/// # Example
/// ```rust
/// #[tokio::test]
/// async fn my_test() -> anyhow::Result<()> {
///     let _guard = common::new_log_guard(); // ✓ Correct
///     // let _ = common::new_log_guard();    // ✗ Wrong - immediately drops
///     // test code here
///     Ok(())
/// }
/// ```
#[must_use]
#[allow(dead_code)]
pub fn new_log_guard() -> impl Drop {
    let level_filter = EnvFilter::from_str("info,rdbinsight=debug").expect("invalid level filter");
    tracing_subscriber::fmt()
        .with_env_filter(level_filter)
        .with_test_writer()
        .set_default()
}

/// Redis testing utilities
pub struct RedisInstance {
    pub container: ContainerAsync<GenericImage>,
    pub connection_string: String,
    pub redis_version: String,
}

impl RedisInstance {
    pub async fn new(redis_version: &str) -> Result<Self> {
        let wait_for = if redis_version.starts_with("2.")
            || redis_version.starts_with("3.")
            || redis_version.starts_with("4.")
        {
            WaitFor::message_on_stdout("ready to accept connections")
        } else {
            WaitFor::message_on_stdout("Ready to accept connections")
        };

        let redis_image = GenericImage::new("redis", redis_version).with_wait_for(wait_for);

        let container: ContainerAsync<GenericImage> = redis_image.start().await?;
        let host = container.get_host().await?;
        let port = container.get_host_port_ipv4(6379).await?;
        let connection_string = format!("redis://{}:{}", host, port);

        Ok(Self {
            container,
            connection_string,
            redis_version: redis_version.to_string(),
        })
    }

    /// Generate RDB file using provided data seeder function
    pub async fn generate_rdb<F>(&self, test_case_name: &str, data_seeder: F) -> Result<PathBuf>
    where F: for<'c> FnOnce(
            &'c mut AsyncConnection,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'c>> {
        let client = Client::open(self.connection_string.as_str())?;
        // Obtain an async multiplexed connection (tokio runtime).
        let mut conn = client.get_multiplexed_tokio_connection().await?;

        // Seed data using the provided async closure.
        data_seeder(&mut conn).await?;

        // Ensure data is persisted to RDB.
        redis::cmd("SAVE").query_async::<()>(&mut conn).await?;

        // Give Redis some time to finish writing the RDB file on disk.
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let local_dumps_dir = PathBuf::from("tests/dumps");
        tokio::fs::create_dir_all(&local_dumps_dir).await?;

        let filename = format!("{}_{}.rdb", test_case_name, self.redis_version);
        let local_rdb_path = local_dumps_dir.join(&filename);

        let container_id = self.container.id();
        let copy_cmd = format!(
            "docker cp {}:/data/dump.rdb {}",
            container_id,
            local_rdb_path.to_string_lossy()
        );

        let output = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(&copy_cmd)
            .output()
            .await?;

        if !output.status.success() {
            return Err(anyhow!(
                "Failed to copy RDB file from container: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        Ok(local_rdb_path)
    }
}

pub async fn seed_list(conn: &mut AsyncConnection, key: &str, count: usize) -> Result<()> {
    for idx in 0..count {
        conn.rpush::<_, _, ()>(key, idx.to_string()).await?;
    }
    Ok(())
}

pub mod parser_utils;
