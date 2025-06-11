use std::path::PathBuf;

use anyhow::{Result, anyhow};
use redis::{Client, Connection};
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::redis::Redis;

/// Redis testing utilities
pub struct RedisInstance {
    pub container: ContainerAsync<Redis>,
    pub connection_string: String,
    pub redis_version: String,
}

impl RedisInstance {
    /// Create a new Redis instance with specified version
    pub async fn new(redis_version: &str) -> Result<Self> {
        let redis_image = Redis::default().with_tag(redis_version);
        let container = redis_image.start().await?;
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
    where F: Fn(&mut Connection) -> Result<()> {
        let client = Client::open(self.connection_string.as_str())?;
        let mut conn = client.get_connection()?;

        data_seeder(&mut conn)?;
        redis::cmd("SAVE").exec(&mut conn)?;
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
