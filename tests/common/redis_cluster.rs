use std::{
    net::TcpListener,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use redis::Client;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};

const NODE_COUNT: u16 = 6;
const PORT_RANGE_START: u16 = 20_000;
const PORT_RANGE_END: u16 = 40_000;

pub struct RedisClusterInstance {
    pub container: ContainerAsync<GenericImage>,
    pub ports: Vec<u16>,
}

impl RedisClusterInstance {
    pub async fn start() -> Result<Self> {
        let ports = reserve_contiguous_ports()?;
        let startup_script = startup_script(&ports);
        let image_repo = std::env::var("RDBINSIGHT_TEST_REDIS_IMAGE_REPO")
            .unwrap_or_else(|_| "ghcr.io/dcjanus/rdbinsight/redis".to_string());

        let mut image = GenericImage::new(image_repo, "8.0.5".to_string())
            .with_wait_for(WaitFor::message_on_stdout("rdbinsight cluster ready"))
            .with_cmd(["sh", "-c", startup_script.as_str()]);
        for port in &ports {
            image = image.with_mapped_port(*port, port.tcp());
        }

        let container = image
            .start()
            .await
            .context("start Redis Cluster container")?;
        let cluster = Self { container, ports };
        cluster
            .wait_until_ready()
            .await
            .context("wait for Redis Cluster readiness")?;
        Ok(cluster)
    }

    pub fn addresses(&self) -> Vec<String> {
        self.ports
            .iter()
            .map(|port| format!("127.0.0.1:{port}"))
            .collect()
    }

    pub fn initial_node_url(&self) -> String {
        format!("redis://127.0.0.1:{}", self.ports[0])
    }

    async fn wait_until_ready(&self) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let result = async {
                let client = Client::open(self.initial_node_url())?;
                let mut conn = client.get_multiplexed_async_connection().await?;
                let info: String = redis::cmd("CLUSTER")
                    .arg("INFO")
                    .query_async(&mut conn)
                    .await?;
                Result::<_, anyhow::Error>::Ok(info)
            }
            .await;

            if let Ok(info) = result
                && info.lines().any(|line| line == "cluster_state:ok")
                && info
                    .lines()
                    .any(|line| line == "cluster_slots_assigned:16384")
            {
                return Ok(());
            }
            ensure!(
                Instant::now() < deadline,
                "Redis Cluster did not become ready within 10 seconds"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

fn reserve_contiguous_ports() -> Result<Vec<u16>> {
    for _ in 0..100 {
        let base = PORT_RANGE_START + rand::random::<u16>() % (PORT_RANGE_END - PORT_RANGE_START);
        let mut listeners = Vec::with_capacity(NODE_COUNT as usize);
        let mut ports = Vec::with_capacity(NODE_COUNT as usize);
        for offset in 0..NODE_COUNT {
            let port = base + offset;
            match TcpListener::bind(("127.0.0.1", port)) {
                Ok(listener) => {
                    listeners.push(listener);
                    ports.push(port);
                }
                Err(_) => break,
            }
        }
        if listeners.len() == NODE_COUNT as usize {
            return Ok(ports);
        }
    }
    anyhow::bail!("failed to reserve six contiguous ports for Redis Cluster")
}

fn startup_script(ports: &[u16]) -> String {
    let mut script = String::from("set -eu\n");
    for port in ports {
        script.push_str(&format!(
            "mkdir -p /data/{port}\n\
             redis-server --port {port} --bind 0.0.0.0 --protected-mode no \\\n               --cluster-enabled yes --cluster-config-file /data/{port}/nodes.conf \\\n               --cluster-node-timeout 1000 --cluster-announce-ip 127.0.0.1 \\\n               --cluster-announce-port {port} --cluster-announce-bus-port {} \\\n               --appendonly no --save '' --daemonize yes --dir /data/{port} \\\n               --logfile /data/{port}/redis.log\n",
            port + 10_000
        ));
    }

    let node_addresses = ports
        .iter()
        .map(|port| format!("127.0.0.1:{port}"))
        .collect::<Vec<_>>()
        .join(" ");
    script.push_str(&format!(
        "until redis-cli -p {} ping >/dev/null 2>&1; do sleep 0.1; done\n\
         redis-cli --cluster create {node_addresses} --cluster-replicas 1 --cluster-yes\n\
         until redis-cli -p {} cluster info | grep -q '^cluster_state:ok'; do sleep 0.1; done\n\
         echo 'rdbinsight cluster ready'\n\
         tail -f /dev/null\n",
        ports[0], ports[0]
    ));
    script
}
