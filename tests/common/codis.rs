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

const GROUP_COUNT: u16 = 2;
const PORT_COUNT: u16 = 1 + GROUP_COUNT * 2;
const PORT_RANGE_START: u16 = 40_000;
const PORT_RANGE_END: u16 = 55_000;

pub struct CodisInstance {
    _container: ContainerAsync<GenericImage>,
    dashboard_port: u16,
    master_ports: Vec<u16>,
    replica_ports: Vec<u16>,
}

impl CodisInstance {
    pub async fn start() -> Result<Self> {
        let ports = reserve_contiguous_ports()?;
        let dashboard_port = ports[0];
        let master_ports = vec![ports[1], ports[3]];
        let replica_ports = vec![ports[2], ports[4]];
        let startup_script = startup_script(dashboard_port, &master_ports, &replica_ports);
        let image_repo = std::env::var("RDBINSIGHT_TEST_CODIS_IMAGE_REPO")
            .unwrap_or_else(|_| "pikadb/codis".to_string());
        let image_tag = std::env::var("RDBINSIGHT_TEST_CODIS_IMAGE_TAG")
            .unwrap_or_else(|_| "v3.5.6".to_string());

        let mut image = GenericImage::new(image_repo, image_tag)
            .with_wait_for(WaitFor::message_on_stdout("rdbinsight codis ready"))
            .with_cmd(["sh", "-c", startup_script.as_str()]);
        for port in &ports {
            image = image.with_mapped_port(*port, port.tcp());
        }

        let container = image.start().await.context("start Codis container")?;
        let instance = Self {
            _container: container,
            dashboard_port,
            master_ports,
            replica_ports,
        };
        instance
            .wait_until_ready()
            .await
            .context("wait for Codis readiness")?;
        Ok(instance)
    }

    pub fn dashboard_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.dashboard_port)
    }

    pub fn master_ports(&self) -> &[u16] {
        &self.master_ports
    }

    pub fn replica_ports(&self) -> &[u16] {
        &self.replica_ports
    }

    async fn wait_until_ready(&self) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let mut ready = true;
            for port in self.master_ports.iter().chain(&self.replica_ports) {
                let result = async {
                    let client = Client::open(format!("redis://127.0.0.1:{port}"))?;
                    let mut conn = client.get_multiplexed_async_connection().await?;
                    redis::cmd("PING").query_async::<String>(&mut conn).await?;
                    Result::<_, anyhow::Error>::Ok(())
                }
                .await;
                if result.is_err() {
                    ready = false;
                    break;
                }
            }
            if ready {
                return Ok(());
            }
            ensure!(
                Instant::now() < deadline,
                "Codis servers did not become ready within 15 seconds"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

fn reserve_contiguous_ports() -> Result<Vec<u16>> {
    for _ in 0..100 {
        let base = PORT_RANGE_START
            + rand::random::<u16>() % (PORT_RANGE_END - PORT_RANGE_START - PORT_COUNT);
        let mut listeners = Vec::with_capacity(PORT_COUNT as usize);
        let mut ports = Vec::with_capacity(PORT_COUNT as usize);
        for offset in 0..PORT_COUNT {
            let port = base + offset;
            match TcpListener::bind(("127.0.0.1", port)) {
                Ok(listener) => {
                    listeners.push(listener);
                    ports.push(port);
                }
                Err(_) => break,
            }
        }
        if listeners.len() == PORT_COUNT as usize {
            return Ok(ports);
        }
    }
    anyhow::bail!("failed to reserve five contiguous ports for Codis")
}

fn startup_script(dashboard_port: u16, masters: &[u16], replicas: &[u16]) -> String {
    let mut script = format!(
        "set -eu\n\
         mkdir -p /tmp/rdbinsight-codis/rootfs\n\
         printf '%s\\n' \\\n+           'coordinator_name = \"filesystem\"' \\\n+           'coordinator_addr = \"/tmp/rdbinsight-codis/rootfs\"' \\\n+           'product_name = \"rdbinsight-codis-e2e\"' \\\n+           'product_auth = \"\"' \\\n+           'admin_addr = \"0.0.0.0:{dashboard_port}\"' \\\n+           'max_slot_num = 1024' \\\n+           'migration_method = \"semi-async\"' \\\n+           'migration_timeout = \"30s\"' \\\n+           > /tmp/rdbinsight-codis/dashboard.toml\n"
    );

    for (master, replica) in masters.iter().zip(replicas) {
        script.push_str(&server_command(*master, None));
        script.push_str(&server_command(*replica, Some(*master)));
    }

    script.push_str(&format!(
        "/codis/bin/codis-dashboard -c /tmp/rdbinsight-codis/dashboard.toml \\\n+           -l /tmp/rdbinsight-codis/dashboard.log &\n\
         until /codis/bin/codis-admin --dashboard=127.0.0.1:{dashboard_port} model >/dev/null 2>&1; do sleep 0.1; done\n"
    ));

    for (index, (master, replica)) in masters.iter().zip(replicas).enumerate() {
        let group = index + 1;
        let slot_start = index * 512;
        let slot_end = slot_start + 511;
        script.push_str(&format!(
            "/codis/bin/codis-admin --dashboard=127.0.0.1:{dashboard_port} --create-group --gid={group}\n\
             /codis/bin/codis-admin --dashboard=127.0.0.1:{dashboard_port} --group-add --gid={group} --addr=127.0.0.1:{master} --datacenter=test\n\
             /codis/bin/codis-admin --dashboard=127.0.0.1:{dashboard_port} --group-add --gid={group} --addr=127.0.0.1:{replica} --datacenter=test\n\
             /codis/bin/codis-admin --dashboard=127.0.0.1:{dashboard_port} --slots-assign --beg={slot_start} --end={slot_end} --gid={group} --confirm\n\
             /codis/bin/codis-admin --dashboard=127.0.0.1:{dashboard_port} --resync-group --gid={group}\n"
        ));
    }

    script.push_str(
        "echo 'rdbinsight codis ready'\n\
         tail -f /dev/null\n",
    );
    script
}

fn server_command(port: u16, master: Option<u16>) -> String {
    let replication = master
        .map(|master| format!(" --slaveof 127.0.0.1 {master}"))
        .unwrap_or_default();
    format!(
        "/codis/bin/codis-server --port {port} --bind 0.0.0.0 \\\n+           --protected-mode no --save '' --appendonly no --daemonize yes \\\n+           --dir /tmp/rdbinsight-codis --dbfilename {port}.rdb \\\n+           --logfile /tmp/rdbinsight-codis/{port}.log{replication}\n"
    )
}
