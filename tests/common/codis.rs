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
const PORT_RANGE_START: u16 = 10_000;
const PORT_RANGE_END: u16 = 19_000;

pub struct CodisInstance {
    _codis_container: ContainerAsync<GenericImage>,
    _backend_container: ContainerAsync<GenericImage>,
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

        let backend_image_repo = std::env::var("RDBINSIGHT_TEST_PIKA_IMAGE_REPO")
            .unwrap_or_else(|_| "pikadb/pika".to_string());
        let backend_image_tag = std::env::var("RDBINSIGHT_TEST_PIKA_IMAGE_TAG")
            .unwrap_or_else(|_| "v3.5.6".to_string());
        let mut backend_image = GenericImage::new(backend_image_repo, backend_image_tag)
            .with_wait_for(WaitFor::message_on_stdout(
                "rdbinsight codis backends ready",
            ))
            .with_cmd([
                "sh",
                "-c",
                pika_startup_script(&master_ports, &replica_ports).as_str(),
            ]);
        for port in master_ports.iter().chain(&replica_ports) {
            backend_image = backend_image.with_mapped_port(*port, port.tcp());
        }
        let backend_container = backend_image
            .start()
            .await
            .context("start Codis Pika backends")?;

        let codis_image_repo = std::env::var("RDBINSIGHT_TEST_CODIS_IMAGE_REPO")
            .unwrap_or_else(|_| "pikadb/codis".to_string());
        let codis_image_tag = std::env::var("RDBINSIGHT_TEST_CODIS_IMAGE_TAG")
            .unwrap_or_else(|_| "v3.5.6".to_string());
        let codis_image = GenericImage::new(codis_image_repo, codis_image_tag)
            .with_wait_for(WaitFor::message_on_stdout("rdbinsight codis ready"))
            .with_cmd([
                "sh",
                "-c",
                codis_startup_script(dashboard_port, &master_ports, &replica_ports).as_str(),
            ])
            .with_network("host");
        let codis_container = codis_image
            .start()
            .await
            .context("start Codis control plane")?;

        let instance = Self {
            _codis_container: codis_container,
            _backend_container: backend_container,
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
        let deadline = Instant::now() + Duration::from_secs(30);
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
                "Codis servers did not become ready within 30 seconds"
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

fn pika_startup_script(masters: &[u16], replicas: &[u16]) -> String {
    let mut script = "set -eu\nmkdir -p /tmp/rdbinsight-codis\n".to_string();
    for (master, replica) in masters.iter().zip(replicas) {
        script.push_str(&pika_server_command(*master, None));
        script.push_str(&pika_server_command(*replica, Some(*master)));
    }
    script.push_str("echo 'rdbinsight codis backends ready'\ntail -f /dev/null\n");
    script
}

fn pika_server_command(port: u16, master: Option<u16>) -> String {
    let replication = master
        .map(|master| {
            format!("echo 'slaveof : 127.0.0.1:{master}' >> /tmp/rdbinsight-codis/{port}.conf\n")
        })
        .unwrap_or_default();
    format!(
        "mkdir -p /tmp/rdbinsight-codis/{port}/log \
           /tmp/rdbinsight-codis/{port}/db \
           /tmp/rdbinsight-codis/{port}/dump \
           /tmp/rdbinsight-codis/{port}/dbsync\n\
         cp /pika/conf/pika.conf /tmp/rdbinsight-codis/{port}.conf\n\
         sed -i \
           -e 's|^port :.*|port : {port}|' \
           -e 's|^log-path :.*|log-path : /tmp/rdbinsight-codis/{port}/log/|' \
           -e 's|^db-path :.*|db-path : /tmp/rdbinsight-codis/{port}/db/|' \
           -e 's|^dump-path :.*|dump-path : /tmp/rdbinsight-codis/{port}/dump/|' \
           -e 's|^db-sync-path :.*|db-sync-path : /tmp/rdbinsight-codis/{port}/dbsync/|' \
           -e 's|^pidfile :.*|pidfile : /tmp/rdbinsight-codis/{port}/pika.pid|' \
           -e 's|^instance-mode :.*|instance-mode : sharding|' \
           /tmp/rdbinsight-codis/{port}.conf\n\
         {replication}\
         /pika/bin/pika -c /tmp/rdbinsight-codis/{port}.conf &\n"
    )
}

fn codis_startup_script(dashboard_port: u16, masters: &[u16], replicas: &[u16]) -> String {
    let mut script = format!(
        "set -u\n\
         mkdir -p /tmp/rdbinsight-codis/rootfs\n\
         /codis/bin/codis-dashboard --default-config > /tmp/rdbinsight-codis/dashboard.toml\n\
         sed -i \
           -e 's|^coordinator_name =.*|coordinator_name = \"filesystem\"|' \
           -e 's|^coordinator_addr =.*|coordinator_addr = \"/tmp/rdbinsight-codis/rootfs\"|' \
           -e 's|^product_name =.*|product_name = \"rdbinsight-codis-e2e\"|' \
           -e 's|^product_auth =.*|product_auth = \"\"|' \
           -e 's|^admin_addr =.*|admin_addr = \"0.0.0.0:{dashboard_port}\"|' \
           /tmp/rdbinsight-codis/dashboard.toml\n\
         /codis/bin/codis-dashboard -c /tmp/rdbinsight-codis/dashboard.toml &\n\
         until /codis/bin/codis-admin --dashboard=127.0.0.1:{dashboard_port} model >/dev/null 2>&1; do sleep 0.1; done\n"
    );

    for (index, (master, replica)) in masters.iter().zip(replicas).enumerate() {
        let group = index + 1;
        script.push_str(&format!(
            "/codis/bin/codis-admin --dashboard=127.0.0.1:{dashboard_port} --create-group --gid={group}\n\
             /codis/bin/codis-admin --dashboard=127.0.0.1:{dashboard_port} --group-add --gid={group} --addr=127.0.0.1:{master} --datacenter=test\n\
             /codis/bin/codis-admin --dashboard=127.0.0.1:{dashboard_port} --group-add --gid={group} --addr=127.0.0.1:{replica} --datacenter=test\n"
        ));
    }

    script.push_str("echo 'rdbinsight codis ready'\ntail -f /dev/null\n");
    script
}
