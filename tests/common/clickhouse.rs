use anyhow::{Context, Result};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor, wait::HttpWaitStrategy},
    runners::AsyncRunner,
};

pub const CLICKHOUSE_IMAGE: &str = "clickhouse/clickhouse-server";
pub const CLICKHOUSE_TAG: &str = "23.8";
pub const CLICKHOUSE_PORT: u16 = 8123;

pub struct ClickHouseInstance {
    pub container: ContainerAsync<GenericImage>,
    pub internal_url: String,
    pub host_url: String,
}

pub async fn start_clickhouse(network_name: Option<&str>) -> Result<ClickHouseInstance> {
    let mut image = GenericImage::new(CLICKHOUSE_IMAGE, CLICKHOUSE_TAG)
        .with_exposed_port(CLICKHOUSE_PORT.tcp())
        .with_wait_for(WaitFor::http(
            HttpWaitStrategy::new("/ping")
                .with_port(CLICKHOUSE_PORT.tcp())
                .with_response_matcher(|res| res.status().is_success()),
        ))
        .with_env_var("CLICKHOUSE_DB", "rdbinsight")
        .with_env_var("CLICKHOUSE_USER", "default")
        .with_env_var("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1");

    if let Some(net) = network_name {
        image = image.with_network(net.to_string());
    }

    let container = image
        .start()
        .await
        .context("Failed to start ClickHouse container")?;

    let host_port = container
        .get_host_port_ipv4(CLICKHOUSE_PORT)
        .await
        .context("Failed to get ClickHouse container host port")?;

    let internal_ip = container
        .get_bridge_ip_address()
        .await
        .context("Failed to get ClickHouse container bridge IP address")?;

    let internal_url = format!("http://{internal_ip}:{CLICKHOUSE_PORT}");
    let host_url = format!("http://127.0.0.1:{host_port}");

    Ok(ClickHouseInstance {
        container,
        internal_url,
        host_url,
    })
}
