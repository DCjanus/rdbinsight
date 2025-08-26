use anyhow::{Context, Result};
use rdbinsight::helper::AnyResult;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor, wait::HttpWaitStrategy},
    runners::AsyncRunner,
};
use tracing::debug;
use url::Url;

mod common;
use common::clickhouse::start_clickhouse;

// Kept for historical context; now handled in common::clickhouse
#[allow(dead_code)]
const CLICKHOUSE_IMAGE: &str = "clickhouse/clickhouse-server";
#[allow(dead_code)]
const CLICKHOUSE_TAG: &str = "23.8";
#[allow(dead_code)]
const CLICKHOUSE_PORT: u16 = 8123;

#[derive(Debug, Clone, PartialEq, Default)]
pub enum ProxyType {
    #[default]
    Http,
    Socks5,
}

#[derive(Debug, Clone, Default)]
pub struct TestInfrastructureConfig {
    pub clickhouse: bool,
    pub proxy_enabled: bool,
    pub proxy_type: ProxyType,
    pub proxy_username: Option<String>,
    pub proxy_password: Option<String>,
}

struct ClickHouseSetup {
    _container: ContainerAsync<GenericImage>,
    internal_url: String,
    host_url: String,
}

struct ProxySetup {
    _container: ContainerAsync<GenericImage>,
    url: String,
}

struct TestInfrastructure {
    clickhouse: ClickHouseSetup,
    proxy: Option<ProxySetup>,
    _network_name: String,
}

impl TestInfrastructure {
    async fn start(config: TestInfrastructureConfig) -> Result<Self> {
        let test_id = rand::random::<u32>().to_be_bytes();
        let network_name = format!("rdbinsight-test-{}", hex::encode(test_id));

        let clickhouse = Self::setup_clickhouse(&network_name)
            .await
            .context("Failed to setup ClickHouse")?;

        let proxy = if config.proxy_enabled {
            Some(
                Self::setup_proxy(&network_name, &config)
                    .await
                    .context("Failed to setup proxy")?,
            )
        } else {
            None
        };

        Ok(Self {
            clickhouse,
            proxy,
            _network_name: network_name,
        })
    }

    async fn setup_clickhouse(network_name: &str) -> Result<ClickHouseSetup> {
        let inst = start_clickhouse(Some(network_name)).await?;
        debug!("ClickHouse container started");
        Ok(ClickHouseSetup {
            _container: inst.container,
            internal_url: inst.internal_url,
            host_url: inst.host_url,
        })
    }

    async fn setup_proxy(
        network_name: &str,
        config: &TestInfrastructureConfig,
    ) -> Result<ProxySetup> {
        const GOST_IMAGE: &str = "gogost/gost";
        const GOST_TAG: &str = "3";
        const GOST_PORT: u16 = 8080;
        const GOST_METRICS_PORT: u16 = 9090;

        let mut cmd = vec![
            "-L".to_string(),
            "-metrics".to_string(),
            format!(":{GOST_METRICS_PORT}"),
        ];

        let listen_arg = Self::build_proxy_listen_arg(config, GOST_PORT);
        cmd.insert(1, listen_arg);

        let gost_image = GenericImage::new(GOST_IMAGE, GOST_TAG)
            .with_exposed_port(GOST_PORT.tcp())
            .with_exposed_port(GOST_METRICS_PORT.tcp())
            .with_wait_for(WaitFor::http(
                HttpWaitStrategy::new("/metrics")
                    .with_port(GOST_METRICS_PORT.tcp())
                    .with_response_matcher(|res| res.status().is_success()),
            ))
            .with_network(network_name.to_string())
            .with_cmd(cmd);

        let gost_container = gost_image.start().await?;
        let proxy_type_name = match config.proxy_type {
            ProxyType::Http => "HTTP",
            ProxyType::Socks5 => "SOCKS5",
        };
        debug!("GOST {} proxy container started", proxy_type_name);

        let proxy_host_port = gost_container.get_host_port_ipv4(GOST_PORT).await?;
        let url = Self::build_proxy_url(config, proxy_host_port);

        Ok(ProxySetup {
            _container: gost_container,
            url,
        })
    }

    fn build_proxy_listen_arg(config: &TestInfrastructureConfig, port: u16) -> String {
        let scheme = match config.proxy_type {
            ProxyType::Http => "http",
            ProxyType::Socks5 => "socks5",
        };

        match (&config.proxy_username, &config.proxy_password) {
            (Some(username), Some(password)) => {
                format!("{}://{}:{}@:{}", scheme, username, password, port)
            }
            _ => format!("{}://:{}", scheme, port),
        }
    }

    fn build_proxy_url(config: &TestInfrastructureConfig, port: u16) -> String {
        let scheme = match config.proxy_type {
            ProxyType::Http => "http",
            ProxyType::Socks5 => "socks5",
        };

        match (&config.proxy_username, &config.proxy_password) {
            (Some(username), Some(password)) => {
                format!("{scheme}://{username}:{password}@127.0.0.1:{port}")
            }
            _ => format!("{scheme}://127.0.0.1:{port}"),
        }
    }

    /// Returns the ClickHouse URL for access from within the Docker network
    pub fn clickhouse_internal_url(&self) -> &str {
        &self.clickhouse.internal_url
    }

    /// Returns the ClickHouse URL for access from the host machine
    pub fn clickhouse_host_url(&self) -> &str {
        &self.clickhouse.host_url
    }

    /// Returns the complete proxy URL including authentication if configured
    pub fn proxy_url(&self) -> Option<&str> {
        self.proxy.as_ref().map(|p| p.url.as_str())
    }
}

struct TestCase {
    name: &'static str,
    config: TestInfrastructureConfig,
}

async fn run_clickhouse_test(test_case: &TestCase) -> AnyResult {
    common::init_log_for_debug();

    let infrastructure = TestInfrastructure::start(test_case.config.clone())
        .await
        .context("Failed to start test infrastructure")?;

    let clickhouse_url = if test_case.config.proxy_enabled {
        infrastructure.clickhouse_internal_url()
    } else {
        infrastructure.clickhouse_host_url()
    };

    let proxy_url = infrastructure.proxy_url().map(|s| s.to_string());

    if let Some(ref proxy) = proxy_url {
        println!("✅ {} started", test_case.name);
        println!("Proxy URL: {}", proxy);
    }

    let url_with_db = format!("{}?database=rdbinsight", clickhouse_url);
    let client = rdbinsight::config::ClickHouseConfig::new(
        Url::parse(&url_with_db).unwrap(),
        false,
        proxy_url,
    )?
    .create_client()
    .context("Failed to create ClickHouse client")?;

    let result: u16 = client
        .query("SELECT 1+1")
        .fetch_one()
        .await
        .context("Failed to execute query")?;
    assert_eq!(result, 2);

    println!("✅ {} test passed", test_case.name);
    Ok(())
}

#[tokio::test]
async fn test_clickhouse_connections() {
    // TODO: add testcase to clickhouse with authentication
    let test_cases = [
        TestCase {
            name: "Direct connection",
            config: TestInfrastructureConfig {
                clickhouse: true,
                proxy_enabled: false,
                proxy_type: ProxyType::Http,
                proxy_username: None,
                proxy_password: None,
            },
        },
        TestCase {
            name: "Anonymous HTTP proxy",
            config: TestInfrastructureConfig {
                clickhouse: true,
                proxy_enabled: true,
                proxy_type: ProxyType::Http,
                proxy_username: None,
                proxy_password: None,
            },
        },
        TestCase {
            name: "Authenticated HTTP proxy",
            config: TestInfrastructureConfig {
                clickhouse: true,
                proxy_enabled: true,
                proxy_type: ProxyType::Http,
                proxy_username: Some("testuser".to_string()),
                proxy_password: Some("testpass".to_string()),
            },
        },
        TestCase {
            name: "Anonymous SOCKS5 proxy",
            config: TestInfrastructureConfig {
                clickhouse: true,
                proxy_enabled: true,
                proxy_type: ProxyType::Socks5,
                proxy_username: None,
                proxy_password: None,
            },
        },
        TestCase {
            name: "Authenticated SOCKS5 proxy",
            config: TestInfrastructureConfig {
                clickhouse: true,
                proxy_enabled: true,
                proxy_type: ProxyType::Socks5,
                proxy_username: Some("testuser".to_string()),
                proxy_password: Some("testpass".to_string()),
            },
        },
    ];

    for test_case in &test_cases {
        run_clickhouse_test(test_case)
            .await
            .unwrap_or_else(|e| panic!("Test '{}' failed, error:\n {:?}", test_case.name, e));
    }
}
