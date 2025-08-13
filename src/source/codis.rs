// Codis source implementation for RDBInsight
// This module provides support for fetching RDB data from Codis clusters
// by interacting with the Codis Dashboard API.

use std::time::Duration;

use anyhow::{Context as AnyhowContext, anyhow, bail, ensure};
use reqwest::Client;
use serde::Deserialize;
use tracing::{debug, info};

use crate::helper::AnyResult;

#[derive(Debug, Deserialize)]
pub struct CodisOverview {
    pub version: String,
    pub compile: String,
    pub model: Option<CodisModel>,
    pub stats: Option<CodisStats>,
}

#[derive(Debug, Deserialize)]
pub struct CodisModel {
    pub product_name: String,
}

#[derive(Debug, Deserialize)]
pub struct CodisStats {
    pub group: CodisGroupStats,
}

#[derive(Debug, Deserialize)]
pub struct CodisGroupStats {
    pub models: Vec<Group>,
}

#[derive(Debug, Deserialize)]
pub struct Group {
    pub id: u32,
    pub servers: Vec<GroupServer>,
}

#[derive(Debug, Deserialize)]
pub struct GroupServer {
    #[serde(rename = "server")]
    pub addr: String, // "host:port"
    pub datacenter: Option<String>,
    pub replica_group: bool,
}

pub struct CodisClient {
    http_client: Client,
    dashboard_addr: String,
}

impl CodisClient {
    pub fn new(dashboard_addr: &str) -> Self {
        let http_client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            http_client,
            dashboard_addr: dashboard_addr.to_string(),
        }
    }

    /// Select the appropriate server for a group based on require_slave setting
    fn select_server_for_group<'a>(
        &self,
        group: &'a Group,
        require_slave: bool,
    ) -> AnyResult<&'a GroupServer> {
        ensure!(
            !group.servers.is_empty(),
            "Group {} has no servers",
            group.id
        );

        let (master, slaves) = group
            .servers
            .split_first()
            .expect("Group has servers checked above");

        let no_read_slave = slaves.iter().find(|s| !s.replica_group);
        let read_slave = slaves.iter().find(|s| s.replica_group);

        if require_slave && no_read_slave.is_none() {
            if read_slave.is_none() {
                bail!(
                    "Group {} has no replica servers, but require_slave is true",
                    group.id
                );
            } else {
                bail!(
                    "Group {} only has replicas with read traffic, but require_slave is true",
                    group.id
                );
            }
        }

        let selected = no_read_slave.or(read_slave).unwrap_or(master);

        Ok(selected)
    }

    /// Fetch Codis overview data from Dashboard
    async fn fetch_overview(&self) -> AnyResult<CodisOverview> {
        let api_url = format!("{}/topom", self.dashboard_addr);

        debug!(
            operation = "codis_overview_fetch",
            api_url = %api_url,
            "Fetching Codis overview"
        );

        let response = self
            .http_client
            .get(&api_url)
            .send()
            .await
            .with_context(|| format!("Failed to fetch overview from Codis Dashboard: {api_url}"))?;

        if !response.status().is_success() {
            return Err(anyhow!(
                "Codis Dashboard API returned error: {} - {}",
                response.status(),
                response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Unknown error".to_string())
            ));
        }

        let overview: CodisOverview = response
            .json()
            .await
            .context("Failed to parse JSON response from Codis Dashboard")?;

        Ok(overview)
    }

    /// Fetch product name from Codis Dashboard
    pub async fn fetch_product_name(&self) -> AnyResult<String> {
        let overview = self.fetch_overview().await?;

        let product_name = overview
            .model
            .as_ref()
            .ok_or_else(|| anyhow!("Failed to get model information from Codis Dashboard"))?
            .product_name
            .clone();

        info!(
            operation = "codis_product_name_fetched",
            product_name = %product_name,
            "Successfully fetched Codis product name"
        );

        Ok(product_name)
    }

    /// Fetch Redis instances from Codis Dashboard
    /// Returns redis_addresses
    pub async fn fetch_redis_instances(&self, require_slave: bool) -> AnyResult<Vec<String>> {
        let overview = self.fetch_overview().await?;

        let product_name = overview
            .model
            .as_ref()
            .ok_or_else(|| anyhow!("Failed to get model information from Codis Dashboard"))?
            .product_name
            .clone();

        let empty_groups = vec![];
        let groups = overview
            .stats
            .as_ref()
            .map(|s| &s.group.models)
            .unwrap_or(&empty_groups);

        let mut redis_addrs = Vec::new();
        let mut group_info = Vec::new();

        for group in groups {
            debug!(
                operation = "group_processing",
                group_id = group.id,
                servers_count = group.servers.len(),
                "Processing group"
            );
            if group.servers.is_empty() {
                continue;
            }

            let selected_server = self.select_server_for_group(group, require_slave)?;
            let server_type =
                if group.servers.first().map(|s| &s.addr) == Some(&selected_server.addr) {
                    "master"
                } else {
                    "replica"
                };

            debug!(
                operation = "server_selected",
                group_id = group.id,
                server_addr = %selected_server.addr,
                server_type = server_type,
                "Selected server from group"
            );

            group_info.push(format!(
                "group{}:{} ({})",
                group.id, selected_server.addr, server_type
            ));

            redis_addrs.push(selected_server.addr.clone());
        }

        ensure!(
            !redis_addrs.is_empty(),
            "No Redis instances found in Codis cluster"
        );

        info!(
            operation = "codis_instances_selected",
            instance_count = redis_addrs.len(),
            cluster_name = %product_name,
            group_info = %group_info.join(", "),
            "Selected Redis instances from Codis cluster"
        );

        Ok(redis_addrs)
    }
}
