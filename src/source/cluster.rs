use std::{collections::HashMap, pin::Pin, time::Duration};

use anyhow::{Context as AnyhowContext, ensure};
use async_trait::async_trait;
use redis::cluster::ClusterClientBuilder;
use tracing::{debug, info, warn};

use crate::{
    helper::AnyResult,
    source::{RDBStream, RdbSourceConfig, SourceType, redis_stream::RedisRdbStream},
};

/// Configuration for Redis Cluster source
#[derive(Clone)]
pub struct Config {
    pub addrs: Vec<String>,
    pub username: String,
    pub password: Option<String>,
    pub require_slave: bool,
}

impl Config {
    /// Validate the cluster configuration
    pub fn validate(&self) -> AnyResult<()> {
        ensure!(
            !self.addrs.is_empty(),
            "Redis cluster addresses cannot be empty"
        );

        for addr in &self.addrs {
            ensure!(
                addr.contains(':'),
                "Invalid address format: '{}', expected format 'host:port'",
                addr
            );

            // Basic port validation
            if let Some(port_str) = addr.split(':').next_back() {
                port_str.parse::<u16>().with_context(|| {
                    format!(
                        "Invalid port in address '{addr}': port must be a number between 1-65535"
                    )
                })?;
            }
        }

        Ok(())
    }

    /// Select the appropriate node for a shard based on require_slave setting
    fn select_node_for_shard<'a>(
        &self,
        master: &'a ClusterNode,
        master_to_slaves: &HashMap<String, Vec<&'a ClusterNode>>,
    ) -> AnyResult<&'a ClusterNode> {
        let short_shard_id = &master.node_id[..8.min(master.node_id.len())];

        let empty_vec = vec![];
        let slaves = master_to_slaves.get(&master.node_id).unwrap_or(&empty_vec);

        let slave_candidate = slaves
            .iter()
            .find(|slave| slave.is_healthy())
            .copied()
            .or_else(|| slaves.first().copied());

        if let Some(slave) = slave_candidate {
            return Ok(slave);
        }

        if self.require_slave {
            anyhow::bail!(
                "Shard {} has no available slaves, but require_slave is true",
                short_shard_id
            );
        }

        warn!(
            "No available slaves found for master {}, using master itself (this may impact performance)",
            master.node_id
        );
        Ok(master)
    }
}

#[async_trait]
impl RdbSourceConfig for Config {
    async fn get_rdb_streams(&self) -> AnyResult<Vec<Pin<Box<dyn RDBStream>>>> {
        // Validate configuration first
        self.validate()
            .context("Invalid Redis cluster configuration")?;

        debug!("Building Redis cluster client...");
        let mut builder =
            ClusterClientBuilder::new(self.addrs.iter().map(|addr| format!("redis://{addr}")))
                .connection_timeout(Duration::from_secs(5))
                .response_timeout(Duration::from_secs(3));

        if !self.username.is_empty() {
            builder = builder.username(self.username.clone());
        }

        if let Some(ref password) = self.password {
            builder = builder.password(password.clone());
        }

        let cluster_client = builder.build().with_context(|| {
            format!(
                "Failed to create Redis cluster client for addresses: {:?}",
                self.addrs
            )
        })?;

        debug!("Establishing connection to Redis cluster...");
        let mut conn = cluster_client
            .get_async_connection()
            .await
            .with_context(|| {
                format!(
                    "Failed to connect to Redis cluster. Ensure cluster is running and accessible at: {:?}",
                    self.addrs
                )
            })?;

        debug!("Executing CLUSTER NODES command...");
        let nodes_info: String = redis::cmd("CLUSTER")
            .arg("NODES")
            .query_async(&mut conn)
            .await
            .with_context(|| {
                format!(
                    "Failed to execute CLUSTER NODES command. Ensure this is a valid Redis cluster at: {:?}",
                    self.addrs
                )
            })?;

        debug!(
            operation = "cluster_nodes_info_received",
            nodes_info = %nodes_info,
            "Cluster nodes information received"
        );

        let nodes = parse_cluster_nodes(&nodes_info)
            .context("Failed to parse cluster nodes information")?;

        ensure!(
            !nodes.is_empty(),
            "No valid cluster nodes found. Check cluster configuration."
        );

        let mut master_to_slaves: HashMap<String, Vec<&ClusterNode>> = HashMap::new();

        for node in &nodes {
            if let Some(master_id) = &node.master_id {
                master_to_slaves
                    .entry(master_id.clone())
                    .or_default()
                    .push(node);
            }
        }

        let master_nodes: Vec<&ClusterNode> = nodes.iter().filter(|n| n.is_master()).collect();

        ensure!(
            !master_nodes.is_empty(),
            "No master nodes found in cluster. Cluster may be in invalid state."
        );

        debug!("Found {} master nodes in cluster", master_nodes.len());

        let mut streams = Vec::new();
        let mut shard_info = Vec::new();

        for master in master_nodes {
            let source_node = self.select_node_for_shard(master, &master_to_slaves)?;

            // Use first 8 characters of shard ID for cleaner logging
            let short_shard_id = &master.node_id[..8.min(master.node_id.len())];

            debug!(
                "Selected node {} for shard {} (role: {}, healthy: {})",
                source_node.address,
                short_shard_id,
                if source_node.is_master() {
                    "master"
                } else {
                    "slave"
                },
                source_node.is_healthy()
            );

            shard_info.push(format!(
                "{}:{} ({})",
                short_shard_id,
                source_node.address,
                if source_node.is_master() {
                    "master"
                } else {
                    "slave"
                }
            ));

            let stream = RedisRdbStream::new(
                source_node.address.clone(),
                self.username.clone(),
                self.password.clone(),
                SourceType::Cluster,
            );

            streams.push(Box::pin(stream) as Pin<Box<dyn RDBStream>>);
        }

        info!(
            operation = "cluster_shards_found",
            shard_count = streams.len(),
            shard_info = %shard_info.join(", "),
            "Found cluster shards"
        );
        Ok(streams)
    }
}

/// Represents a node in Redis cluster
#[derive(Debug, Clone)]
pub struct ClusterNode {
    pub node_id: String,
    pub address: String,
    pub flags: Vec<String>,
    pub master_id: Option<String>,
}

impl ClusterNode {
    pub fn is_master(&self) -> bool {
        self.flags.contains(&"master".to_string())
    }

    pub fn is_healthy(&self) -> bool {
        // Check if node is not in fail state
        !self.flags.iter().any(|flag| flag.contains("fail"))
    }
}

pub fn parse_cluster_nodes(nodes_info: &str) -> AnyResult<Vec<ClusterNode>> {
    let mut nodes = Vec::new();

    for (line_num, line) in nodes_info.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 8 {
            warn!(
                "Skipping malformed cluster node line {}: '{}'",
                line_num + 1,
                line
            );
            continue;
        }

        let node_id = parts[0].to_string();
        let address = parts[1].split('@').next().unwrap_or(parts[1]).to_string();
        let flags: Vec<String> = parts[2].split(',').map(|s| s.to_string()).collect();
        let master_id = if parts[3] == "-" {
            None
        } else {
            Some(parts[3].to_string())
        };

        nodes.push(ClusterNode {
            node_id,
            address,
            flags,
            master_id,
        });
    }

    ensure!(
        !nodes.is_empty(),
        "No valid nodes found in cluster nodes output: {}",
        nodes_info
    );

    debug!("Parsed {} cluster nodes", nodes.len());
    Ok(nodes)
}
