use std::{collections::HashMap, pin::Pin, time::Duration};

use anyhow::{Context as AnyhowContext, ensure};
use async_trait::async_trait;
use redis::Client;
use tracing::{debug, info, warn};

use crate::{
    helper::AnyResult,
    source::{RDBStream, RdbSourceConfig, redis_stream::RedisRdbStream},
};

/// Configuration for Redis Cluster source
#[derive(Clone)]
pub struct Config {
    pub addrs: Vec<String>,
    pub username: Option<String>,
    pub password: Option<String>,
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

    /// Get recommended concurrency based on cluster size
    pub fn recommended_concurrency(&self) -> usize {
        // Suggest concurrency based on cluster size, but respect system limits
        self.addrs.len().min(num_cpus::get()).max(1)
    }
}

#[async_trait]
impl RdbSourceConfig for Config {
    async fn get_rdb_streams(&self) -> AnyResult<Vec<Pin<Box<dyn RDBStream>>>> {
        // Validate configuration first
        self.validate()
            .context("Invalid Redis cluster configuration")?;

        debug!(
            "Connecting to Redis Cluster with {} addresses: {:?}",
            self.addrs.len(),
            self.addrs
        );

        let url = format!(
            "redis://{}:{}@{}",
            self.username.as_deref().unwrap_or(""),
            self.password.as_deref().unwrap_or(""),
            self.addrs.join(",")
        );

        debug!(
            "Using Redis connection URL: redis://{}:***@{}",
            self.username.as_deref().unwrap_or(""),
            self.addrs.join(",")
        );

        let client: Client = Client::open(url).with_context(|| {
            format!(
                "Failed to create Redis client for cluster addresses: {:?}",
                self.addrs
            )
        })?;

        debug!("Establishing connection to Redis cluster...");
        let mut conn = client
            .get_multiplexed_tokio_connection()
            .await
            .with_context(|| {
                format!(
                    "Failed to connect to Redis cluster. Ensure cluster is running and accessible at: {:?}",
                    self.addrs
                )
            })?;
        conn.set_response_timeout(Duration::from_secs(3));

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

        debug!("Cluster nodes info:\n{}", nodes_info);

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
            let source_node = master_to_slaves
                .get(&master.node_id)
                .and_then(|slaves| {
                    debug!("Master {} has {} slaves", master.node_id, slaves.len());
                    // Prefer healthy slaves
                    slaves.iter().find(|slave| slave.is_healthy()).copied()
                        .or_else(|| slaves.first().copied())
                })
                .unwrap_or_else(|| {
                    warn!(
                        "No available slaves found for master {}, using master itself (this may impact performance)",
                        master.node_id
                    );
                    master
                });

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
            );

            streams.push(Box::pin(stream) as Pin<Box<dyn RDBStream>>);
        }

        info!(
            "Found cluster with {} shards: [{}]",
            streams.len(),
            shard_info.join(", ")
        );
        Ok(streams)
    }
}

/// Represents a node in Redis cluster
#[derive(Debug, Clone)]
struct ClusterNode {
    node_id: String,
    address: String,
    flags: Vec<String>,
    master_id: Option<String>,
}

impl ClusterNode {
    fn is_master(&self) -> bool {
        self.flags.contains(&"master".to_string())
    }

    fn is_healthy(&self) -> bool {
        // Check if node is not in fail state
        !self.flags.iter().any(|flag| flag.contains("fail"))
    }
}

fn parse_cluster_nodes(nodes_info: &str) -> AnyResult<Vec<ClusterNode>> {
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
