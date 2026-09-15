use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result, ensure};
use futures_util::StreamExt;
use rdbinsight::{
    config::SourceConfig,
    helper::redis_slot,
    parser::core::raw::RDBStr,
    record::{Record, RecordStream},
    source::{RdbSourceConfig, SourceType},
};
use redis::{AsyncCommands, cluster::ClusterClientBuilder};

use crate::common::redis_cluster::RedisClusterInstance;

mod common;

#[tokio::test(flavor = "current_thread")]
async fn redis_cluster_source_reads_every_shard_from_replicas() -> Result<()> {
    let cluster = RedisClusterInstance::start().await?;
    let keys = keys_spanning_all_shards();
    seed_cluster(&cluster, &keys).await?;

    let source = SourceConfig::RedisCluster {
        cluster_name: "source-cluster-e2e".to_string(),
        addrs: cluster.addresses(),
        username: String::new(),
        password: None,
        require_slave: true,
    };
    let mut streams = source.get_rdb_streams().await?;
    assert_eq!(streams.len(), 3, "expected one RDB stream per shard");

    let replica_ports = replica_ports(&cluster).await?;
    let mut records = Vec::new();
    for mut stream in streams.drain(..) {
        assert_eq!(stream.source_type(), SourceType::Cluster);
        let instance = stream.instance();
        assert!(
            replica_ports
                .iter()
                .any(|port| instance == format!("127.0.0.1:{port}")),
            "expected source stream {instance} to use a replica"
        );
        stream.as_mut().prepare().await?;
        records.extend(collect_records(stream).await?);
    }

    assert_seeded_records(&records, &keys);
    Ok(())
}

fn keys_spanning_all_shards() -> Vec<String> {
    let ranges = [0..=5460, 5461..=10922, 10923..=16383];
    ranges
        .into_iter()
        .enumerate()
        .map(|(shard, range)| {
            (0..10_000)
                .map(|candidate| format!("cluster-e2e:{{shard-{shard}-{candidate}}}"))
                .find(|key| range.contains(&redis_slot(key.as_bytes())))
                .expect("a key should be found for every Redis Cluster slot range")
        })
        .collect()
}

async fn seed_cluster(cluster: &RedisClusterInstance, keys: &[String]) -> Result<()> {
    let client = ClusterClientBuilder::new(
        cluster
            .addresses()
            .into_iter()
            .map(|addr| format!("redis://{addr}")),
    )
    .build()?;
    let mut conn = client.get_async_connection().await?;
    for (index, key) in keys.iter().enumerate() {
        conn.set::<_, _, ()>(key, format!("value-{index}"))
            .await
            .with_context(|| format!("seed key {key}"))?;
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if replicas_contain_keys(cluster, keys).await? {
            return Ok(());
        }
        ensure!(
            std::time::Instant::now() < deadline,
            "cluster replicas did not receive all seeded keys within 10 seconds"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn replica_ports(cluster: &RedisClusterInstance) -> Result<Vec<u16>> {
    let client = redis::Client::open(cluster.initial_node_url())?;
    let mut conn = client.get_multiplexed_async_connection().await?;
    let nodes: String = redis::cmd("CLUSTER")
        .arg("NODES")
        .query_async(&mut conn)
        .await?;
    let ports = nodes
        .lines()
        .filter_map(|line| {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            fields.get(2).filter(|flags| {
                flags
                    .split(',')
                    .any(|flag| flag == "slave" || flag == "replica")
            })?;
            fields
                .get(1)?
                .split('@')
                .next()?
                .rsplit(':')
                .next()?
                .parse::<u16>()
                .ok()
        })
        .collect::<Vec<_>>();
    ensure!(ports.len() == 3, "expected three replicas, got {ports:?}");
    Ok(ports)
}

async fn replicas_contain_keys(cluster: &RedisClusterInstance, keys: &[String]) -> Result<bool> {
    let ports = replica_ports(cluster).await?;
    let mut found = 0;
    for port in ports {
        let client = redis::Client::open(format!("redis://127.0.0.1:{port}"))?;
        let mut conn = client.get_multiplexed_async_connection().await?;
        redis::cmd("READONLY").query_async::<()>(&mut conn).await?;
        for key in keys {
            if redis::cmd("GET")
                .arg(key)
                .query_async::<Option<String>>(&mut conn)
                .await
                .ok()
                .flatten()
                .is_some()
            {
                found += 1;
            }
        }
    }
    Ok(found == keys.len())
}

async fn collect_records(
    stream: std::pin::Pin<Box<dyn rdbinsight::source::RDBStream>>,
) -> Result<Vec<Record>> {
    let mut records = Vec::new();
    let mut record_stream = RecordStream::new(stream, SourceType::Cluster);
    while let Some(record) = record_stream.next().await {
        records.push(record?);
    }
    Ok(records)
}

fn assert_seeded_records(records: &[Record], keys: &[String]) {
    let mut counts = HashMap::new();
    for record in records {
        if let RDBStr::Str(key) = &record.key {
            *counts.entry(key.clone()).or_insert(0usize) += 1;
        }
    }

    for key in keys {
        let bytes = bytes::Bytes::copy_from_slice(key.as_bytes());
        assert_eq!(counts.get(&bytes), Some(&1), "expected key {key} once");
        let record = records
            .iter()
            .find(|record| record.key == RDBStr::Str(bytes.clone()))
            .expect("seeded record should exist");
        assert_eq!(record.redis_slot, Some(redis_slot(key.as_bytes())));
    }
}
