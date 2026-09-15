use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result, ensure};
use futures_util::StreamExt;
use rdbinsight::{
    config::SourceConfig,
    helper::codis_slot,
    parser::core::raw::RDBStr,
    record::{Record, RecordStream},
    source::{RdbSourceConfig, SourceType},
};
use redis::AsyncCommands;

use crate::common::codis::CodisInstance;

mod common;

#[tokio::test(flavor = "current_thread")]
async fn codis_source_discovers_replicas_and_reads_every_group() -> Result<()> {
    let codis = CodisInstance::start().await?;
    let keys = ["codis-e2e:group-1"];
    seed_groups(&codis, &keys).await?;

    let mut source = SourceConfig::Codis {
        cluster_name: None,
        dashboard_addr: codis.dashboard_url(),
        password: None,
        require_slave: true,
    };
    source.preprocess().await?;
    assert_eq!(source.cluster_name(), "rdbinsight-codis-e2e");

    let mut streams = source.get_rdb_streams().await?;
    assert_eq!(streams.len(), 1, "expected one RDB stream per Codis group");

    let mut records = Vec::new();
    for mut stream in streams.drain(..) {
        assert_eq!(stream.source_type(), SourceType::Codis);
        let instance = stream.instance();
        assert!(
            codis
                .replica_ports()
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

async fn seed_groups(codis: &CodisInstance, keys: &[&str]) -> Result<()> {
    for ((master, replica), key) in codis
        .master_ports()
        .iter()
        .zip(codis.replica_ports())
        .zip(keys)
    {
        let client = redis::Client::open(format!("redis://127.0.0.1:{master}"))?;
        let mut conn = client.get_multiplexed_async_connection().await?;
        conn.set::<_, _, ()>(*key, format!("value-from-{master}"))
            .await
            .with_context(|| format!("seed key {key}"))?;
        wait_for_replica(*replica, key).await?;
    }
    Ok(())
}

async fn wait_for_replica(port: u16, key: &str) -> Result<()> {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let result = async {
            let client = redis::Client::open(format!("redis://127.0.0.1:{port}"))?;
            let mut conn = client.get_multiplexed_async_connection().await?;
            let value: Option<String> = conn.get(key).await?;
            Result::<_, anyhow::Error>::Ok(value)
        }
        .await;
        if matches!(result, Ok(Some(_))) {
            return Ok(());
        }
        ensure!(
            std::time::Instant::now() < deadline,
            "replica {port} did not receive key {key} within 10 seconds"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn collect_records(
    stream: std::pin::Pin<Box<dyn rdbinsight::source::RDBStream>>,
) -> Result<Vec<Record>> {
    let mut records = Vec::new();
    let mut record_stream = RecordStream::new(stream, SourceType::Codis);
    while let Some(record) = record_stream.next().await {
        records.push(record?);
    }
    Ok(records)
}

fn assert_seeded_records(records: &[Record], keys: &[&str]) {
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
        assert_eq!(record.codis_slot, Some(codis_slot(key.as_bytes())));
        assert_eq!(record.redis_slot, None);
    }
}
