use std::time::Duration;

use anyhow::{bail, ensure};
use redis::aio::MultiplexedConnection;
use tokio::time::sleep;

use super::redis::{RedisConfig, RedisPreset};
use crate::{helper::AnyResult, parser::Item, source::SourceType};

const CLUSTER_CONFIG_FILE: &str = "nodes.conf";
const CLUSTER_KEY: &str = "integration:cluster:slot_info";
const KEY_VALUE: &str = "value";
const TOTAL_SLOTS: usize = 16_384;
const SLOT_CHUNK: usize = 1_024;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slot_info_emitted_from_single_node_cluster() -> AnyResult<()> {
    let env = RedisConfig::from_preset(RedisPreset::Redis8_4_0)
        .with_cluster_config_file(CLUSTER_CONFIG_FILE)
        .build()
        .await?;

    let mut conn = env.connection().await?;
    assign_all_slots(&mut conn).await?;
    wait_for_cluster_ok(&mut conn).await?;
    seed_cluster_key(&mut conn).await?;
    drop(conn);

    let items = env.collect_items_with_type(SourceType::Cluster).await?;

    ensure!(
        items
            .iter()
            .any(|item| matches!(item, Item::SlotInfo { .. })),
        "SlotInfo opcode not found in parsed items",
    );

    ensure!(
        items.iter().any(|item| match item {
            Item::StringRecord { key, .. } => key == CLUSTER_KEY,
            _ => false,
        }),
        "Expected StringRecord for '{CLUSTER_KEY}' not found",
    );

    Ok(())
}

async fn assign_all_slots(conn: &mut MultiplexedConnection) -> AnyResult<()> {
    let mut slot = 0usize;
    while slot < TOTAL_SLOTS {
        let upper = usize::min(slot + SLOT_CHUNK, TOTAL_SLOTS);
        let mut cmd = redis::cmd("CLUSTER");
        cmd.arg("ADDSLOTS");
        for current in slot..upper {
            cmd.arg(current);
        }
        cmd.query_async::<()>(&mut *conn).await?;
        slot = upper;
    }
    Ok(())
}

async fn wait_for_cluster_ok(conn: &mut MultiplexedConnection) -> AnyResult<()> {
    for _ in 0..100 {
        let info: String = redis::cmd("CLUSTER")
            .arg("INFO")
            .query_async(&mut *conn)
            .await?;
        if info.lines().any(|line| line.trim() == "cluster_state:ok") {
            return Ok(());
        }
        sleep(Duration::from_millis(100)).await;
    }
    bail!("cluster_state never became ok");
}

async fn seed_cluster_key(conn: &mut MultiplexedConnection) -> AnyResult<()> {
    redis::cmd("SET")
        .arg(CLUSTER_KEY)
        .arg(KEY_VALUE)
        .query_async::<()>(&mut *conn)
        .await?;
    Ok(())
}
