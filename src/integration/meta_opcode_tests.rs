use std::time::Duration;

use anyhow::ensure;
use redis::aio::MultiplexedConnection;
use tokio::time::sleep;

use super::redis::{RedisConfig, RedisPreset};
use crate::{helper::AnyResult, parser::Item};

const MAXMEMORY_BYTES: &str = "5mb";
const IDLE_KEY: &str = "integration:meta:idle";
const FREQ_KEY: &str = "integration:meta:freq";
const KEY_VALUE: &str = "v";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_opcode_emitted_under_lru_policy() -> AnyResult<()> {
    let env = RedisConfig::from_preset(RedisPreset::Redis8_4_0)
        .build()
        .await?;

    let mut conn = env.connection().await?;
    configure_eviction_policy(&mut conn, "allkeys-lru").await?;
    seed_simple_key(&mut conn, IDLE_KEY).await?;
    sleep(Duration::from_millis(200)).await;
    drop(conn);

    let items = env.collect_items().await?;

    ensure!(
        items.iter().any(|item| matches!(item, Item::Idle { .. })),
        "Idle opcode not found in parsed items",
    );
    ensure!(
        items
            .iter()
            .any(|item| matches!(item, Item::StringRecord { key, .. } if key == IDLE_KEY)),
        "Expected StringRecord for '{IDLE_KEY}' not found",
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn freq_opcode_emitted_under_lfu_policy() -> AnyResult<()> {
    let env = RedisConfig::from_preset(RedisPreset::Redis8_4_0)
        .build()
        .await?;

    let mut conn = env.connection().await?;
    configure_lfu_policy(&mut conn).await?;
    seed_simple_key(&mut conn, FREQ_KEY).await?;
    bump_frequency(&mut conn, FREQ_KEY, 64).await?;
    drop(conn);

    let items = env.collect_items().await?;

    ensure!(
        items.iter().any(|item| matches!(item, Item::Freq { .. })),
        "Freq opcode not found in parsed items",
    );
    ensure!(
        items
            .iter()
            .any(|item| matches!(item, Item::StringRecord { key, .. } if key == FREQ_KEY)),
        "Expected StringRecord for '{FREQ_KEY}' not found",
    );

    Ok(())
}

async fn configure_eviction_policy(
    conn: &mut MultiplexedConnection,
    policy: &str,
) -> AnyResult<()> {
    let mut pipe = redis::pipe();
    pipe.cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory")
        .arg(MAXMEMORY_BYTES)
        .ignore();
    pipe.cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory-policy")
        .arg(policy)
        .ignore();
    pipe.query_async::<()>(&mut *conn).await?;
    Ok(())
}

async fn configure_lfu_policy(conn: &mut MultiplexedConnection) -> AnyResult<()> {
    let mut pipe = redis::pipe();
    pipe.cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory")
        .arg(MAXMEMORY_BYTES)
        .ignore();
    pipe.cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory-policy")
        .arg("allkeys-lfu")
        .ignore();
    pipe.cmd("CONFIG")
        .arg("SET")
        .arg("lfu-log-factor")
        .arg("0")
        .ignore();
    pipe.cmd("CONFIG")
        .arg("SET")
        .arg("lfu-decay-time")
        .arg("1")
        .ignore();
    pipe.query_async::<()>(&mut *conn).await?;
    Ok(())
}

async fn seed_simple_key(conn: &mut MultiplexedConnection, key: &str) -> AnyResult<()> {
    redis::cmd("SET")
        .arg(key)
        .arg(KEY_VALUE)
        .query_async::<()>(&mut *conn)
        .await?;
    Ok(())
}

async fn bump_frequency(conn: &mut MultiplexedConnection, key: &str, hits: usize) -> AnyResult<()> {
    for _ in 0..hits {
        let _: String = redis::cmd("GET").arg(key).query_async(&mut *conn).await?;
    }
    Ok(())
}
