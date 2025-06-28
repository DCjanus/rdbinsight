#![allow(dead_code)]

use anyhow::Result;
use redis::aio::MultiplexedConnection as AsyncConnection;
use tracing_subscriber::EnvFilter;

pub mod setup;
pub mod trace;
pub mod utils;

pub fn init_log_for_debug() {
    let filter: EnvFilter = "info,rdbinsight=debug".parse().expect("invalid filter");
    // Ignore error if the global subscriber has already been set (which happens when multiple
    // tests call this helper).
    let _ = tracing_subscriber::fmt::fmt()
        .with_env_filter(filter)
        .try_init();
}

pub async fn seed_list(conn: &mut AsyncConnection, key: &str, count: usize) -> Result<()> {
    let mut pipe = redis::pipe();
    for idx in 0..count {
        pipe.rpush(key, idx.to_string()).ignore();
    }
    pipe.query_async::<()>(&mut *conn).await?;
    Ok(())
}

pub async fn seed_set(conn: &mut AsyncConnection, key: &str, count: usize) -> Result<()> {
    let mut pipe = redis::pipe();
    for idx in 0..count {
        pipe.sadd(key, idx.to_string()).ignore();
    }
    pipe.query_async::<()>(&mut *conn).await?;
    Ok(())
}

pub async fn seed_hash(conn: &mut AsyncConnection, key: &str, count: usize) -> Result<()> {
    let mut pipe = redis::pipe();
    for idx in 0..count {
        pipe.hset(key, format!("f{}", idx), format!("v{}", idx))
            .ignore();
    }
    pipe.query_async::<()>(conn).await?;
    Ok(())
}

pub async fn config_set_many(conn: &mut AsyncConnection, pairs: &[(&str, &str)]) -> Result<()> {
    let mut pipe = redis::pipe();
    for (key, value) in pairs {
        pipe.cmd("CONFIG").arg("SET").arg(*key).arg(*value).ignore();
    }
    pipe.query_async::<()>(&mut *conn).await?;
    Ok(())
}

pub async fn seed_zset(conn: &mut AsyncConnection, key: &str, count: usize) -> Result<()> {
    // Insert `count` members with score = idx as f64.
    // Use pipeline to batch commands for performance.
    let mut pipe = redis::pipe();
    for idx in 0..count {
        pipe.cmd("ZADD")
            .arg(key)
            .arg(idx as isize)
            .arg(format!("m{}", idx))
            .ignore();
    }
    pipe.query_async::<()>(&mut *conn).await?;
    Ok(())
}

pub async fn seed_stream(conn: &mut AsyncConnection, key: &str, count: usize) -> Result<()> {
    // Insert `count` messages into a Redis stream using XADD.
    let mut pipe = redis::pipe();
    for idx in 0..count {
        // Each entry will have a field "f" with value idx as string.
        // Use * to let Redis assign the ID.
        pipe.cmd("XADD")
            .arg(key)
            .arg("*")
            .arg("f")
            .arg(idx.to_string())
            .ignore();
    }
    pipe.query_async::<()>(&mut *conn).await?;
    Ok(())
}

pub async fn create_stream_groups(
    conn: &mut AsyncConnection,
    key: &str,
    groups: &[&str],
) -> Result<()> {
    for group in groups {
        // Create consumer group starting from the beginning (ID 0) and ensure the stream exists.
        redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(key)
            .arg(*group)
            .arg("0")
            .arg("MKSTREAM")
            .query_async::<()>(&mut *conn)
            .await?;
    }
    Ok(())
}
