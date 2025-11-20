use anyhow::{anyhow, ensure};
use redis::aio::MultiplexedConnection;

use super::{
    redis::{RedisConfig, RedisPreset, RedisTestEnv},
    trace,
};
use crate::{
    helper::AnyResult,
    parser::{
        Item,
        model::{ListEncoding, SetEncoding},
    },
};

const KEY: &str = "integration:set:listpack:int_flags";
const SPECIAL_INTS: [&str; 4] = ["4095", "32767", "8388607", "2147483647"];
const FILLER_MEMBERS: [&str; 3] = ["alpha", "beta", "gamma"];
const QUICKLIST_KEY: &str = "integration:list:quicklist2:plain";
const LARGE_ELEMENT_SIZE: usize = 10_000;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_listpack_integer_variants_from_redis() -> AnyResult<()> {
    let env = spawn_default_env().await?;

    let mut conn = env.connection().await?;
    configure_listpack_limits(&mut conn).await?;
    seed_set(&mut conn).await?;
    drop(conn);

    let items = env.collect_items().await?;
    let item = items
        .iter()
        .find(|item| item.key().is_some_and(|key| key == KEY))
        .ok_or_else(|| anyhow!("expected set record for key '{KEY}'"))?;

    match item {
        Item::SetRecord {
            encoding,
            member_count,
            ..
        } => {
            ensure!(
                matches!(encoding, SetEncoding::ListPack),
                "unexpected set encoding {encoding:?}",
            );
            let expected = (SPECIAL_INTS.len() + FILLER_MEMBERS.len()) as u64;
            ensure!(
                *member_count == expected,
                "unexpected member count {member_count}, expected {expected}",
            );
        }
        other => anyhow::bail!("expected SetRecord but got {other:?}"),
    }

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn list_quicklist2_plain_node_from_redis() -> AnyResult<()> {
    let env = spawn_default_env().await?;
    let guard = trace::capture();

    let mut conn = env.connection().await?;
    configure_quicklist_plain(&mut conn).await?;
    seed_quicklist_plain(&mut conn).await?;
    drop(conn);

    let items = env.collect_items().await?;
    let record = items
        .iter()
        .find(|item| matches!(item, Item::ListRecord { key, .. } if *key == QUICKLIST_KEY))
        .ok_or_else(|| anyhow!("expected list record for key '{QUICKLIST_KEY}'"))?;

    match record {
        Item::ListRecord {
            encoding,
            member_count,
            ..
        } => {
            ensure!(
                matches!(encoding, ListEncoding::QuickList2),
                "unexpected list encoding {encoding:?}",
            );
            ensure!(
                *member_count == 1,
                "quicklist plain node should contain exactly one element, got {member_count}",
            );
        }
        _ => anyhow::bail!("expected ListRecord for '{QUICKLIST_KEY}'"),
    }

    ensure!(
        guard.hit("quicklist2.plain"),
        "expected quicklist2.plain trace event; collected {:?}",
        guard.collected(),
    );

    Ok(())
}

async fn configure_listpack_limits(conn: &mut MultiplexedConnection) -> AnyResult<()> {
    let mut pipe = redis::pipe();
    pipe.cmd("CONFIG")
        .arg("SET")
        .arg("set-max-listpack-entries")
        .arg("70000")
        .ignore();
    pipe.cmd("CONFIG")
        .arg("SET")
        .arg("set-max-listpack-value")
        .arg("64")
        .ignore();
    pipe.cmd("CONFIG")
        .arg("SET")
        .arg("rdbcompression")
        .arg("no")
        .ignore();
    pipe.query_async::<()>(&mut *conn).await?;
    Ok(())
}

async fn seed_set(conn: &mut MultiplexedConnection) -> AnyResult<()> {
    let mut pipe = redis::pipe();
    for value in SPECIAL_INTS.iter().chain(FILLER_MEMBERS.iter()) {
        pipe.cmd("SADD").arg(KEY).arg(*value).ignore();
    }
    pipe.query_async::<()>(&mut *conn).await?;
    Ok(())
}

async fn configure_quicklist_plain(conn: &mut MultiplexedConnection) -> AnyResult<()> {
    let mut pipe = redis::pipe();
    pipe.cmd("CONFIG")
        .arg("SET")
        .arg("list-max-listpack-size")
        .arg("-2")
        .ignore();
    pipe.cmd("CONFIG")
        .arg("SET")
        .arg("list-compress-depth")
        .arg("0")
        .ignore();
    pipe.cmd("CONFIG")
        .arg("SET")
        .arg("rdbcompression")
        .arg("no")
        .ignore();
    pipe.query_async::<()>(&mut *conn).await?;
    Ok(())
}

async fn seed_quicklist_plain(conn: &mut MultiplexedConnection) -> AnyResult<()> {
    let payload = "X".repeat(LARGE_ELEMENT_SIZE);
    redis::cmd("RPUSH")
        .arg(QUICKLIST_KEY)
        .arg(payload)
        .query_async::<()>(&mut *conn)
        .await?;
    Ok(())
}

async fn spawn_default_env() -> AnyResult<RedisTestEnv> {
    RedisConfig::from_preset(RedisPreset::Redis8_4_0)
        .build()
        .await
}
