use anyhow::{anyhow, ensure};
use redis::aio::MultiplexedConnection;

use super::redis::{RedisConfig, RedisPreset};
use crate::{
    helper::AnyResult,
    parser::{Item, model::SetEncoding},
};

const KEY: &str = "integration:set:listpack:int_flags";
const SPECIAL_INTS: [&str; 4] = ["4095", "32767", "8388607", "2147483647"];
const FILLER_MEMBERS: [&str; 3] = ["alpha", "beta", "gamma"];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_listpack_integer_variants_from_redis() -> AnyResult<()> {
    let env = RedisConfig::from_preset(RedisPreset::Redis8_4_0)
        .build()
        .await?;

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
