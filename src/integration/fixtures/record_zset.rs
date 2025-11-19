use anyhow::{anyhow, ensure};
use async_trait::async_trait;
use redis::aio::MultiplexedConnection;
use semver::Version;

use super::TestFixture;
use crate::{
    helper::AnyResult,
    parser::{Item, model::ZSetEncoding},
};

const SMALL_KEY: &str = "integration:zset:small";
const SMALL_MEMBERS: &[(f64, &str)] = &[(1.0, "bronze"), (2.0, "silver"), (3.0, "gold")];
const SKIPLIST_KEY: &str = "integration:zset:skiplist";
const SKIPLIST_MEMBER_COUNT: usize = 256;

#[derive(Debug, Default)]
pub struct SmallZSetRecordFixture;

impl SmallZSetRecordFixture {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl TestFixture for SmallZSetRecordFixture {
    fn name(&self) -> &'static str {
        "small_zset_record_fixture"
    }

    async fn load(&self, conn: &mut MultiplexedConnection) -> AnyResult<()> {
        let members: Vec<(f64, String)> = SMALL_MEMBERS
            .iter()
            .map(|(score, member)| (*score, (*member).to_string()))
            .collect();
        zadd_compat(conn, SMALL_KEY, &members).await
    }

    fn assert(&self, version: &Version, items: &[Item]) -> AnyResult<()> {
        let item = find_by_key(items, SMALL_KEY)?;
        let expected_encoding = if version.major >= 7 {
            ZSetEncoding::ListPack
        } else if version.major >= 2 {
            ZSetEncoding::ZipList
        } else {
            ZSetEncoding::SkipList
        };

        match item {
            Item::ZSetRecord {
                encoding,
                member_count,
                ..
            }
            | Item::ZSet2Record {
                encoding,
                member_count,
                ..
            } => {
                ensure!(
                    *member_count == SMALL_MEMBERS.len() as u64,
                    "unexpected member count {member_count}, expected {}",
                    SMALL_MEMBERS.len()
                );
                if version.major == 7 {
                    ensure!(
                        matches!(encoding, ZSetEncoding::ZipList | ZSetEncoding::ListPack),
                        "unexpected zset encoding {encoding:?} for Redis {version}"
                    );
                } else {
                    ensure!(
                        *encoding == expected_encoding,
                        "unexpected zset encoding {encoding:?}, expected {expected_encoding:?} for Redis {version}"
                    );
                }
            }
            _ => unreachable!("checked zset variants"),
        }

        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct SkipListZSetRecordFixture;

impl SkipListZSetRecordFixture {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl TestFixture for SkipListZSetRecordFixture {
    fn name(&self) -> &'static str {
        "skiplist_zset_record_fixture"
    }

    async fn load(&self, conn: &mut MultiplexedConnection) -> AnyResult<()> {
        let members = large_members();
        zadd_compat(conn, SKIPLIST_KEY, &members).await
    }

    fn assert(&self, version: &Version, items: &[Item]) -> AnyResult<()> {
        let item = find_by_key(items, SKIPLIST_KEY)?;
        let expect_zset2 = version.major >= 4;

        match (expect_zset2, item) {
            (
                true,
                Item::ZSet2Record {
                    encoding,
                    member_count,
                    ..
                },
            ) => {
                ensure!(
                    matches!(encoding, ZSetEncoding::SkipList),
                    "expected skiplist encoding for ZSET2 but got {encoding:?}"
                );
                ensure!(
                    *member_count == SKIPLIST_MEMBER_COUNT as u64,
                    "unexpected member count {member_count}, expected {}",
                    SKIPLIST_MEMBER_COUNT
                );
            }
            (
                false,
                Item::ZSetRecord {
                    encoding,
                    member_count,
                    ..
                },
            ) => {
                ensure!(
                    matches!(encoding, ZSetEncoding::SkipList),
                    "expected skiplist encoding for large zset but got {encoding:?}"
                );
                ensure!(
                    *member_count == SKIPLIST_MEMBER_COUNT as u64,
                    "unexpected member count {member_count}, expected {}",
                    SKIPLIST_MEMBER_COUNT
                );
            }
            (true, other) => {
                anyhow::bail!("Expected ZSet2 record for Redis {version} but got {other:?}");
            }
            (false, other) => {
                anyhow::bail!("Redis {version} should emit legacy ZSet record but got {other:?}");
            }
        }

        Ok(())
    }
}

fn find_by_key<'a>(items: &'a [Item], key: &str) -> AnyResult<&'a Item> {
    items
        .iter()
        .filter(|item| matches!(item, Item::ZSetRecord { .. } | Item::ZSet2Record { .. }))
        .find(|item| item.key().is_some_and(|candidate| candidate == key))
        .ok_or_else(|| {
            anyhow!(
                "Expected to find zset record with key '{key}' but none found. Total items: {}",
                items.len()
            )
        })
}

fn large_members() -> Vec<(f64, String)> {
    (0..SKIPLIST_MEMBER_COUNT)
        .map(|idx| (idx as f64, format!("member-{idx:04}")))
        .collect()
}

async fn zadd_compat(
    conn: &mut MultiplexedConnection,
    key: &str,
    members: &[(f64, String)],
) -> AnyResult<()> {
    let mut cmd = redis::cmd("ZADD");
    cmd.arg(key);
    for (score, member) in members {
        cmd.arg(*score).arg(member);
    }
    match cmd.query_async::<()>(conn).await {
        Ok(_) => Ok(()),
        Err(err) if is_legacy_zadd_multi_error(&err) => {
            for (score, member) in members {
                redis::cmd("ZADD")
                    .arg(key)
                    .arg(*score)
                    .arg(member)
                    .query_async::<()>(conn)
                    .await?;
            }
            Ok(())
        }
        Err(err) => Err(err.into()),
    }
}

fn is_legacy_zadd_multi_error(err: &redis::RedisError) -> bool {
    err.kind() == redis::ErrorKind::ResponseError
        && err
            .to_string()
            .to_lowercase()
            .contains("wrong number of arguments for 'zadd'")
}
