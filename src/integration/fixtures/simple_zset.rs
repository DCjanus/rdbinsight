use anyhow::{anyhow, ensure};
use async_trait::async_trait;
use redis::aio::MultiplexedConnection;
use semver::Version;

use super::TestFixture;
use crate::{
    helper::AnyResult,
    parser::{Item, model::ZSetEncoding},
};

#[derive(Debug, Default)]
pub struct SimpleZSetFixture;

impl SimpleZSetFixture {
    pub fn new() -> Self {
        Self
    }
}

const SMALL_KEY: &str = "integration:zset:small";
const SMALL_MEMBERS: &[(f64, &str)] = &[(1.0, "bronze"), (2.0, "silver"), (3.0, "gold")];
const SKIPLIST_KEY: &str = "integration:zset:skiplist";
const SKIPLIST_MEMBER_COUNT: usize = 256;

#[async_trait]
impl TestFixture for SimpleZSetFixture {
    fn name(&self) -> &'static str {
        "simple_zset_fixture"
    }

    async fn load(&self, conn: &mut MultiplexedConnection) -> AnyResult<()> {
        let small_members: Vec<(f64, String)> = SMALL_MEMBERS
            .iter()
            .map(|(score, member)| (*score, (*member).to_string()))
            .collect();
        zadd_compat(conn, SMALL_KEY, &small_members).await?;

        let skiplist_members = large_members();
        zadd_compat(conn, SKIPLIST_KEY, &skiplist_members).await?;

        Ok(())
    }

    fn assert(&self, version: &Version, items: &[Item]) -> AnyResult<()> {
        let item = items
            .iter()
            .filter(|item| matches!(item, Item::ZSetRecord { .. } | Item::ZSet2Record { .. }))
            .collect::<Vec<_>>();

        let find_by_key = |key: &str| -> AnyResult<&Item> {
            item.iter()
                .copied()
                .find(|item| item.key().is_some_and(|k| k == key))
                .ok_or_else(|| {
                    anyhow!(
                        "Expected to find zset record with key '{}' but none found. Total items: {}",
                        key,
                        items.len()
                    )
                })
        };

        self.assert_small(version, find_by_key(SMALL_KEY)?)?;
        self.assert_skiplist(version, find_by_key(SKIPLIST_KEY)?)?;

        Ok(())
    }
}

impl SimpleZSetFixture {
    fn assert_small(&self, version: &Version, item: &Item) -> AnyResult<()> {
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
                let expected_encoding = if version.major >= 7 {
                    ZSetEncoding::ListPack
                } else if version.major >= 2 {
                    ZSetEncoding::ZipList
                } else {
                    ZSetEncoding::SkipList
                };
                ensure!(
                    *encoding == expected_encoding,
                    "unexpected zset encoding {encoding:?}, expected {expected_encoding:?} for Redis {version}"
                );
            }
            _ => unreachable!("checked zset variants"),
        }

        Ok(())
    }

    fn assert_skiplist(&self, version: &Version, item: &Item) -> AnyResult<()> {
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
