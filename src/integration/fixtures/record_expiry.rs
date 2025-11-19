use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{bail, ensure};
use async_trait::async_trait;
use redis::aio::MultiplexedConnection;
use semver::Version;

use super::TestFixture;
use crate::{
    helper::AnyResult,
    parser::{Item, model::StringEncoding},
};

const KEY_PEXPIRE_AT: &str = "integration:expiry:pexpireat";
const KEY_EXPIRE_AT: &str = "integration:expiry:expireat";
const VALUE: &str = "expiring-value";

#[derive(Debug)]
pub struct PExpireAtRecordFixture {
    expire_at_ms: u64,
}

impl Default for PExpireAtRecordFixture {
    fn default() -> Self {
        Self::new()
    }
}

impl PExpireAtRecordFixture {
    pub fn new() -> Self {
        Self {
            expire_at_ms: compute_expire_at_ms(),
        }
    }
}

#[async_trait]
impl TestFixture for PExpireAtRecordFixture {
    fn name(&self) -> &'static str {
        "pexpireat_record_fixture"
    }

    fn supported(&self, version: &Version) -> bool {
        supports_pexpireat(version)
    }

    async fn load(&self, conn: &mut MultiplexedConnection) -> AnyResult<()> {
        set_value(conn, KEY_PEXPIRE_AT).await?;
        redis::cmd("PEXPIREAT")
            .arg(KEY_PEXPIRE_AT)
            .arg(self.expire_at_ms)
            .query_async::<()>(conn)
            .await?;
        Ok(())
    }

    fn assert(&self, _: &Version, items: &[Item]) -> AnyResult<()> {
        ensure_expiring_string(items, KEY_PEXPIRE_AT, self.expire_at_ms)
    }
}

#[derive(Debug)]
pub struct ExpireAtRecordFixture {
    expire_at_ms: u64,
}

impl Default for ExpireAtRecordFixture {
    fn default() -> Self {
        Self::new()
    }
}

impl ExpireAtRecordFixture {
    pub fn new() -> Self {
        Self {
            expire_at_ms: compute_expire_at_ms(),
        }
    }

    fn expected_expire_ms(&self) -> u64 {
        (self.expire_at_ms / 1000) * 1000
    }
}

#[async_trait]
impl TestFixture for ExpireAtRecordFixture {
    fn name(&self) -> &'static str {
        "expireat_record_fixture"
    }

    async fn load(&self, conn: &mut MultiplexedConnection) -> AnyResult<()> {
        set_value(conn, KEY_EXPIRE_AT).await?;
        let expire_at_s = self.expire_at_ms / 1000;
        redis::cmd("EXPIREAT")
            .arg(KEY_EXPIRE_AT)
            .arg(expire_at_s)
            .query_async::<()>(conn)
            .await?;
        Ok(())
    }

    fn assert(&self, _: &Version, items: &[Item]) -> AnyResult<()> {
        ensure_expiring_string(items, KEY_EXPIRE_AT, self.expected_expire_ms())
    }
}

async fn set_value(conn: &mut MultiplexedConnection, key: &str) -> AnyResult<()> {
    redis::cmd("SET")
        .arg(key)
        .arg(VALUE)
        .query_async::<()>(conn)
        .await?;
    Ok(())
}

fn ensure_expiring_string(items: &[Item], key: &str, expected_expire_at_ms: u64) -> AnyResult<()> {
    for idx in 0..items.len().saturating_sub(1) {
        if let Item::ExpiryMs { expire_at_ms } = &items[idx] {
            if let Some(next) = items.get(idx + 1)
                && next.key().is_some_and(|candidate| candidate == key)
            {
                ensure!(
                    *expire_at_ms == expected_expire_at_ms,
                    "unexpected expire timestamp {expire_at_ms}, expected {}",
                    expected_expire_at_ms
                );
                if let Item::StringRecord { encoding, .. } = next {
                    ensure!(
                        matches!(encoding, StringEncoding::Raw),
                        "unexpected encoding for expiring key {key}: {encoding:?}"
                    );
                    return Ok(());
                } else {
                    bail!(
                        "expected string record after expiry opcode for key '{key}', found {next:?}"
                    );
                }
            }
        }
    }

    bail!(
        "failed to find ExpiryMs opcode paired with key '{key}'. Items: {}",
        items.len()
    );
}

fn compute_expire_at_ms() -> u64 {
    let fallback = SystemTime::now();
    let future = fallback
        .checked_add(Duration::from_secs(24 * 60 * 60))
        .unwrap_or(fallback);
    let duration = future
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0));
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

fn supports_pexpireat(version: &Version) -> bool {
    version.major > 2 || (version.major == 2 && version.minor >= 6)
}
