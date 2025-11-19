use std::{
    sync::Mutex,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, ensure};
use async_trait::async_trait;
use redis::aio::MultiplexedConnection;
use semver::Version;

use super::TestFixture;
use crate::{
    helper::AnyResult,
    parser::{Item, model::StringEncoding},
};

#[derive(Debug)]
pub struct ExpiringStringFixture {
    expire_at_ms: u64,
    command: Mutex<ExpiryCommand>,
}

impl Default for ExpiringStringFixture {
    fn default() -> Self {
        Self::new()
    }
}

impl ExpiringStringFixture {
    pub fn new() -> Self {
        Self {
            expire_at_ms: compute_expire_at_ms(),
            command: Mutex::new(ExpiryCommand::PExpireAt),
        }
    }

    fn expected_expire_ms(&self) -> u64 {
        match *self.command.lock().expect("poisoned expiry command") {
            ExpiryCommand::PExpireAt => self.expire_at_ms,
            ExpiryCommand::ExpireAt => (self.expire_at_ms / 1000) * 1000,
        }
    }
}

const KEY: &str = "integration:expiry";
const VALUE: &str = "expiring-value";

#[derive(Debug, Clone, Copy)]
enum ExpiryCommand {
    PExpireAt,
    ExpireAt,
}

#[async_trait]
impl TestFixture for ExpiringStringFixture {
    fn name(&self) -> &'static str {
        "expiring_string_fixture"
    }

    async fn load(&self, conn: &mut MultiplexedConnection) -> AnyResult<()> {
        redis::cmd("SET")
            .arg(KEY)
            .arg(VALUE)
            .query_async::<()>(conn)
            .await?;
        match redis::cmd("PEXPIREAT")
            .arg(KEY)
            .arg(self.expire_at_ms)
            .query_async::<()>(conn)
            .await
        {
            Ok(_) => {
                *self.command.lock().expect("poisoned expiry command") = ExpiryCommand::PExpireAt;
            }
            Err(err) if is_legacy_expiry_error(&err) => {
                let expire_at_s = self.expire_at_ms / 1000;
                redis::cmd("EXPIREAT")
                    .arg(KEY)
                    .arg(expire_at_s)
                    .query_async::<()>(conn)
                    .await?;
                *self.command.lock().expect("poisoned expiry command") = ExpiryCommand::ExpireAt;
            }
            Err(err) => return Err(err.into()),
        }
        Ok(())
    }

    fn assert(&self, _: &Version, items: &[Item]) -> AnyResult<()> {
        let mut matched = false;
        let expected_expire_at_ms = self.expected_expire_ms();

        for idx in 0..items.len().saturating_sub(1) {
            if let Item::ExpiryMs { expire_at_ms } = &items[idx] {
                if let Some(next) = items.get(idx + 1)
                    && next.key().is_some_and(|key| key == KEY)
                {
                    ensure!(
                        *expire_at_ms == expected_expire_at_ms,
                        "unexpected expire timestamp {expire_at_ms}, expected {}",
                        expected_expire_at_ms
                    );
                    if let Item::StringRecord { encoding, .. } = next {
                        ensure!(
                            matches!(encoding, StringEncoding::Raw),
                            "unexpected encoding for expiring key: {encoding:?}"
                        );
                    } else {
                        bail!(
                            "expected string record after expiry opcode for key '{KEY}', found {next:?}"
                        );
                    }
                    matched = true;
                    break;
                }
            }
        }

        if !matched {
            bail!(
                "failed to find ExpiryMs opcode paired with key '{KEY}'. Items: {}",
                items.len()
            );
        }

        Ok(())
    }
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

fn is_legacy_expiry_error(err: &redis::RedisError) -> bool {
    err.kind() == redis::ErrorKind::ResponseError
        && err
            .to_string()
            .to_lowercase()
            .contains("unknown command 'pexpireat'")
}
