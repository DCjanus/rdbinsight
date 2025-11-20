use anyhow::{anyhow, ensure};
use async_trait::async_trait;
use redis::aio::MultiplexedConnection;
use semver::Version;

use super::TestFixture;
use crate::{
    helper::AnyResult,
    parser::{Item, model::HashEncoding},
};

#[derive(Debug, Default)]
pub struct HashRecordFixture;

impl HashRecordFixture {
    pub fn new() -> Self {
        Self
    }
}

#[derive(Debug, Default)]
pub struct HashZipListRecordFixture;

impl HashZipListRecordFixture {
    pub fn new() -> Self {
        Self
    }
}

#[derive(Debug, Default)]
pub struct HashListPackRecordFixture;

impl HashListPackRecordFixture {
    pub fn new() -> Self {
        Self
    }
}

#[derive(Debug, Default)]
pub struct HashListPackExRecordFixture;

impl HashListPackExRecordFixture {
    pub fn new() -> Self {
        Self
    }
}

#[derive(Debug, Default)]
pub struct HashZipMapRecordFixture;

impl HashZipMapRecordFixture {
    pub fn new() -> Self {
        Self
    }
}

#[derive(Debug, Default)]
pub struct HashMetadataRecordFixture;

impl HashMetadataRecordFixture {
    pub fn new() -> Self {
        Self
    }
}

const KEY: &str = "integration:hash";
const FIELD_COUNT: usize = 700;
const VALUE_PAD: &str = "hash-value-padding-to-force-raw-encoding--------------------------------";
const ZIPLIST_KEY: &str = "integration:hash:ziplist";
const ZIPLIST_FIELD_COUNT: usize = 16;
const LISTPACK_KEY: &str = "integration:hash:listpack";
const LISTPACK_FIELDS: [(&str, &str); 4] = [
    ("lp-field-alpha", "lp-value-alpha"),
    ("lp-field-beta", "lp-value-beta"),
    ("lp-field-gamma", "lp-value-gamma"),
    ("lp-field-delta", "lp-value-delta"),
];
const LISTPACK_EX_KEY: &str = "integration:hash:listpack_ex";
const LISTPACK_EX_FIELDS: [(&str, &str); 3] = [
    ("lpex-field-alpha", "lpex-value-alpha"),
    ("lpex-field-beta", "lpex-value-beta"),
    ("lpex-field-gamma", "lpex-value-gamma"),
];
const LISTPACK_EX_TTL_SECS: usize = 600;
const ZIPMAP_KEY: &str = "integration:hash:zipmap";
const ZIPMAP_FIELDS: [(&str, &str); 3] = [
    ("zipmap-field-a", "zipmap-value-a"),
    ("zipmap-field-b", "zipmap-value-b"),
    ("zipmap-field-c", "zipmap-value-c"),
];
const METADATA_KEY: &str = "integration:hash:metadata";
const METADATA_FIELD_COUNT: usize = 600;
const METADATA_TTL_SECS: usize = 3600;

#[async_trait]
impl TestFixture for HashRecordFixture {
    fn name(&self) -> &'static str {
        "hash_record_fixture"
    }

    fn supported(&self, version: &Version) -> bool {
        version.major >= 2
    }

    async fn load(&self, conn: &mut MultiplexedConnection) -> AnyResult<()> {
        // Redis 2.x only supports single field/value per HSET invocation, so
        // pipeline the individual commands for compatibility across versions.
        let mut pipe = redis::pipe();
        for idx in 0..FIELD_COUNT {
            let field = format!("hash-field-{idx:04}");
            let value = format!("{VALUE_PAD}-{idx:04}");
            pipe.cmd("HSET").arg(KEY).arg(field).arg(value).ignore();
        }
        pipe.query_async::<()>(&mut *conn).await?;

        Ok(())
    }

    fn assert(&self, _: &Version, items: &[Item]) -> AnyResult<()> {
        let item = items
            .iter()
            .filter(|item| matches!(item, Item::HashRecord { .. }))
            .find(|item| item.key().is_some_and(|key| key == KEY))
            .ok_or_else(|| {
                anyhow!(
                    "Expected to find hash record with key '{KEY}' but none found. Total items: {}",
                    items.len()
                )
            })?;

        match item {
            Item::HashRecord {
                encoding,
                pair_count,
                ..
            } => {
                ensure!(
                    *pair_count == FIELD_COUNT as u64,
                    "unexpected hash pair count {pair_count}, expected {FIELD_COUNT}"
                );
                ensure!(
                    matches!(encoding, HashEncoding::Raw),
                    "unexpected hash encoding {encoding:?}, expected Raw"
                );
            }
            _ => unreachable!("checked hash record variant"),
        }

        Ok(())
    }
}

#[async_trait]
impl TestFixture for HashZipListRecordFixture {
    fn name(&self) -> &'static str {
        "hash_ziplist_record_fixture"
    }

    fn supported(&self, version: &Version) -> bool {
        match version.major {
            0 | 1 => false,
            2 => version.minor >= 6,
            3 | 4 | 5 | 6 => true,
            _ => false,
        }
    }

    async fn load(&self, conn: &mut MultiplexedConnection) -> AnyResult<()> {
        let mut pipe = redis::pipe();
        for idx in 0..ZIPLIST_FIELD_COUNT {
            let field = format!("zip-field-{idx:04}");
            let value = format!("zip-value-{idx:04}");
            pipe.cmd("HSET")
                .arg(ZIPLIST_KEY)
                .arg(field)
                .arg(value)
                .ignore();
        }
        pipe.query_async::<()>(&mut *conn).await?;

        Ok(())
    }

    fn assert(&self, _: &Version, items: &[Item]) -> AnyResult<()> {
        let item = items
            .iter()
            .filter(|item| matches!(item, Item::HashRecord { .. }))
            .find(|item| item.key().is_some_and(|key| key == ZIPLIST_KEY))
            .ok_or_else(|| {
                anyhow!(
                    "Expected to find hash record with key '{ZIPLIST_KEY}' but none found. Total items: {}",
                    items.len()
                )
            })?;

        match item {
            Item::HashRecord {
                encoding,
                pair_count,
                ..
            } => {
                ensure!(
                    *pair_count == ZIPLIST_FIELD_COUNT as u64,
                    "unexpected hash pair count {pair_count}, expected {ZIPLIST_FIELD_COUNT}"
                );
                ensure!(
                    matches!(encoding, HashEncoding::ZipList),
                    "unexpected hash encoding {encoding:?}, expected ZipList"
                );
            }
            _ => unreachable!("checked hash record variant"),
        }

        Ok(())
    }
}

#[async_trait]
impl TestFixture for HashListPackRecordFixture {
    fn name(&self) -> &'static str {
        "hash_listpack_record_fixture"
    }

    fn supported(&self, version: &Version) -> bool {
        version.major >= 7
    }

    async fn load(&self, conn: &mut MultiplexedConnection) -> AnyResult<()> {
        let mut pipe = redis::pipe();
        for (field, value) in LISTPACK_FIELDS {
            pipe.cmd("HSET")
                .arg(LISTPACK_KEY)
                .arg(field)
                .arg(value)
                .ignore();
        }
        pipe.query_async::<()>(&mut *conn).await?;

        Ok(())
    }

    fn assert(&self, _: &Version, items: &[Item]) -> AnyResult<()> {
        let item = items
            .iter()
            .filter(|item| matches!(item, Item::HashRecord { .. }))
            .find(|item| item.key().is_some_and(|key| key == LISTPACK_KEY))
            .ok_or_else(|| {
                anyhow!(
                    "Expected to find hash record with key '{}' but none found. Total items: {}",
                    LISTPACK_KEY,
                    items.len()
                )
            })?;

        match item {
            Item::HashRecord {
                encoding,
                pair_count,
                ..
            } => {
                ensure!(
                    matches!(encoding, HashEncoding::ListPack),
                    "unexpected hash encoding {encoding:?}, expected ListPack"
                );
                ensure!(
                    *pair_count == LISTPACK_FIELDS.len() as u64,
                    "unexpected hash pair count {pair_count}, expected {}",
                    LISTPACK_FIELDS.len()
                );
            }
            _ => unreachable!("checked hash record variant"),
        }

        Ok(())
    }
}

#[async_trait]
impl TestFixture for HashListPackExRecordFixture {
    fn name(&self) -> &'static str {
        "hash_listpack_ex_record_fixture"
    }

    fn supported(&self, version: &Version) -> bool {
        version.major > 7 || (version.major == 7 && version.minor >= 4)
    }

    async fn load(&self, conn: &mut MultiplexedConnection) -> AnyResult<()> {
        let mut pipe = redis::pipe();
        for (field, value) in LISTPACK_EX_FIELDS {
            pipe.cmd("HSET")
                .arg(LISTPACK_EX_KEY)
                .arg(field)
                .arg(value)
                .ignore();
        }

        let cmd = pipe
            .cmd("HEXPIRE")
            .arg(LISTPACK_EX_KEY)
            .arg(LISTPACK_EX_TTL_SECS)
            .arg("FIELDS")
            .arg(LISTPACK_EX_FIELDS.len());
        for (field, _) in LISTPACK_EX_FIELDS {
            cmd.arg(field);
        }
        cmd.ignore();

        pipe.query_async::<()>(&mut *conn).await?;

        Ok(())
    }

    fn assert(&self, _: &Version, items: &[Item]) -> AnyResult<()> {
        let item = items
            .iter()
            .filter(|item| matches!(item, Item::HashRecord { .. }))
            .find(|item| item.key().is_some_and(|key| key == LISTPACK_EX_KEY))
            .ok_or_else(|| {
                anyhow!(
                    "Expected to find hash record with key '{}' but none found. Total items: {}",
                    LISTPACK_EX_KEY,
                    items.len()
                )
            })?;

        match item {
            Item::HashRecord {
                encoding,
                pair_count,
                ..
            } => {
                ensure!(
                    matches!(encoding, HashEncoding::ListPackEx),
                    "unexpected hash encoding {encoding:?}, expected ListPackEx"
                );
                ensure!(
                    *pair_count == LISTPACK_EX_FIELDS.len() as u64,
                    "unexpected hash pair count {pair_count}, expected {}",
                    LISTPACK_EX_FIELDS.len()
                );
            }
            _ => unreachable!("checked hash record variant"),
        }

        Ok(())
    }
}

#[async_trait]
impl TestFixture for HashZipMapRecordFixture {
    fn name(&self) -> &'static str {
        "hash_zipmap_record_fixture"
    }

    fn supported(&self, version: &Version) -> bool {
        version <= &Version::new(2, 6, 0) && version >= &Version::new(2, 0, 0)
    }

    async fn load(&self, conn: &mut MultiplexedConnection) -> AnyResult<()> {
        let mut pipe = redis::pipe();
        for (field, value) in ZIPMAP_FIELDS {
            pipe.cmd("HSET")
                .arg(ZIPMAP_KEY)
                .arg(field)
                .arg(value)
                .ignore();
        }
        pipe.query_async::<()>(&mut *conn).await?;

        Ok(())
    }

    fn assert(&self, _: &Version, items: &[Item]) -> AnyResult<()> {
        let item = items
            .iter()
            .filter(|item| matches!(item, Item::HashRecord { .. }))
            .find(|item| item.key().is_some_and(|key| key == ZIPMAP_KEY))
            .ok_or_else(|| {
                anyhow!(
                    "Expected to find hash record with key '{}' but none found. Total items: {}",
                    ZIPMAP_KEY,
                    items.len()
                )
            })?;

        match item {
            Item::HashRecord {
                encoding,
                pair_count,
                ..
            } => {
                ensure!(
                    matches!(encoding, HashEncoding::ZipMap),
                    "unexpected hash encoding {encoding:?}, expected ZipMap"
                );
                ensure!(
                    *pair_count == ZIPMAP_FIELDS.len() as u64,
                    "unexpected hash pair count {pair_count}, expected {}",
                    ZIPMAP_FIELDS.len()
                );
            }
            _ => unreachable!("checked hash record variant"),
        }

        Ok(())
    }
}

#[async_trait]
impl TestFixture for HashMetadataRecordFixture {
    fn name(&self) -> &'static str {
        "hash_metadata_record_fixture"
    }

    fn supported(&self, version: &Version) -> bool {
        version.major >= 8
    }

    async fn load(&self, conn: &mut MultiplexedConnection) -> AnyResult<()> {
        let mut pipe = redis::pipe();
        for idx in 0..METADATA_FIELD_COUNT {
            let field = format!("meta-field-{idx:04}");
            let value = format!("meta-value-{idx:04}");
            pipe.cmd("HSETEX")
                .arg(METADATA_KEY)
                .arg("EX")
                .arg(METADATA_TTL_SECS)
                .arg("FIELDS")
                .arg(1)
                .arg(&field)
                .arg(&value)
                .ignore();
        }
        pipe.query_async::<()>(&mut *conn).await?;

        Ok(())
    }

    fn assert(&self, _: &Version, items: &[Item]) -> AnyResult<()> {
        let item = items
            .iter()
            .filter(|item| matches!(item, Item::HashRecord { .. }))
            .find(|item| item.key().is_some_and(|key| key == METADATA_KEY))
            .ok_or_else(|| {
                anyhow!(
                    "Expected to find hash record with key '{}' but none found. Total items: {}",
                    METADATA_KEY,
                    items.len()
                )
            })?;

        match item {
            Item::HashRecord {
                encoding,
                pair_count,
                ..
            } => {
                ensure!(
                    matches!(encoding, HashEncoding::Metadata),
                    "unexpected hash encoding {encoding:?}, expected Metadata"
                );
                ensure!(
                    *pair_count == METADATA_FIELD_COUNT as u64,
                    "unexpected hash pair count {pair_count}, expected {}",
                    METADATA_FIELD_COUNT
                );
            }
            _ => unreachable!("checked hash record variant"),
        }

        Ok(())
    }
}
