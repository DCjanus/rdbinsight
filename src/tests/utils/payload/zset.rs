use async_trait::async_trait;
use redis::AsyncCommands;
use semver::Version;

use super::*;

#[derive(Debug, Default, Clone, Copy)]
pub struct ZSetItem;

impl ZSetItem {
    const KEY: &'static str = "zset_key";
    const ENTRIES: [(f64, &'static str); 3] =
        [(1.0, "member1"), (2.0, "member2"), (3.0, "member3")];
}

#[async_trait]
impl TestDataItem for ZSetItem {
    async fn init(&self, conn: &mut redis::aio::MultiplexedConnection) -> AnyResult<()> {
        let _: () = conn.zadd_multiple(Self::KEY, &Self::ENTRIES).await?;
        Ok(())
    }

    fn supports(&self, _version: Version) -> bool {
        true
    }

    fn assert_expected(&self, records: &[Record]) -> AnyResult<()> {
        let record = find_record(records, Self::KEY)?;
        ensure_type(record, RecordType::ZSet, Self::KEY)?;
        ensure!(
            matches!(record.encoding, RecordEncoding::ZSet(_)),
            "key '{}' expected zset encoding, got {:?}",
            Self::KEY,
            record.encoding
        );
        ensure_member_count(record, Self::ENTRIES.len() as u64, Self::KEY)?;
        Ok(())
    }
}
