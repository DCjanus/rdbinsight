use async_trait::async_trait;
use redis::AsyncCommands;
use semver::Version;

use super::*;

#[derive(Debug, Default, Clone, Copy)]
pub struct HashItem;

impl HashItem {
    const KEY: &'static str = "hash_key";
    const FIELDS: [(&'static str, &'static str); 3] = [
        ("field1", "value1"),
        ("field2", "value2"),
        ("field3", "value3"),
    ];
}

#[async_trait]
impl TestDataItem for HashItem {
    async fn init(&self, conn: &mut redis::aio::MultiplexedConnection) -> AnyResult<()> {
        let _: () = conn.hset_multiple(Self::KEY, &Self::FIELDS).await?;
        Ok(())
    }

    fn supports(&self, _version: Version) -> bool {
        true
    }

    fn assert_expected(&self, records: &[Record]) -> AnyResult<()> {
        let record = find_record(records, Self::KEY)?;
        ensure_type(record, RecordType::Hash, Self::KEY)?;
        ensure!(
            matches!(record.encoding, RecordEncoding::Hash(_)),
            "key '{}' expected hash encoding, got {:?}",
            Self::KEY,
            record.encoding
        );
        ensure_member_count(record, Self::FIELDS.len() as u64, Self::KEY)?;
        Ok(())
    }
}
