use async_trait::async_trait;
use redis::AsyncCommands;
use semver::Version;

use super::*;

#[derive(Debug, Default, Clone, Copy)]
pub struct StringItem;

impl StringItem {
    const KEY: &'static str = "string_key";
    const VALUE: &'static str = "hello_world";
}

#[async_trait]
impl TestDataItem for StringItem {
    async fn init(&self, conn: &mut redis::aio::MultiplexedConnection) -> AnyResult<()> {
        let _: () = conn.set(Self::KEY, Self::VALUE).await?;
        Ok(())
    }

    fn supports(&self, _version: Version) -> bool {
        true
    }

    fn assert_expected(&self, records: &[Record]) -> AnyResult<()> {
        let record = find_record(records, Self::KEY)?;
        ensure_type(record, RecordType::String, Self::KEY)?;
        ensure_encoding(
            record,
            RecordEncoding::String(StringEncoding::Raw),
            Self::KEY,
        )?;
        ensure_member_count(record, 1, Self::KEY)?;
        Ok(())
    }
}
