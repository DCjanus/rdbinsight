use async_trait::async_trait;
use redis::AsyncCommands;

use super::*;

#[derive(Debug, Default, Clone, Copy)]
pub struct SetItem;

impl SetItem {
    const KEY: &'static str = "set_key";
    const MEMBERS: [&'static str; 3] = ["member1", "member2", "member3"];
}

#[async_trait]
impl TestDataItem for SetItem {
    async fn init(&self, conn: &mut redis::aio::MultiplexedConnection) -> AnyResult<()> {
        let members: Vec<&str> = Self::MEMBERS.iter().copied().collect();
        let _: () = conn.sadd(Self::KEY, members).await?;
        Ok(())
    }

    fn supports(&self, _version: RedisVersion) -> bool {
        true
    }

    fn assert_expected(&self, records: &[Record]) -> AnyResult<()> {
        let record = find_record(records, Self::KEY)?;
        ensure_type(record, RecordType::Set, Self::KEY)?;
        ensure!(
            matches!(record.encoding, RecordEncoding::Set(_)),
            "key '{}' expected set encoding, got {:?}",
            Self::KEY,
            record.encoding
        );
        ensure_member_count(record, Self::MEMBERS.len() as u64, Self::KEY)?;
        Ok(())
    }
}
