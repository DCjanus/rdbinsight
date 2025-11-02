use async_trait::async_trait;
use redis::AsyncCommands;

use super::*;

#[derive(Debug, Default, Clone, Copy)]
pub struct ListItem;

impl ListItem {
    const KEY: &'static str = "list_key";
    const VALUES: [&'static str; 3] = ["item1", "item2", "item3"];
}

#[async_trait]
impl TestDataItem for ListItem {
    async fn init(&self, conn: &mut redis::aio::MultiplexedConnection) -> AnyResult<()> {
        let values: Vec<&str> = Self::VALUES.iter().copied().collect();
        let _: () = conn.rpush(Self::KEY, values).await?;
        Ok(())
    }

    fn supports(&self, _version: RedisVersion) -> bool {
        true
    }

    fn assert_expected(&self, records: &[Record]) -> AnyResult<()> {
        let record = find_record(records, Self::KEY)?;
        ensure_type(record, RecordType::List, Self::KEY)?;
        ensure!(
            matches!(record.encoding, RecordEncoding::List(_)),
            "key '{}' expected list encoding, got {:?}",
            Self::KEY,
            record.encoding
        );
        ensure_member_count(record, Self::VALUES.len() as u64, Self::KEY)?;
        Ok(())
    }
}
