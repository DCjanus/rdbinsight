use async_trait::async_trait;

use super::*;
use crate::tests::redis::version::REDIS_V5_0_14;

#[derive(Debug, Default, Clone, Copy)]
pub struct StreamItem;

impl StreamItem {
    const KEY: &'static str = "stream_key";
    const ENTRIES: [(&'static str, &'static [(&'static str, &'static str)]); 2] = [
        ("0-1", &[("field1", "value1")]),
        ("0-2", &[("field2", "value2")]),
    ];
}

#[async_trait]
impl TestDataItem for StreamItem {
    async fn init(&self, conn: &mut redis::aio::MultiplexedConnection) -> AnyResult<()> {
        for &(id, fields) in Self::ENTRIES.iter() {
            let mut cmd = redis::cmd("XADD");
            cmd.arg(Self::KEY).arg(id);
            for &(field, value) in fields.iter() {
                cmd.arg(field).arg(value);
            }
            let _: String = cmd.query_async(conn).await?;
        }
        Ok(())
    }

    fn supports(&self, version: Version) -> bool {
        version.major >= 5
    }

    fn assert_expected(&self, records: &[Record]) -> AnyResult<()> {
        let record = find_record(records, Self::KEY)?;
        ensure_type(record, RecordType::Stream, Self::KEY)?;
        ensure_encoding(
            record,
            RecordEncoding::Stream(StreamEncoding::ListPacks),
            Self::KEY,
        )?;
        ensure_member_count(record, Self::ENTRIES.len() as u64, Self::KEY)?;
        Ok(())
    }
}
