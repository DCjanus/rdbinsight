use async_trait::async_trait;

use super::*;

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

    fn supports(&self, version: RedisVersion) -> bool {
        !matches!(version, RedisVersion::V1_2_6 | RedisVersion::V2_8_24)
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
