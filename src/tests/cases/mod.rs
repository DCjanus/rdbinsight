use anyhow::{bail, ensure};
use async_trait::async_trait;

use crate::{
    helper::AnyResult,
    parser::{
        core::raw::RDBStr,
        model::{StreamEncoding, StringEncoding},
    },
    record::{Record, RecordEncoding, RecordType},
    tests::redis::RedisVersion,
};

const DEFAULT_DB: u64 = 0;

mod hash;
mod list;
mod set;
mod stream;
mod string;
mod zset;

/// Common contract for test data items that can seed Redis and validate parsed records.
#[async_trait]
pub trait TestDataItem {
    /// Seed Redis with the data for this test item.
    async fn init(&self, conn: &mut redis::aio::MultiplexedConnection) -> AnyResult<()>;

    /// Return `true` if this data item is supported on the provided Redis version.
    fn supports(&self, version: RedisVersion) -> bool;

    /// Validate parsed records against expectations.
    fn assert_expected(&self, records: &[Record]) -> AnyResult<()>;
}

fn find_record<'a>(records: &'a [Record], key: &str) -> AnyResult<&'a Record> {
    for record in records {
        if record.db == DEFAULT_DB && key_matches(&record.key, key) {
            return Ok(record);
        }
    }
    bail!("expected key '{}' in db {}", key, DEFAULT_DB);
}

fn ensure_type(record: &Record, expected: RecordType, key: &str) -> AnyResult<()> {
    ensure!(
        record.r#type == expected,
        "key '{}' expected type {:?}, got {:?}",
        key,
        expected,
        record.r#type
    );
    Ok(())
}

fn ensure_encoding(record: &Record, expected: RecordEncoding, key: &str) -> AnyResult<()> {
    ensure!(
        record.encoding == expected,
        "key '{}' expected encoding {:?}, got {:?}",
        key,
        expected,
        record.encoding
    );
    Ok(())
}

fn ensure_member_count(record: &Record, expected: u64, key: &str) -> AnyResult<()> {
    ensure!(
        record.member_count.is_some(),
        "key '{}' expected member count metadata",
        key
    );
    let actual = record.member_count.unwrap();
    ensure!(
        actual == expected,
        "key '{}' expected member count {}, got {}",
        key,
        expected,
        actual
    );
    Ok(())
}

fn key_matches(record_key: &RDBStr, expected: &str) -> bool {
    match record_key {
        RDBStr::Str(bytes) => bytes.as_ref() == expected.as_bytes(),
        RDBStr::Int(value) => expected == value.to_string(),
    }
}
