use anyhow::{Context, ensure};

use super::{
    buffer::Buffer,
    item::{Item, ZSetEncoding},
    record_list::ZipListLengthParser,
    record_string::StringEncodingParser,
    state_parser::StateParser,
};
use crate::{
    helper::AnyResult,
    parser::rdb_parsers::{RDBStr, read_rdb_len, read_rdb_str},
};

pub struct ZSetRecordParser {
    started: u64,
    key: RDBStr,
    member_count: u64,
    remain: u64,
    entrust: Option<ZSetSkipListEntryParser>,
}

impl ZSetRecordParser {
    pub fn init(started: u64, input: &[u8]) -> AnyResult<(&[u8], Self)> {
        let (input, key) = read_rdb_str(input).context("read key")?;
        let (input, member_count) = read_rdb_len(input).context("read zset length")?;
        let member_count = member_count
            .as_simple()
            .context("zset length should be a simple number")?;

        // Trace point.
        crate::parser_trace!("zset.skiplist");

        Ok((input, Self {
            started,
            key,
            member_count,
            remain: member_count,
            entrust: None,
        }))
    }
}

impl StateParser for ZSetRecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        loop {
            if let Some(parser) = self.entrust.as_mut() {
                parser.call(buffer)?;
                self.entrust = None;
                self.remain -= 1;
            }

            if self.remain == 0 {
                break;
            }

            let (input, entrust) = ZSetSkipListEntryParser::init(buffer.as_ref())?;
            buffer.consume_to(input.as_ptr());
            self.entrust = Some(entrust);
        }

        Ok(Item::ZSetRecord {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            encoding: ZSetEncoding::SkipList,
            member_count: self.member_count,
        })
    }
}

pub struct ZSetZipListRecordParser {
    started: u64,
    key: RDBStr,
    entrust: ZipListLengthParser,
}

impl ZSetZipListRecordParser {
    pub fn init(started: u64, input: &[u8]) -> AnyResult<(&[u8], Self)> {
        let (input, key) = read_rdb_str(input).context("read key")?;
        let (input, entrust) = ZipListLengthParser::init(input).context("init ziplist parser")?;
        Ok((input, Self {
            started,
            key,
            entrust,
        }))
    }
}

impl StateParser for ZSetZipListRecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        let entry_count = self.entrust.call(buffer)?;
        ensure!(
            entry_count % 2 == 0,
            "zset ziplist entry count should be even"
        );
        let member_count = entry_count / 2;

        Ok(Item::ZSetRecord {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            encoding: ZSetEncoding::ZipList,
            member_count,
        })
    }
}

struct ZSetSkipListEntryParser {
    remain: u8, // 2 components: member and score
    entrust: Option<StringEncodingParser>,
}

impl ZSetSkipListEntryParser {
    fn init(input: &[u8]) -> AnyResult<(&[u8], Self)> {
        Ok((input, Self {
            remain: 2,
            entrust: None,
        }))
    }
}

impl StateParser for ZSetSkipListEntryParser {
    type Output = ();

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        loop {
            if let Some(entrust) = self.entrust.as_mut() {
                let _ = entrust.call(buffer)?;
                self.remain -= 1;
                self.entrust = None;
            }
            if self.remain == 0 {
                return Ok(());
            }
            let input = buffer.as_ref();
            let (input, entrust) = StringEncodingParser::init(input)?;
            buffer.consume_to(input.as_ptr());
            self.entrust = Some(entrust);
        }
    }
}
