use anyhow::Context;

use super::{
    buffer::Buffer,
    item::{Item, ZSetEncoding},
    record_string::StringEncodingParser,
    state_parser::StateParser,
};
use crate::{
    helper::AnyResult,
    parser::rdb_parsers::{RDBStr, read_rdb_len, read_rdb_str},
};

pub struct ZSet2RecordParser {
    started: u64,
    key: RDBStr,
    member_count: u64,
    remain: u64,
    entrust: Option<ZSet2SkipListEntryParser>,
}

impl ZSet2RecordParser {
    pub fn init(started: u64, input: &[u8]) -> AnyResult<(&[u8], Self)> {
        let (input, key) = read_rdb_str(input).context("read key")?;
        let (input, member_count) = read_rdb_len(input).context("read zset2 length")?;
        let member_count = member_count
            .as_simple()
            .context("zset2 length should be a simple number")?;

        // Trace point.
        crate::parser_trace!("zset2.skiplist");

        Ok((input, Self {
            started,
            key,
            member_count,
            remain: member_count,
            entrust: None,
        }))
    }
}

impl StateParser for ZSet2RecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        loop {
            // Give the delegated sub-parser a chance to finish.
            if let Some(parser) = self.entrust.as_mut() {
                parser.call(buffer)?;
                self.entrust = None;
                self.remain -= 1;
            }

            // All members processed – emit record.
            if self.remain == 0 {
                break;
            }

            // Spawn a new entry parser for the next member/score pair.
            let (input, entrust) = ZSet2SkipListEntryParser::init(buffer.as_ref())?;
            buffer.consume_to(input.as_ptr());
            self.entrust = Some(entrust);
        }

        Ok(Item::ZSet2Record {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            encoding: ZSetEncoding::SkipList,
            member_count: self.member_count,
        })
    }
}

enum ZSet2SkipListEntryParser {
    Member(StringEncodingParser),
    Score { to_skip: u64 },
}

impl ZSet2SkipListEntryParser {
    fn init(input: &[u8]) -> AnyResult<(&[u8], Self)> {
        let (input, member_parser) = StringEncodingParser::init(input)?;
        Ok((input, Self::Member(member_parser)))
    }
}

impl StateParser for ZSet2SkipListEntryParser {
    type Output = ();

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        loop {
            match self {
                Self::Member(parser) => {
                    let _ = parser.call(buffer)?;
                    *self = Self::Score { to_skip: 8 };
                }
                Self::Score { to_skip } => {
                    super::buffer::skip_bytes(buffer, to_skip)?;
                    return Ok(());
                }
            }
        }
    }
}
