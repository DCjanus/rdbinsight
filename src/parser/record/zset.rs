use anyhow::{Context, ensure};

use crate::{
    helper::AnyResult,
    parser::{
        core::{
            buffer::Buffer,
            raw::{RDBStr, read_rdb_len, read_rdb_str},
        },
        model::{Item, ZSetEncoding},
        record::{
            list::ZipListLengthParser, set::ListPackLengthParser, string::StringEncodingParser,
        },
        state::{
            combinators::RDBStrBox,
            traits::{InitializableParser, StateParser},
        },
    },
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
            .as_u64()
            .context("zset length should be a number")?;

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
    entrust: RDBStrBox<ZipListLengthParser>,
}

impl ZSetZipListRecordParser {
    pub fn init<'a>(buffer: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let (input, key) = read_rdb_str(input).context("read key")?;
        let (input, entrust) = RDBStrBox::<ZipListLengthParser>::init(buffer, input)?;
        Ok((input, Self {
            started: buffer.tell(),
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
            let (input, entrust) = StringEncodingParser::init(buffer, input)?;
            buffer.consume_to(input.as_ptr());
            self.entrust = Some(entrust);
        }
    }
}

pub struct ZSetListPackRecordParser {
    started: u64,
    key: RDBStr,
    entrust: RDBStrBox<ListPackLengthParser>,
}

impl ZSetListPackRecordParser {
    pub fn init<'a>(buffer: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let (input, key) = read_rdb_str(input).context("read key")?;
        let (input, entrust) = RDBStrBox::<ListPackLengthParser>::init(buffer, input)?;
        if entrust.is_lzf() {
            crate::parser_trace!("zset.listpack.lzf");
        } else {
            crate::parser_trace!("zset.listpack.raw");
        }

        Ok((input, Self {
            started: buffer.tell(),
            key,
            entrust,
        }))
    }
}

impl StateParser for ZSetListPackRecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        // Determine member count.
        let member_count = self.entrust.call(buffer)?;
        ensure!(
            member_count % 2 == 0,
            "zset listpack entry count should be even"
        );
        let member_count = member_count / 2;

        Ok(Item::ZSetRecord {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            encoding: ZSetEncoding::ListPack,
            member_count,
        })
    }
}

// ZSet2 (double score) parsers

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
            .as_u64()
            .context("zset2 length should be a number")?;

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
            let (input, entrust) = ZSet2SkipListEntryParser::init(buffer, buffer.as_ref())?;
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

impl InitializableParser for ZSet2SkipListEntryParser {
    fn init<'a>(buf: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let (input, member_parser) = StringEncodingParser::init(buf, input)?;
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
                    crate::parser::core::buffer::skip_bytes(buffer, to_skip)?;
                    return Ok(());
                }
            }
        }
    }
}
