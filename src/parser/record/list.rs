use anyhow::Context;
use futures_util::future::Either;

use crate::{
    helper::AnyResult,
    parser::{
        core::{
            buffer::{Buffer, skip_bytes},
            combinators::{read_be_u32, read_exact, read_u8},
            raw::{RDBStr, read_rdb_len, read_rdb_str},
        },
        model::{Item, ListEncoding, StringEncoding},
        record::{set::ListPackLengthParser, string::StringEncodingParser},
        state::{
            combinators::{RDBStrBox, ReduceParser},
            traits::{InitializableParser, StateParser},
        },
    },
};

pub struct ListRecordParser {
    started: u64,
    key: RDBStr,
    entrust: ReduceParser<StringEncodingParser, u64, fn(u64, StringEncoding) -> u64>,
}

impl ListRecordParser {
    pub fn init(started: u64, input: &[u8]) -> AnyResult<(&[u8], Self)> {
        let (input, key) = read_rdb_str(input).context("read key")?;
        let (input, member_count) = read_rdb_len(input).context("read list length")?;
        let member_count = member_count
            .as_u64()
            .context("list length should be a number")?;
        let entrust: ReduceParser<StringEncodingParser, u64, fn(u64, StringEncoding) -> u64> =
            ReduceParser::new(member_count, 0, |acc, _item: StringEncoding| acc + 1);
        Ok((input, Self {
            started,
            key,
            entrust,
        }))
    }
}

impl StateParser for ListRecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        let member_count = self.entrust.call(buffer)?;
        Ok(Item::ListRecord {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            encoding: ListEncoding::List,
            member_count,
        })
    }
}

pub struct ListZipListRecordParser {
    started: u64,
    key: RDBStr,
    entrust: RDBStrBox<ZipListLengthParser>,
}

impl InitializableParser for ListZipListRecordParser {
    fn init<'a>(buf: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let (input, key) = read_rdb_str(input).context("read key")?;
        let (input, entrust) = RDBStrBox::<ZipListLengthParser>::init(buf, input)?;
        Ok((input, Self {
            started: buf.tell(),
            key,
            entrust,
        }))
    }
}

impl StateParser for ListZipListRecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        let member_count = self.entrust.call(buffer)?;
        Ok(Item::ListRecord {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            encoding: ListEncoding::ZipList,
            member_count,
        })
    }
}

pub struct ZipListLengthParser {
    entrust: Option<IsEndZipListEntryParser>,
    counted: u64,
}

impl InitializableParser for ZipListLengthParser {
    fn init<'a>(_: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let (input, _) = read_exact(input, 10)?;
        Ok((input, Self {
            entrust: None,
            counted: 0,
        }))
    }
}

impl StateParser for ZipListLengthParser {
    type Output = u64;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        loop {
            if let Some(entrust) = self.entrust.as_mut() {
                if entrust.call(buffer)? {
                    return Ok(self.counted);
                }
                self.counted += 1;
                self.entrust = None;
            }
            let (input, entrust) = IsEndZipListEntryParser::init(buffer.as_ref())?;
            buffer.consume_to(input.as_ptr());
            self.entrust = Some(entrust);
        }
    }
}

pub struct IsEndZipListEntryParser {
    to_skip: u64,
    is_enl: bool,
}

impl IsEndZipListEntryParser {
    fn init(input: &[u8]) -> AnyResult<(&[u8], Self)> {
        let (mut input, prevlen_flag) = read_u8(input).context("read prevlen")?;
        if prevlen_flag == 0xFF {
            return Ok((input, Self {
                to_skip: 0,
                is_enl: true,
            }));
        }
        if prevlen_flag == 0xFE {
            let (rest, _) = read_exact(input, 4).context("skip prevlen4")?;
            input = rest;
        }

        let (input, to_skip) = Self::read_to_skip(input)?;

        Ok((input, Self {
            to_skip,
            is_enl: false,
        }))
    }

    fn read_to_skip(input: &[u8]) -> AnyResult<(&[u8], u64)> {
        let (input, encoding_byte) = read_u8(input).context("read encoding byte")?;
        let encoding_type = encoding_byte >> 6;
        match encoding_type {
            0 => {
                let content_len = encoding_byte & 0b0011_1111;
                Ok((input, content_len as u64))
            }
            1 => {
                let (input, low_byte) = read_u8(input).context("read second byte")?;
                let high_byte = encoding_byte & 0b0011_1111;
                let content_len = u64::from_be_bytes([0, 0, 0, 0, 0, 0, high_byte, low_byte]);
                Ok((input, content_len))
            }
            2 => {
                let (input, content_len) = read_be_u32(input)?;
                Ok((input, content_len as u64))
            }
            3 => {
                let to_skip = match encoding_byte {
                    0b1100_0000 => 2,
                    0b1101_0000 => 4,
                    0b1110_0000 => 8,
                    0b1111_0000 => 3,
                    0b1111_1110 => 1,
                    _ => 0,
                };
                Ok((input, to_skip))
            }
            _ => unreachable!("invalid encoding type: {:#04x}", encoding_byte),
        }
    }
}

impl StateParser for IsEndZipListEntryParser {
    type Output = bool;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        skip_bytes(buffer, &mut self.to_skip)?;
        Ok(self.is_enl)
    }
}

pub struct ListQuickListRecordParser {
    started: u64,
    key: RDBStr,
    entrust: QuickListLengthParser,
}

impl InitializableParser for ListQuickListRecordParser {
    fn init<'a>(buffer: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let (input, key) = read_rdb_str(input).context("read key")?;
        let (input, entrust) = QuickListLengthParser::init(buffer, input)?;
        Ok((input, Self {
            started: buffer.tell(),
            key,
            entrust,
        }))
    }
}

impl StateParser for ListQuickListRecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        let member_count = self.entrust.call(buffer)?;
        Ok(Item::ListRecord {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            encoding: ListEncoding::QuickList,
            member_count,
        })
    }
}

struct QuickListLengthParser {
    nodes_remain: u64,
    count: u64,
    entrust: Option<RDBStrBox<ZipListLengthParser>>,
}

impl InitializableParser for QuickListLengthParser {
    fn init<'a>(_: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let (input, nodes_remain) = read_rdb_len(input)?;
        let nodes_remain = nodes_remain
            .as_u64()
            .context("nodes remain should be a number")?;
        Ok((input, Self {
            nodes_remain,
            count: 0,
            entrust: None,
        }))
    }
}

impl StateParser for QuickListLengthParser {
    type Output = u64;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        loop {
            if let Some(parser) = self.entrust.as_mut() {
                self.count += parser.call(buffer)?;
                self.nodes_remain -= 1;
                self.entrust = None;
            }

            if self.nodes_remain == 0 {
                break;
            }

            let (input, entrust) = RDBStrBox::<ZipListLengthParser>::init(buffer, buffer.as_ref())?;
            if entrust.is_lzf() {
                crate::parser_trace!("quicklist.ziplist.lzf");
            } else {
                crate::parser_trace!("quicklist.ziplist.raw");
            }

            buffer.consume_to(input.as_ptr());
            self.entrust = Some(entrust);
        }
        Ok(self.count)
    }
}

pub struct ListQuickList2RecordParser {
    started: u64,
    key: RDBStr,
    entrust: ReduceParser<ListQuickList2NodeLengthParser, u64>,
}

impl InitializableParser for ListQuickList2RecordParser {
    fn init<'a>(buffer: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let started = buffer.tell();
        let (input, key) = read_rdb_str(input)?;
        let (input, node_count) = read_rdb_len(input)?;
        let node_count = node_count
            .as_u64()
            .context("node count should be a number")?;
        let entrust = ReduceParser::<ListQuickList2NodeLengthParser, _>::new(
            node_count,
            0,
            |acc, item: u64| acc + item,
        );
        Ok((input, Self {
            started,
            key,
            entrust,
        }))
    }
}

impl StateParser for ListQuickList2RecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        let member_count = self.entrust.call(buffer)?;
        Ok(Item::ListRecord {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            encoding: ListEncoding::QuickList2,
            member_count,
        })
    }
}

pub struct ListQuickList2NodeLengthParser {
    entrust: Either<StringEncodingParser, RDBStrBox<ListPackLengthParser>>,
}

impl InitializableParser for ListQuickList2NodeLengthParser {
    fn init<'a>(buffer: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let (input, flag) = read_u8(input)?;
        match flag {
            1 => {
                crate::parser_trace!("quicklist2.plain");
                let (input, entrust) = StringEncodingParser::init(buffer, input)?;
                Ok((input, Self {
                    entrust: Either::Left(entrust),
                }))
            }
            2 => {
                let (input, entrust) = RDBStrBox::<ListPackLengthParser>::init(buffer, input)?;
                if entrust.is_lzf() {
                    crate::parser_trace!("quicklist2.packed.lzf");
                } else {
                    crate::parser_trace!("quicklist2.packed.raw");
                }
                Ok((input, Self {
                    entrust: Either::Right(entrust),
                }))
            }
            _ => anyhow::bail!("unknown quicklist2 node flag: {:#04x}", flag),
        }
    }
}

impl StateParser for ListQuickList2NodeLengthParser {
    type Output = u64;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        match &mut self.entrust {
            Either::Left(parser) => {
                // plain node always contains only one element
                let _ = parser.call(buffer)?;
                Ok(1)
            }
            Either::Right(parser) => parser.call(buffer),
        }
    }
}
