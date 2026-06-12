use anyhow::{Context, ensure};

use crate::{
    helper::AnyResult,
    parse_try,
    parser::{
        StringEncoding,
        core::{
            buffer::Buffer,
            parse::{ParseResult, Parser, ParserInit},
            raw::{RDBStr, read_rdb_len, read_rdb_str},
            view::View,
        },
        model::{Item, ZSetEncoding},
        record::{
            list::ZipListLengthParser, set::ListPackLengthParser, string::StringEncodingParser,
        },
        runtime::combinators::{RDBStrBox, ReduceParser, Seq2Parser, skip_bytes::SkipBytesParser},
    },
};

pub struct ZSetRecordParser {
    started: u64,
    key: RDBStr,
    entrust: ReduceParser<StringEncodingParser, u64>,
}

impl ParserInit for ZSetRecordParser {
    fn init(view: &mut View<'_>) -> ParseResult<Self> {
        view.parse_init(|buffer, input| {
            let (input, key) = read_rdb_str(input).context("read key")?;
            let (input, member_count) = read_rdb_len(input).context("read zset length")?;
            let member_count = member_count
                .as_u64()
                .context("zset length should be a number")?;
            let entrust: ReduceParser<StringEncodingParser, u64> =
                ReduceParser::new(member_count * 2, 0, |acc, _: StringEncoding| acc + 1);

            crate::parser_trace!("zset.skiplist");

            Ok((input, Self {
                started: buffer.tell(),
                key,
                entrust,
            }))
        })
    }
}

impl Parser for ZSetRecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        let entry_count = self.entrust.call(buffer)?;

        Ok(Item::ZSetRecord {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            encoding: ZSetEncoding::SkipList,
            member_count: entry_count / 2,
        })
    }
}

pub struct ZSetZipListRecordParser {
    started: u64,
    key: RDBStr,
    entrust: RDBStrBox<ZipListLengthParser>,
}

impl ParserInit for ZSetZipListRecordParser {
    fn init(view: &mut View<'_>) -> ParseResult<Self> {
        let started = view.base_offset();
        let key = parse_try!(view.parse_init(|_, input| {
            let (input, key) = read_rdb_str(input).context("read key")?;
            Ok((input, key))
        }));
        let entrust = parse_try!(view.init_parser::<RDBStrBox<ZipListLengthParser>>());
        ParseResult::Ok(Self {
            started,
            key,
            entrust,
        })
    }
}

impl Parser for ZSetZipListRecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        let entry_count = self.entrust.call(buffer)?;
        ensure!(
            entry_count.is_multiple_of(2),
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

pub struct ZSetListPackRecordParser {
    started: u64,
    key: RDBStr,
    entrust: RDBStrBox<ListPackLengthParser>,
}

impl ParserInit for ZSetListPackRecordParser {
    fn init(view: &mut View<'_>) -> ParseResult<Self> {
        let started = view.base_offset();
        let key = parse_try!(view.parse_init(|_, input| {
            let (input, key) = read_rdb_str(input).context("read key")?;
            Ok((input, key))
        }));
        let entrust = parse_try!(view.init_parser::<RDBStrBox<ListPackLengthParser>>());
        if entrust.is_lzf() {
            crate::parser_trace!("zset.listpack.lzf");
        } else {
            crate::parser_trace!("zset.listpack.raw");
        }
        ParseResult::Ok(Self {
            started,
            key,
            entrust,
        })
    }
}

impl Parser for ZSetListPackRecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        // Determine member count.
        let member_count = self.entrust.call(buffer)?;
        ensure!(
            member_count.is_multiple_of(2),
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

pub struct ZSet2RecordParser {
    started: u64,
    key: RDBStr,
    entrust: ReduceParser<ZSet2SkipListEntryParser, u64>,
}

impl ParserInit for ZSet2RecordParser {
    fn init(view: &mut View<'_>) -> ParseResult<Self> {
        view.parse_init(|buffer, input| {
            let (input, key) = read_rdb_str(input).context("read key")?;
            let (input, member_count) = read_rdb_len(input).context("read zset2 length")?;
            let member_count = member_count
                .as_u64()
                .context("zset2 length should be a number")?;

            crate::parser_trace!("zset2.skiplist");

            let entrust: ReduceParser<ZSet2SkipListEntryParser, u64> =
                ReduceParser::new(member_count, 0, |acc, _: _| acc + 1);

            Ok((input, Self {
                started: buffer.tell(),
                key,
                entrust,
            }))
        })
    }
}

impl Parser for ZSet2RecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        let member_count = self.entrust.call(buffer)?;

        Ok(Item::ZSet2Record {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            encoding: ZSetEncoding::SkipList,
            member_count,
        })
    }
}

type ZSet2SkipListEntryParser = Seq2Parser<StringEncodingParser, SkipBytesParser<8>>;
