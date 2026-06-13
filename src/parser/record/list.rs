use anyhow::{Context, bail, ensure};

use crate::{
    helper::AnyResult,
    parse_try,
    parser::{
        core::{
            buffer::{Buffer, skip_bytes},
            combinators::{read_be_u32, read_exact, read_u8},
            parse::{ParseResult, Parser, ParserInit},
            raw::{RDBLen, RDBStr, read_rdb_len, read_rdb_str},
            view::View,
        },
        error::NeedMoreData,
        model::{Item, ListEncoding},
        record::{
            set::ListPackLengthParser,
            string::{RawStringCountParser, StringEncodingParser},
        },
        runtime::{combinators::RDBStrBox, lzf::LzfChunkDecoder},
    },
};

pub struct ListRecordParser {
    started: u64,
    key: RDBStr,
    entrust: RawStringCountParser,
}

impl ParserInit for ListRecordParser {
    fn init(view: &mut View<'_>) -> ParseResult<Self> {
        view.parse_init(|buffer, input| {
            let (input, key) = read_rdb_str(input).context("read key")?;
            let (input, member_count) = read_rdb_len(input).context("read list length")?;
            let member_count = member_count
                .as_u64()
                .context("list length should be a number")?;
            let entrust = RawStringCountParser::new(member_count);
            Ok((input, Self {
                started: buffer.tell(),
                key,
                entrust,
            }))
        })
    }
}

impl Parser for ListRecordParser {
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

impl ParserInit for ListZipListRecordParser {
    fn init(view: &mut View<'_>) -> ParseResult<Self> {
        view.parse_init(|buffer, input| {
            let (input, key) = read_rdb_str(input).context("read key")?;
            let (input, entrust) =
                RDBStrBox::init_from_input(buffer, input, ZipListLengthParser::init_from_input)?;
            Ok((input, Self {
                started: buffer.tell(),
                key,
                entrust,
            }))
        })
    }
}

impl Parser for ListZipListRecordParser {
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

impl ZipListLengthParser {
    pub(crate) fn init_from_input(input: &[u8]) -> AnyResult<(&[u8], Self)> {
        let (input, _) = read_exact(input, 10)?;
        Ok((input, Self {
            entrust: None,
            counted: 0,
        }))
    }
}

impl ParserInit for ZipListLengthParser {
    fn init(view: &mut View<'_>) -> ParseResult<Self> {
        view.parse_init(|_buffer, input| Self::init_from_input(input))
    }
}

impl Parser for ZipListLengthParser {
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
            let (input, entrust) = IsEndZipListEntryParser::init(buffer.as_slice())?;
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

impl Parser for IsEndZipListEntryParser {
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

impl ParserInit for ListQuickListRecordParser {
    fn init(view: &mut View<'_>) -> ParseResult<Self> {
        let started = view.base_offset();
        let key = parse_try!(view.parse_init(|_, input| {
            let (input, key) = read_rdb_str(input).context("read key")?;
            Ok((input, key))
        }));
        let entrust = parse_try!(view.init_parser::<QuickListLengthParser>());
        ParseResult::Ok(Self {
            started,
            key,
            entrust,
        })
    }
}

impl Parser for ListQuickListRecordParser {
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
    entrust: Option<QuickListZipListParser>,
}

impl ParserInit for QuickListLengthParser {
    fn init(view: &mut View<'_>) -> ParseResult<Self> {
        view.parse_init(|_buffer, input| {
            let (input, nodes_remain) = read_rdb_len(input)?;
            let nodes_remain = nodes_remain
                .as_u64()
                .context("nodes remain should be a number")?;
            Ok((input, Self {
                nodes_remain,
                count: 0,
                entrust: None,
            }))
        })
    }
}

impl Parser for QuickListLengthParser {
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

            let (input, entrust) = QuickListZipListParser::init(buffer)?;
            buffer.consume_to(input.as_ptr());
            if entrust.is_lzf() {
                crate::parser_trace!("quicklist.ziplist.lzf");
            } else {
                crate::parser_trace!("quicklist.ziplist.raw");
            }

            self.entrust = Some(entrust);
        }
        Ok(self.count)
    }
}

enum QuickListZipListParser {
    Simple {
        expect_end: u64,
        entrust: ZipListLengthParser,
    },
    Lzf(RDBStrBox<ZipListLengthParser>),
}

impl QuickListZipListParser {
    fn init(buffer: &Buffer) -> AnyResult<(&[u8], Self)> {
        let (input, len) = read_rdb_len(buffer.as_slice()).context("read quicklist node length")?;
        match len {
            RDBLen::Simple(length) => {
                let expect_end = buffer.tell_to(input.as_ptr()) + length;
                let (input, entrust) = ZipListLengthParser::init_from_input(input)?;
                Ok((input, Self::Simple {
                    expect_end,
                    entrust,
                }))
            }
            RDBLen::IntStr(_) => bail!("encoded integer cannot wrap quicklist ziplist content"),
            RDBLen::LZFStr => {
                let (input, in_len) =
                    read_rdb_len(input).context("read compressed quicklist node in_len")?;
                let in_len = in_len
                    .as_u64()
                    .context("compressed in_len must be simple")?;
                let (input, out_len) =
                    read_rdb_len(input).context("read compressed quicklist node out_len")?;
                let out_len = out_len
                    .as_u64()
                    .context("compressed out_len must be simple")?;
                Ok((
                    input,
                    Self::Lzf(RDBStrBox::Lzf {
                        remain_in: in_len,
                        remain_out: out_len,
                        out_buffer: Buffer::new(out_len as usize),
                        decoder: LzfChunkDecoder::default(),
                        entrust: None,
                    }),
                ))
            }
        }
    }

    fn is_lzf(&self) -> bool {
        matches!(self, Self::Lzf(_))
    }
}

impl Parser for QuickListZipListParser {
    type Output = u64;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        match self {
            Self::Simple {
                expect_end,
                entrust,
            } => {
                let ret = entrust.call(buffer);
                let e = match ret {
                    Ok(output) => {
                        ensure!(
                            buffer.tell() == *expect_end,
                            "quicklist ziplist offset mismatch: expect: {}, actual: {}",
                            expect_end,
                            buffer.tell()
                        );
                        return Ok(output);
                    }
                    Err(e) => e,
                };

                if buffer.tell() >= *expect_end && e.is::<NeedMoreData>() {
                    bail!(
                        "all quicklist ziplist content should be consumed, parser not finished: {e}"
                    );
                }

                Err(e)
            }
            Self::Lzf(parser) => parser.call(buffer),
        }
    }
}

pub struct ListQuickList2RecordParser {
    started: u64,
    key: RDBStr,
    entrust: QuickList2LengthParser,
}

impl ParserInit for ListQuickList2RecordParser {
    fn init(view: &mut View<'_>) -> ParseResult<Self> {
        view.parse_init(|buffer, input| {
            let started = buffer.tell();
            let (input, key) = read_rdb_str(input)?;
            let (input, node_count) = read_rdb_len(input)?;
            let node_count = node_count
                .as_u64()
                .context("node count should be a number")?;
            let entrust = QuickList2LengthParser::new(node_count);
            Ok((input, Self {
                started,
                key,
                entrust,
            }))
        })
    }
}

impl Parser for ListQuickList2RecordParser {
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

struct QuickList2LengthParser {
    nodes_remain: u64,
    count: u64,
    entrust: Option<ListQuickList2NodeLengthParser>,
}

impl QuickList2LengthParser {
    fn new(nodes_remain: u64) -> Self {
        Self {
            nodes_remain,
            count: 0,
            entrust: None,
        }
    }
}

impl Parser for QuickList2LengthParser {
    type Output = u64;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        loop {
            if let Some(parser) = self.entrust.as_mut() {
                self.count += parser.call(buffer)?;
                self.nodes_remain -= 1;
                self.entrust = None;
            }

            if self.nodes_remain == 0 {
                return Ok(self.count);
            }

            let (input, entrust) = ListQuickList2NodeLengthParser::init(buffer)?;
            buffer.consume_to(input.as_ptr());
            self.entrust = Some(entrust);
        }
    }
}

pub struct ListQuickList2NodeLengthParser {
    entrust: QuickList2NodeParser,
}

enum QuickList2NodeParser {
    Plain(StringEncodingParser),
    Packed(QuickList2ListPackParser),
}

impl ListQuickList2NodeLengthParser {
    fn init(buffer: &Buffer) -> AnyResult<(&[u8], Self)> {
        let (input, flag) = read_u8(buffer.as_slice())?;
        let (input, entrust) = match flag {
            1 => {
                crate::parser_trace!("quicklist2.plain");
                let (input, parser) = StringEncodingParser::init_from_input(input)?;
                (input, QuickList2NodeParser::Plain(parser))
            }
            2 => {
                let (input, entrust) = QuickList2ListPackParser::init(buffer, input)?;
                if entrust.is_lzf() {
                    crate::parser_trace!("quicklist2.packed.lzf");
                } else {
                    crate::parser_trace!("quicklist2.packed.raw");
                }
                (input, QuickList2NodeParser::Packed(entrust))
            }
            _ => bail!("unknown quicklist2 node flag: {:#04x}", flag),
        };
        Ok((input, Self { entrust }))
    }
}

impl Parser for ListQuickList2NodeLengthParser {
    type Output = u64;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        match &mut self.entrust {
            QuickList2NodeParser::Plain(parser) => {
                // plain node always contains only one element
                let _ = parser.call(buffer)?;
                Ok(1)
            }
            QuickList2NodeParser::Packed(parser) => parser.call(buffer),
        }
    }
}

enum QuickList2ListPackParser {
    Simple {
        expect_end: u64,
        entrust: ListPackLengthParser,
    },
    Lzf(RDBStrBox<ListPackLengthParser>),
}

impl QuickList2ListPackParser {
    fn init<'a>(buffer: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let (input, len) = read_rdb_len(input).context("read quicklist2 node length")?;
        match len {
            RDBLen::Simple(length) => {
                let expect_end = buffer.tell_to(input.as_ptr()) + length;
                let (input, entrust) = ListPackLengthParser::init_from_input(input)?;
                Ok((input, Self::Simple {
                    expect_end,
                    entrust,
                }))
            }
            RDBLen::IntStr(_) => bail!("encoded integer cannot wrap quicklist2 listpack content"),
            RDBLen::LZFStr => {
                let (input, in_len) =
                    read_rdb_len(input).context("read compressed quicklist2 node in_len")?;
                let in_len = in_len
                    .as_u64()
                    .context("compressed in_len must be simple")?;
                let (input, out_len) =
                    read_rdb_len(input).context("read compressed quicklist2 node out_len")?;
                let out_len = out_len
                    .as_u64()
                    .context("compressed out_len must be simple")?;
                Ok((
                    input,
                    Self::Lzf(RDBStrBox::Lzf {
                        remain_in: in_len,
                        remain_out: out_len,
                        out_buffer: Buffer::new(out_len as usize),
                        decoder: LzfChunkDecoder::default(),
                        entrust: None,
                    }),
                ))
            }
        }
    }

    fn is_lzf(&self) -> bool {
        matches!(self, Self::Lzf(_))
    }
}

impl Parser for QuickList2ListPackParser {
    type Output = u64;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        match self {
            Self::Simple {
                expect_end,
                entrust,
            } => {
                let ret = entrust.call(buffer);
                let e = match ret {
                    Ok(output) => {
                        ensure!(
                            buffer.tell() == *expect_end,
                            "quicklist2 listpack offset mismatch: expect: {}, actual: {}",
                            expect_end,
                            buffer.tell()
                        );
                        return Ok(output);
                    }
                    Err(e) => e,
                };

                if buffer.tell() >= *expect_end && e.is::<NeedMoreData>() {
                    bail!(
                        "all quicklist2 listpack content should be consumed, parser not finished: {e}"
                    );
                }

                Err(e)
            }
            Self::Lzf(parser) => parser.call(buffer),
        }
    }
}
