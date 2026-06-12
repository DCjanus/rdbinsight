//! Parsers related to STRING values (type id = 0) inside an RDB file.

use anyhow::Context;

use crate::{
    helper::AnyResult,
    parse_try,
    parser::{
        core::{
            buffer::{Buffer, skip_bytes},
            parse::{ParseResult, Parser, ParserInit},
            raw::{RDBLen, RDBStr, read_rdb_len, read_rdb_str},
            view::View,
        },
        model::{Item, StringEncoding},
    },
};
// --------------------------- StringEncoding ----------------------------

pub struct StringEncodingParser {
    to_skip: u64, // remaining bytes to skip
    encoding: StringEncoding,
}

impl ParserInit for StringEncodingParser {
    fn init(view: &mut View<'_>) -> ParseResult<Self> {
        view.parse_init(|_buffer, input| {
            let (input, str_len) = read_rdb_len(input).context("read string length")?;

            let (input, to_skip, encoding) = match str_len {
                RDBLen::Simple(len) => (input, len, StringEncoding::Raw),
                RDBLen::IntStr(_) => (input, 0, StringEncoding::Int),
                RDBLen::LZFStr => {
                    // LZF header := <compressed len> <uncompressed len>
                    let (input, in_len) = read_rdb_len(input).context("read lzf string length")?;
                    let in_len = in_len.as_u64().context("in_len should be a number")?;
                    let (input, _out_len) =
                        read_rdb_len(input).context("read lzf string length")?;
                    (input, in_len, StringEncoding::LZF)
                }
            };

            Ok((input, Self { to_skip, encoding }))
        })
    }
}

impl Parser for StringEncodingParser {
    type Output = StringEncoding;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        skip_bytes(buffer, &mut self.to_skip)?;
        Ok(self.encoding)
    }
}

pub struct RawStringCountParser {
    remain: u64,
    to_skip: u64,
    counted: u64,
}

impl RawStringCountParser {
    pub fn new(remain: u64) -> Self {
        Self {
            remain,
            to_skip: 0,
            counted: 0,
        }
    }
}

impl Parser for RawStringCountParser {
    type Output = u64;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        while self.remain > 0 {
            if self.to_skip > 0 {
                skip_bytes(buffer, &mut self.to_skip)?;
                self.remain -= 1;
                self.counted += 1;
                continue;
            }

            let (input, str_len) = read_rdb_len(buffer.as_slice()).context("read string length")?;
            let (input, to_skip) = match str_len {
                RDBLen::Simple(len) => (input, len),
                RDBLen::IntStr(_) => (input, 0),
                RDBLen::LZFStr => {
                    let (input, in_len) = read_rdb_len(input).context("read lzf string length")?;
                    let in_len = in_len.as_u64().context("in_len should be a number")?;
                    let (input, _out_len) =
                        read_rdb_len(input).context("read lzf string length")?;
                    (input, in_len)
                }
            };

            buffer.consume_to(input.as_ptr());
            self.to_skip = to_skip;
            if self.to_skip == 0 {
                self.remain -= 1;
                self.counted += 1;
            }
        }

        Ok(self.counted)
    }
}

pub struct StringRecordParser {
    started: u64,
    key: RDBStr,
    entrust: StringEncodingParser,
}

impl ParserInit for StringRecordParser {
    fn init(view: &mut View<'_>) -> ParseResult<Self> {
        let started = view.base_offset();
        let key = parse_try!(view.parse_init(|_, input| {
            let (input, key) = read_rdb_str(input).context("read key")?;
            Ok((input, key))
        }));
        let entrust = parse_try!(view.init_parser::<StringEncodingParser>());
        ParseResult::Ok(Self {
            started,
            key,
            entrust,
        })
    }
}

impl Parser for StringRecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        let encoding = self.entrust.call(buffer)?;
        Ok(Item::StringRecord {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            encoding,
        })
    }
}
