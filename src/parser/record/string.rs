//! Parsers related to STRING values (type id = 0) inside an RDB file.

use anyhow::Context;

use crate::{
    helper::AnyResult,
    parser::{
        core::{
            buffer::{Buffer, skip_bytes},
            raw::{RDBLen, RDBStr, read_rdb_len, read_rdb_str},
        },
        model::{Item, StringEncoding},
        state::traits::{InitializableParser, StateParser},
    },
};
// --------------------------- StringEncoding ----------------------------

pub struct StringEncodingParser {
    to_skip: u64, // remaining bytes to skip
    encoding: StringEncoding,
}

impl InitializableParser for StringEncodingParser {
    fn init<'a>(_: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let (input, str_len) = read_rdb_len(input).context("read string length")?;

        let (input, to_skip, encoding) = match str_len {
            RDBLen::Simple(len) => (input, len, StringEncoding::Raw),
            RDBLen::IntStr(_) => (input, 0, StringEncoding::Int),
            RDBLen::LZFStr => {
                // LZF header := <compressed len> <uncompressed len>
                let (input, in_len) = read_rdb_len(input).context("read lzf string length")?;
                let in_len = in_len.as_u64().context("in_len should be a number")?;
                let (input, _out_len) = read_rdb_len(input).context("read lzf string length")?;
                (input, in_len, StringEncoding::LZF)
            }
        };

        Ok((input, Self { to_skip, encoding }))
    }
}

impl StateParser for StringEncodingParser {
    type Output = StringEncoding;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        skip_bytes(buffer, &mut self.to_skip)?;
        Ok(self.encoding)
    }
}

// ----------------------------- StringRecord -----------------------------

pub struct StringRecordParser {
    started: u64,
    key: RDBStr,
    entrust: StringEncodingParser,
}

impl InitializableParser for StringRecordParser {
    fn init<'a>(buf: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let (input, key) = read_rdb_str(input).context("read key")?;
        let (input, entrust) = StringEncodingParser::init(buf, input)?;
        Ok((input, Self {
            started: buf.tell(),
            key,
            entrust,
        }))
    }
}

impl StateParser for StringRecordParser {
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
