//! Parsers related to STRING values (type id = 0) inside an RDB file.

use anyhow::Context;

use super::{
    buffer::{Buffer, skip_bytes},
    item::{Item, StringEncoding},
    state_parser::StateParser,
};
use crate::{
    helper::AnyResult,
    parser::rdb_parsers::{RDBLen, RDBStr, read_rdb_len, read_rdb_str},
};

// --------------------------- StringEncoding ----------------------------

pub struct StringEncodingParser {
    to_skip: u64, // remaining bytes to skip
    encoding: StringEncoding,
}

impl StringEncodingParser {
    pub fn init(input: &[u8]) -> AnyResult<(&[u8], Self)> {
        let (input, str_len) = read_rdb_len(input).context("read string length")?;

        let (input, to_skip, encoding) = match str_len {
            RDBLen::Simple(len) => (input, len, StringEncoding::Raw),
            RDBLen::IntStr(_) => (input, 0, StringEncoding::Int),
            RDBLen::LZFStr => {
                // LZF header := <compressed len> <uncompressed len>
                let (input, in_len) = read_rdb_len(input).context("read lzf string length")?;
                let in_len = in_len
                    .as_simple()
                    .context("in_len should be a simple number")?;
                let (input, _) = read_rdb_len(input).context("read lzf string length")?;
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

impl StringRecordParser {
    pub fn init(started: u64, input: &[u8]) -> AnyResult<(&[u8], Self)> {
        let (input, key) = read_rdb_str(input).context("read key")?;
        let (input, entrust) = StringEncodingParser::init(input)?;
        Ok((input, Self {
            started,
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
