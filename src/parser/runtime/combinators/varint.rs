use crate::{
    helper::AnyResult,
    parser::core::{
        buffer::Buffer,
        parse::{Parser, ParserInit},
        raw::{RDBLen, read_rdb_len},
    },
};

pub struct RDBLenParser;

impl Parser for RDBLenParser {
    type Output = RDBLen;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        let (input, len) = read_rdb_len(buffer.as_slice())?;
        buffer.consume_to(input.as_ptr());
        Ok(len)
    }
}

impl ParserInit for RDBLenParser {
    fn init<'a>(_: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        Ok((input, Self))
    }
}
