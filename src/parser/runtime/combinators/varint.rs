use crate::parser::core::{
    buffer::Buffer,
    parse::{ParseResult, Parser, ParserInit},
    raw::{RDBLen, read_rdb_len},
    view::View,
};

pub struct RDBLenParser;

impl Parser for RDBLenParser {
    type Output = RDBLen;

    fn call(&mut self, buffer: &mut Buffer) -> crate::helper::AnyResult<Self::Output> {
        let (input, len) = read_rdb_len(buffer.as_slice())?;
        buffer.consume_to(input.as_ptr());
        Ok(len)
    }
}

impl ParserInit for RDBLenParser {
    fn init(_: &mut View<'_>) -> ParseResult<Self> {
        ParseResult::Ok(Self)
    }
}
