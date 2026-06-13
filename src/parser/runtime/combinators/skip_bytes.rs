use crate::parser::core::{
    buffer::{Buffer, skip_bytes},
    parse::{ParseResult, Parser, ParserInit},
    view::View,
};

pub struct SkipBytesParser<const N: usize> {
    remain: u64,
}

impl<const N: usize> ParserInit for SkipBytesParser<N> {
    fn init(_: &mut View<'_>) -> ParseResult<Self> {
        ParseResult::Ok(Self { remain: N as u64 })
    }
}

impl<const N: usize> Parser for SkipBytesParser<N> {
    type Output = ();

    fn call(&mut self, buffer: &mut Buffer) -> crate::helper::AnyResult<Self::Output> {
        skip_bytes(buffer, &mut self.remain)?;
        Ok(())
    }
}
