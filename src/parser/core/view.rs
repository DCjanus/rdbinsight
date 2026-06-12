use super::parse::{ParseError, ParseResult, ParserInit};
use crate::{
    helper::AnyResult,
    parser::{core::buffer::Buffer, error::NeedMoreData},
};

pub struct View<'a> {
    buffer: &'a Buffer,
    consumed: usize,
}

impl<'a> View<'a> {
    pub(crate) fn new_with_offset(buffer: &'a Buffer, offset: usize) -> Self {
        assert!(offset <= buffer.len(), "view offset exceeds buffer length");
        Self {
            buffer,
            consumed: offset,
        }
    }

    pub fn remaining(&self) -> &'a [u8] {
        &self.buffer.as_slice()[self.consumed..]
    }

    pub fn consumed(&self) -> usize {
        self.consumed
    }

    pub fn offset(&self) -> u64 {
        self.buffer.tell() + self.consumed as u64
    }

    pub fn base_offset(&self) -> u64 {
        self.buffer.tell()
    }

    pub fn tell_to(&self, ptr: *const u8) -> u64 {
        self.buffer.tell_to(ptr)
    }

    pub fn parse_init<T>(
        &mut self,
        f: impl for<'b> FnOnce(&'b Buffer, &'b [u8]) -> AnyResult<(&'b [u8], T)>,
    ) -> ParseResult<T> {
        match f(self.buffer, self.remaining()) {
            Ok((remaining, output)) => {
                let consumed = self.buffer.len() - remaining.len();
                debug_assert!(consumed >= self.consumed);
                self.consumed = consumed;
                ParseResult::Ok(output)
            }
            Err(e) if e.is::<NeedMoreData>() => ParseResult::NeedMore,
            Err(e) => ParseResult::Err(ParseError::fatal(e)),
        }
    }

    pub fn read<T>(
        &mut self,
        f: impl for<'b> FnOnce(&'b [u8]) -> AnyResult<(&'b [u8], T)>,
    ) -> ParseResult<T> {
        self.parse_init(|_, input| f(input))
    }

    pub fn init_parser<P: ParserInit>(&mut self) -> ParseResult<P> {
        let consumed = self.consumed;
        match P::init(self) {
            ParseResult::Ok(parser) => ParseResult::Ok(parser),
            ParseResult::NeedMore => {
                self.consumed = consumed;
                ParseResult::NeedMore
            }
            ParseResult::Err(err) => {
                self.consumed = consumed;
                ParseResult::Err(err)
            }
        }
    }
}
