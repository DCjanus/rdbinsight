use super::parse::{ParseResult, ParserInit};
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
            Err(e) => ParseResult::Err(e),
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

#[cfg(test)]
mod tests {
    use anyhow::{anyhow, bail};

    use super::*;
    use crate::parser::core::{
        combinators::{read_exact, read_u8},
        parse::{Parser, ParserInit},
    };

    struct UnitParser;

    impl ParserInit for UnitParser {
        fn init(view: &mut View<'_>) -> ParseResult<Self> {
            view.read(|input| {
                let (input, _) = read_exact(input, 2)?;
                Ok((input, Self))
            })
        }
    }

    impl Parser for UnitParser {
        type Output = ();

        fn call(&mut self, _: &mut Buffer) -> AnyResult<Self::Output> {
            Ok(())
        }
    }

    struct NeedMoreParser;

    impl ParserInit for NeedMoreParser {
        fn init(view: &mut View<'_>) -> ParseResult<Self> {
            view.read(|input| {
                let (input, _) = read_exact(input, 8)?;
                Ok((input, Self))
            })
        }
    }

    impl Parser for NeedMoreParser {
        type Output = ();

        fn call(&mut self, _: &mut Buffer) -> AnyResult<Self::Output> {
            Ok(())
        }
    }

    struct FatalParser;

    impl ParserInit for FatalParser {
        fn init(view: &mut View<'_>) -> ParseResult<Self> {
            view.read(|_| bail!("fatal init"))
        }
    }

    impl Parser for FatalParser {
        type Output = ();

        fn call(&mut self, _: &mut Buffer) -> AnyResult<Self::Output> {
            Ok(())
        }
    }

    #[test]
    fn view_reads_from_offset_without_consuming_buffer() -> AnyResult<()> {
        let mut buffer = Buffer::new(8);
        buffer.extend(&[1, 2, 3, 4])?;

        let mut view = View::new_with_offset(&buffer, 1);
        assert_eq!(view.remaining(), &[2, 3, 4]);
        assert_eq!(view.base_offset(), 0);
        assert_eq!(view.offset(), 1);

        let byte = match view.read(read_u8) {
            ParseResult::Ok(byte) => byte,
            other => panic!("unexpected parse result: {other:?}"),
        };

        assert_eq!(byte, 2);
        assert_eq!(view.consumed(), 2);
        assert_eq!(view.offset(), 2);
        assert_eq!(view.tell_to(view.remaining().as_ptr()), 2);
        assert_eq!(buffer.as_slice(), &[1, 2, 3, 4]);
        Ok(())
    }

    #[test]
    fn view_translates_need_more_and_fatal_errors() -> AnyResult<()> {
        let mut buffer = Buffer::new(8);
        buffer.extend(&[1])?;
        let mut view = View::new_with_offset(&buffer, 0);

        assert!(matches!(
            view.read(|input| {
                let (input, _) = read_exact(input, 2)?;
                Ok((input, ()))
            }),
            ParseResult::NeedMore
        ));
        assert_eq!(view.consumed(), 0);

        assert_eq!(
            view.read::<()>(|_| Err(anyhow!("bad input")))
                .into_any_result()
                .unwrap_err()
                .to_string(),
            "bad input"
        );
        Ok(())
    }

    #[test]
    fn nested_parser_init_rolls_back_on_failure() -> AnyResult<()> {
        let mut buffer = Buffer::new(8);
        buffer.extend(&[1, 2, 3])?;

        let mut view = View::new_with_offset(&buffer, 0);
        assert!(matches!(
            view.init_parser::<UnitParser>(),
            ParseResult::Ok(_)
        ));
        assert_eq!(view.consumed(), 2);

        assert!(matches!(
            view.init_parser::<NeedMoreParser>(),
            ParseResult::NeedMore
        ));
        assert_eq!(view.consumed(), 2);

        assert!(matches!(
            view.init_parser::<FatalParser>(),
            ParseResult::Err(_)
        ));
        assert_eq!(view.consumed(), 2);
        assert_eq!(buffer.as_slice(), &[1, 2, 3]);
        Ok(())
    }
}
