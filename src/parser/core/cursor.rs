use crate::{
    helper::AnyResult,
    parser::core::{
        buffer::Buffer,
        parse::{ParseResult, ParserInit},
        view::View,
    },
};

pub struct Cursor<'a> {
    buffer: &'a mut Buffer,
}

impl<'a> Cursor<'a> {
    pub fn new(buffer: &'a mut Buffer) -> Self {
        Self { buffer }
    }

    pub fn init_commit<P: ParserInit>(&mut self) -> AnyResult<P> {
        self.init_commit_from_offset::<P>(0).into_any_result()
    }

    pub fn init_commit_parse<P: ParserInit>(&mut self) -> ParseResult<P> {
        self.init_commit_from_offset::<P>(0)
    }

    pub fn init_commit_from_offset<P: ParserInit>(&mut self, offset: usize) -> ParseResult<P> {
        assert!(
            offset <= self.buffer.len(),
            "init offset exceeds buffer length"
        );
        let mut view = View::new_with_offset(self.buffer, offset);
        match P::init(&mut view) {
            ParseResult::Ok(parser) => {
                self.buffer.consume(view.consumed());
                ParseResult::Ok(parser)
            }
            ParseResult::NeedMore => ParseResult::NeedMore,
            ParseResult::Err(e) => ParseResult::Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{
        core::{
            combinators::read_exact,
            parse::{ParseResult, Parser, ParserInit},
        },
        error::NeedMoreData,
    };

    #[derive(Debug)]
    struct TwoByteParser;

    impl ParserInit for TwoByteParser {
        fn init(view: &mut View<'_>) -> ParseResult<Self> {
            view.parse_init(|_, input| {
                let (input, _) = read_exact(input, 2)?;
                Ok((input, Self))
            })
        }
    }

    impl Parser for TwoByteParser {
        type Output = ();

        fn call(&mut self, _: &mut Buffer) -> AnyResult<Self::Output> {
            Ok(())
        }
    }

    #[test]
    fn init_commit_advances_buffer_on_success() -> AnyResult<()> {
        let mut buffer = Buffer::new(8);
        buffer.extend(&[1, 2, 3])?;

        let parser = {
            let mut cursor = Cursor::new(&mut buffer);
            cursor.init_commit::<TwoByteParser>()?
        };

        assert!(matches!(parser, TwoByteParser));
        assert_eq!(buffer.tell(), 2);
        assert_eq!(buffer.as_slice(), &[3]);
        Ok(())
    }

    #[test]
    fn init_commit_leaves_buffer_on_need_more() -> AnyResult<()> {
        let mut buffer = Buffer::new(8);
        buffer.extend(&[1])?;

        let err = {
            let mut cursor = Cursor::new(&mut buffer);
            cursor
                .init_commit::<TwoByteParser>()
                .expect_err("parser should need more data")
        };

        assert!(err.is::<NeedMoreData>());
        assert_eq!(buffer.tell(), 0);
        assert_eq!(buffer.as_slice(), &[1]);
        Ok(())
    }
}
