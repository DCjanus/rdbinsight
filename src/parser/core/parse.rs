use anyhow::Error;

use crate::{
    helper::AnyResult,
    parser::core::{buffer::Buffer, view::View},
};

pub trait Parser {
    type Output;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output>;
}

pub trait ParserInit: Parser + Sized {
    fn init(view: &mut View<'_>) -> ParseResult<Self>;
}

#[derive(Debug)]
pub enum ParseError {
    Recoverable(Error),
    Fatal(Error),
}

impl ParseError {
    pub fn recoverable(e: impl Into<Error>) -> Self {
        Self::Recoverable(e.into())
    }

    pub fn fatal(e: impl Into<Error>) -> Self {
        Self::Fatal(e.into())
    }

    pub fn into_error(self) -> Error {
        match self {
            Self::Recoverable(e) | Self::Fatal(e) => e,
        }
    }
}

#[derive(Debug)]
pub enum ParseResult<T> {
    Ok(T),
    NeedMore,
    Err(ParseError),
}

impl<T> ParseResult<T> {
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> ParseResult<U> {
        match self {
            Self::Ok(v) => ParseResult::Ok(f(v)),
            Self::NeedMore => ParseResult::NeedMore,
            Self::Err(e) => ParseResult::Err(e),
        }
    }

    pub fn into_any_result(self) -> crate::helper::AnyResult<T> {
        match self {
            Self::Ok(v) => Ok(v),
            Self::NeedMore => Err(crate::parser::error::NeedMoreData.into()),
            Self::Err(e) => Err(e.into_error()),
        }
    }
}

pub fn fatal<T>(e: impl Into<Error>) -> ParseResult<T> {
    ParseResult::Err(ParseError::fatal(e))
}

#[macro_export]
macro_rules! parse_try {
    ($expr:expr) => {
        match $expr {
            $crate::parser::core::parse::ParseResult::Ok(output) => output,
            $crate::parser::core::parse::ParseResult::NeedMore => {
                return $crate::parser::core::parse::ParseResult::NeedMore;
            }
            $crate::parser::core::parse::ParseResult::Err(err) => {
                return $crate::parser::core::parse::ParseResult::Err(err);
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;

    use super::*;
    use crate::parser::error::NeedMoreData;

    #[test]
    fn parse_result_maps_success_only() {
        assert!(matches!(
            ParseResult::Ok(1).map(|v| v + 1),
            ParseResult::Ok(2)
        ));

        let need_more: ParseResult<i32> = ParseResult::NeedMore;
        assert!(matches!(need_more.map(|v| v + 1), ParseResult::NeedMore));

        let err: ParseResult<i32> = ParseResult::Err(ParseError::fatal(anyhow!("boom")));
        assert!(matches!(err.map(|v| v + 1), ParseResult::Err(_)));
    }

    #[test]
    fn parse_result_converts_to_any_result() {
        assert_eq!(ParseResult::Ok(42).into_any_result().unwrap(), 42);

        let need_more: ParseResult<()> = ParseResult::NeedMore;
        assert!(
            need_more
                .into_any_result()
                .unwrap_err()
                .is::<NeedMoreData>()
        );

        let fatal: ParseResult<()> = ParseResult::Err(ParseError::fatal(anyhow!("fatal")));
        assert_eq!(fatal.into_any_result().unwrap_err().to_string(), "fatal");

        let recoverable: ParseResult<()> =
            ParseResult::Err(ParseError::recoverable(anyhow!("recoverable")));
        assert_eq!(
            recoverable.into_any_result().unwrap_err().to_string(),
            "recoverable"
        );
    }
}
