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
