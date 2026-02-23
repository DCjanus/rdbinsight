use anyhow::Error;

use crate::parser::core::cursor::Cursor;

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
}

pub fn ok<T>(v: T) -> ParseResult<T> {
    ParseResult::Ok(v)
}

pub fn need_more<T>() -> ParseResult<T> {
    ParseResult::NeedMore
}

pub fn fatal<T>(e: impl Into<Error>) -> ParseResult<T> {
    ParseResult::Err(ParseError::fatal(e))
}

pub fn recoverable<T>(e: impl Into<Error>) -> ParseResult<T> {
    ParseResult::Err(ParseError::recoverable(e))
}

pub trait ParseFn<T> = for<'a> FnMut(&mut Cursor<'a>) -> ParseResult<T>;
