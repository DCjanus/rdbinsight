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
pub enum ParseResult<T> {
    Ok(T),
    NeedMore,
    Err(anyhow::Error),
}

impl<T> ParseResult<T> {
    pub fn into_any_result(self) -> crate::helper::AnyResult<T> {
        match self {
            Self::Ok(v) => Ok(v),
            Self::NeedMore => Err(crate::parser::error::NeedMoreData.into()),
            Self::Err(e) => Err(e),
        }
    }
}

pub fn fatal<T>(e: impl Into<anyhow::Error>) -> ParseResult<T> {
    ParseResult::Err(e.into())
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
    fn parse_result_converts_to_any_result() {
        assert_eq!(ParseResult::Ok(42).into_any_result().unwrap(), 42);

        let need_more: ParseResult<()> = ParseResult::NeedMore;
        assert!(
            need_more
                .into_any_result()
                .unwrap_err()
                .is::<NeedMoreData>()
        );

        let fatal: ParseResult<()> = ParseResult::Err(anyhow!("fatal"));
        assert_eq!(fatal.into_any_result().unwrap_err().to_string(), "fatal");
    }
}
