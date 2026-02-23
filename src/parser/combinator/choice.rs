use crate::parser::core::{
    cursor::Cursor,
    parse::{ParseError, ParseFn, ParseResult},
};

pub fn alt2<A, B, T>(cursor: &mut Cursor<'_>, mut a: A, mut b: B) -> ParseResult<T>
where
    A: ParseFn<T>,
    B: ParseFn<T>,
{
    let checkpoint = cursor.checkpoint();
    match a(cursor) {
        ParseResult::Ok(v) => ParseResult::Ok(v),
        ParseResult::NeedMore => ParseResult::NeedMore,
        ParseResult::Err(ParseError::Recoverable(_)) => {
            cursor.rewind(checkpoint);
            b(cursor)
        }
        ParseResult::Err(e) => ParseResult::Err(e),
    }
}
