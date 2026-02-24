use crate::parser::core::{
    cursor::Cursor,
    parse::{ParseFn, ParseResult, ok},
};

pub fn fold_n<T, A, P, F>(
    cursor: &mut Cursor<'_>,
    mut times: usize,
    mut acc: A,
    mut parser: P,
    mut reduce: F,
) -> ParseResult<A>
where
    P: ParseFn<T>,
    F: FnMut(A, T) -> A,
{
    while times > 0 {
        let item = match parser(cursor) {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return ParseResult::NeedMore,
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        acc = reduce(acc, item);
        times -= 1;
    }
    ok(acc)
}
