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

#[cfg(test)]
mod tests {
    use super::alt2;
    use crate::parser::{
        combinator::primitive::byte,
        core::{
            buffer::Buffer,
            cursor::Cursor,
            parse::{ParseResult, fatal, recoverable},
        },
    };

    #[test]
    fn alt2_recoverable_falls_back_and_rewinds() {
        let mut buffer = Buffer::new(16);
        buffer.extend(b"ab").unwrap();
        let mut cursor = Cursor::new(&mut buffer);

        let ret = alt2(
            &mut cursor,
            |c| {
                let _ = byte(c);
                recoverable::<u8>(anyhow::anyhow!("recoverable"))
            },
            byte,
        );

        assert!(matches!(ret, ParseResult::Ok(b'a')));
        assert_eq!(cursor.consumed(), 1);
    }

    #[test]
    fn alt2_need_more_keeps_progress() {
        let mut buffer = Buffer::new(16);
        buffer.extend(b"a").unwrap();
        let mut cursor = Cursor::new(&mut buffer);

        let ret = alt2(
            &mut cursor,
            |c| {
                let _ = byte(c);
                ParseResult::<u8>::NeedMore
            },
            byte,
        );

        assert!(matches!(ret, ParseResult::NeedMore));
        assert_eq!(cursor.consumed(), 1);
    }

    #[test]
    fn alt2_fatal_keeps_progress_and_returns_fatal() {
        let mut buffer = Buffer::new(16);
        buffer.extend(b"a").unwrap();
        let mut cursor = Cursor::new(&mut buffer);

        let ret = alt2(
            &mut cursor,
            |c| {
                let _ = byte(c);
                fatal::<u8>(anyhow::anyhow!("fatal"))
            },
            byte,
        );

        assert!(matches!(
            ret,
            ParseResult::Err(crate::parser::core::parse::ParseError::Fatal(_))
        ));
        assert_eq!(cursor.consumed(), 1);
    }
}
