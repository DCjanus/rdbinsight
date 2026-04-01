use anyhow::anyhow;

use crate::parser::{
    core::{
        cursor::Cursor,
        parse::{ParseResult, need_more, ok, recoverable},
        raw::{RDBLen, RDBStr},
    },
    error::NeedMoreData,
};

#[derive(Debug, Clone, Copy)]
pub struct Span<'a>(&'a [u8]);

impl<'a> Span<'a> {
    pub fn as_slice(&self) -> &'a [u8] {
        self.0
    }
}

pub fn byte(cursor: &mut Cursor<'_>) -> ParseResult<u8> {
    let found = match cursor.as_slice().first() {
        Some(v) => *v,
        None => {
            return need_more();
        }
    };
    cursor.advance(1);
    ok(found)
}

pub fn exact<'a>(cursor: &mut Cursor<'a>, len: usize) -> ParseResult<Span<'a>> {
    let input = cursor.as_slice();
    if input.len() < len {
        return need_more();
    }
    let ptr = input.as_ptr();
    cursor.advance(len);
    // SAFETY: `ptr` points into `cursor` backing buffer and `len` bytes are
    // guaranteed available. Returned slice is immutable and caller can't mutate
    // buffer through `Span`.
    let found = unsafe { std::slice::from_raw_parts(ptr, len) };
    ok(Span(found))
}

pub fn tag(cursor: &mut Cursor<'_>, expected: &'static [u8]) -> ParseResult<()> {
    let found = match exact(cursor, expected.len()) {
        ParseResult::Ok(v) => v,
        ParseResult::NeedMore => return need_more(),
        ParseResult::Err(e) => return ParseResult::Err(e),
    };
    if found.as_slice() != expected {
        return recoverable(anyhow!("expected tag mismatched"));
    }
    ok(())
}

pub fn read_rdb_len(cursor: &mut Cursor<'_>) -> ParseResult<RDBLen> {
    use crate::parser::core::raw as legacy_raw;

    let input = cursor.as_slice();
    let (remaining, len) = match legacy_raw::read_rdb_len(input) {
        Ok(v) => v,
        Err(e) if e.is::<NeedMoreData>() => return need_more(),
        Err(e) => return recoverable(e),
    };
    let consumed = input.len() - remaining.len();
    cursor.advance(consumed);
    ok(len)
}

pub fn read_rdb_str(cursor: &mut Cursor<'_>) -> ParseResult<RDBStr> {
    use crate::parser::core::raw as legacy_raw;

    let input = cursor.as_slice();
    let (remaining, value) = match legacy_raw::read_rdb_str(input) {
        Ok(v) => v,
        Err(e) if e.is::<NeedMoreData>() => return need_more(),
        Err(e) => return recoverable(e),
    };
    let consumed = input.len() - remaining.len();
    cursor.advance(consumed);
    ok(value)
}

pub fn le_u32(cursor: &mut Cursor<'_>) -> ParseResult<u32> {
    let bytes = match exact(cursor, 4) {
        ParseResult::Ok(v) => v,
        ParseResult::NeedMore => return need_more(),
        ParseResult::Err(e) => return ParseResult::Err(e),
    };
    let value = match <[u8; 4]>::try_from(bytes.as_slice()) {
        Ok(v) => u32::from_le_bytes(v),
        Err(_) => return recoverable(anyhow!("u32 parse")),
    };
    ok(value)
}

pub fn le_u64(cursor: &mut Cursor<'_>) -> ParseResult<u64> {
    let bytes = match exact(cursor, 8) {
        ParseResult::Ok(v) => v,
        ParseResult::NeedMore => return need_more(),
        ParseResult::Err(e) => return ParseResult::Err(e),
    };
    let value = match <[u8; 8]>::try_from(bytes.as_slice()) {
        Ok(v) => u64::from_le_bytes(v),
        Err(_) => return recoverable(anyhow!("u64 parse")),
    };
    ok(value)
}
