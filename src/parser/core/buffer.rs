use anyhow::ensure;
use bytes::{Buf, BytesMut};

use crate::{
    helper::{AnyResult, wrapping_to_usize},
    parser::{core::combinators::read_at_most_but_at_least_one, error::NotFinished},
};

#[derive(Debug)]
pub struct Buffer {
    buf: BytesMut,
    max: usize,
    pos: u64,
}

impl Buffer {
    pub fn new(max: usize) -> Self {
        Self {
            buf: BytesMut::new(),
            max,
            pos: 0,
        }
    }

    pub fn extend(&mut self, data: &[u8]) -> AnyResult {
        ensure!(self.buf.len() + data.len() <= self.max, "buffer overflow");
        self.buf.extend_from_slice(data);
        Ok(())
    }

    pub fn consume_to(&mut self, ptr: *const u8) {
        let buf_range = self.buf.as_ptr_range();
        assert!(
            buf_range.start <= ptr && ptr <= buf_range.end,
            "pointer out of buffer range",
        );

        // Safety: ptr is inside the current buffer; offset is therefore non-negative.
        let delta = ptr as usize - buf_range.start as usize;

        self.pos += delta as u64;
        self.buf.advance(delta);
        debug_assert_eq!(self.buf.as_ptr(), ptr);
    }

    pub fn tell(&self) -> u64 {
        self.pos
    }

    pub fn tell_to(&self, ptr: *const u8) -> u64 {
        let buf_range = self.buf.as_ptr_range();
        assert!(
            buf_range.start <= ptr && ptr <= buf_range.end,
            "pointer out of buffer range",
        );

        // Safety: ptr is inside the current buffer; offset is therefore non-negative.
        let delta = ptr as u64 - buf_range.start as u64;
        self.pos + delta
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }
}

impl AsRef<[u8]> for Buffer {
    fn as_ref(&self) -> &[u8] {
        self.buf.as_ref()
    }
}

// Skip `remain` bytes from the buffer, returning `NotFinished` when more data is required.
pub(crate) fn skip_bytes(buffer: &mut Buffer, remain: &mut u64) -> AnyResult<()> {
    if *remain == 0 {
        return Ok(());
    }

    let input = buffer.as_ref();
    let (input, _skipped) = read_at_most_but_at_least_one(input, wrapping_to_usize(*remain))?;
    *remain -= _skipped.len() as u64;
    buffer.consume_to(input.as_ptr());

    if *remain > 0 {
        return Err(NotFinished.into());
    }
    Ok(())
}
