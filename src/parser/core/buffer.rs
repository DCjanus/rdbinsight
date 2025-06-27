use anyhow::ensure;
use bytes::{Buf, BytesMut};

use crate::{
    helper::{AnyResult, wrapping_to_usize},
    parser::{core::combinators::read_at_most_but_at_least_one, error::NeedMoreData},
};

#[derive(Debug)]
pub struct Buffer {
    buf: BytesMut,
    max: usize,
    pos: u64,
    finished: bool,
}

impl Buffer {
    pub fn new(max: usize) -> Self {
        Self {
            buf: BytesMut::new(),
            max,
            pos: 0,
            finished: false,
        }
    }

    pub fn extend(&mut self, data: &[u8]) -> AnyResult {
        ensure!(self.remain_capacity() >= data.len(), "buffer overflow");
        self.buf.extend_from_slice(data);
        Ok(())
    }

    pub fn push_u8(&mut self, byte: u8) -> AnyResult {
        self.extend(&[byte])?;
        Ok(())
    }

    pub fn consume_to(&mut self, ptr: *const u8) {
        let delta = self.distance_to(ptr);
        self.pos += delta;
        self.buf.advance(delta as usize);
        debug_assert_eq!(self.buf.as_ptr(), ptr);
    }

    pub fn tell(&self) -> u64 {
        self.pos
    }

    pub fn tell_to(&self, ptr: *const u8) -> u64 {
        self.pos + self.distance_to(ptr)
    }

    pub fn distance_to(&self, ptr: *const u8) -> u64 {
        let buf_range = self.buf.as_ptr_range();
        assert!(
            buf_range.start <= ptr && ptr <= buf_range.end,
            "pointer out of buffer range",
        );
        ptr as u64 - buf_range.start as u64
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn remain_capacity(&self) -> usize {
        self.max - self.buf.len()
    }

    pub fn is_finished(&self) -> bool {
        self.finished
    }

    pub fn set_finished(&mut self) {
        self.finished = true;
    }

    pub fn as_slice(&self) -> &[u8] {
        self.buf.as_ref()
    }

    pub fn truncate(&mut self, len: usize) {
        self.buf.truncate(len);
    }
}

// Skip `remain` bytes from the buffer, returning `NotFinished` when more data is required.
pub(crate) fn skip_bytes(buffer: &mut Buffer, remain: &mut u64) -> AnyResult<()> {
    if *remain == 0 {
        return Ok(());
    }

    let input = buffer.as_slice();
    let (input, _skipped) = read_at_most_but_at_least_one(input, wrapping_to_usize(*remain))?;
    *remain -= _skipped.len() as u64;
    buffer.consume_to(input.as_ptr());

    if *remain > 0 {
        return Err(NeedMoreData.into());
    }
    Ok(())
}
