use anyhow::ensure;
use bytes::{Buf, BytesMut};

use crate::helper::AnyResult;

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

    pub fn consume(&mut self, n: usize) {
        assert!(n <= self.buf.len(), "consume length exceeds buffer length");
        self.pos += n as u64;
        self.buf.advance(n);
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
