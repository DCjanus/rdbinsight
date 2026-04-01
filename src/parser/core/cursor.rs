use crate::parser::core::buffer::Buffer;

#[derive(Debug, Clone, Copy)]
pub struct Checkpoint {
    consumed: usize,
}

/// Read-only view on top of [`Buffer`] that supports transactional reads.
///
/// Parser branches can checkpoint/rewind without touching the underlying
/// buffer, and commit once a branch is selected.
#[derive(Debug)]
pub struct Cursor<'a> {
    buffer: &'a mut Buffer,
    consumed: usize,
}

impl<'a> Cursor<'a> {
    pub fn new(buffer: &'a mut Buffer) -> Self {
        Self {
            buffer,
            consumed: 0,
        }
    }

    pub fn offset(&self) -> u64 {
        self.buffer.tell() + self.consumed() as u64
    }

    pub fn remaining_len(&self) -> usize {
        self.buffer.len().saturating_sub(self.consumed)
    }

    pub fn is_finished(&self) -> bool {
        self.buffer.is_finished()
    }

    pub fn as_slice(&self) -> &[u8] {
        let input = self.buffer.as_slice();
        &input[self.consumed..]
    }

    pub fn buffer(&self) -> &Buffer {
        self.buffer
    }

    pub fn buffer_mut(&mut self) -> &mut Buffer {
        self.buffer
    }

    pub fn checkpoint(&self) -> Checkpoint {
        Checkpoint {
            consumed: self.consumed,
        }
    }

    pub fn rewind(&mut self, checkpoint: Checkpoint) {
        self.consumed = checkpoint.consumed;
    }

    pub fn advance(&mut self, n: usize) {
        debug_assert!(
            n <= self.remaining_len(),
            "advance length exceeds remaining length"
        );
        self.consumed += n;
    }

    pub fn commit(&mut self) {
        let consumed = self.consumed();
        self.buffer.consume(consumed);
        self.consumed = 0;
    }

    pub fn consumed(&self) -> usize {
        self.consumed
    }
}
