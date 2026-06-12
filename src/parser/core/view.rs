use crate::parser::core::buffer::Buffer;

#[derive(Debug, Clone, Copy)]
pub struct View<'a> {
    input: &'a [u8],
    consumed: usize,
    absolute_start: u64,
    base_ptr: *const u8,
}

impl<'a> View<'a> {
    pub(crate) fn new(buffer: &'a Buffer) -> Self {
        let input = buffer.as_slice();
        Self {
            input,
            consumed: 0,
            absolute_start: buffer.tell(),
            base_ptr: input.as_ptr(),
        }
    }

    pub fn remaining(&self) -> &'a [u8] {
        &self.input[self.consumed..]
    }

    pub fn consumed(&self) -> usize {
        self.consumed
    }

    pub fn offset(&self) -> u64 {
        self.absolute_start + self.consumed as u64
    }

    pub(crate) fn base_ptr(&self) -> *const u8 {
        self.base_ptr
    }
}
