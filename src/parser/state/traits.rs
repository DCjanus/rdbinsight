use crate::{helper::AnyResult, parser::core::buffer::Buffer};

pub trait StateParser {
    type Output;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output>;
}

pub trait InitializableParser: StateParser + Sized {
    fn init<'a>(buf: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)>;
}
