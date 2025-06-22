use crate::{
    helper::AnyResult,
    parser::{
        core::buffer::{Buffer, skip_bytes},
        state::traits::{InitializableParser, StateParser},
    },
};

pub struct SkipBytesParser<const N: usize> {
    remain: u64,
}

impl<const N: usize> InitializableParser for SkipBytesParser<N> {
    fn init<'a>(_: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        Ok((input, Self { remain: N as u64 }))
    }
}

impl<const N: usize> StateParser for SkipBytesParser<N> {
    type Output = ();

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        skip_bytes(buffer, &mut self.remain)?;
        Ok(())
    }
}
