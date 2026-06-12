use crate::{
    helper::AnyResult,
    parser::{
        core::{
            buffer::Buffer,
            raw::{RDBLen, read_rdb_len},
        },
        state::traits::{InitializableParser, StateParser},
    },
};

pub struct RDBLenParser;

impl StateParser for RDBLenParser {
    type Output = RDBLen;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        let (input, len) = read_rdb_len(buffer.as_slice())?;
        buffer.consume_to(input.as_ptr());
        Ok(len)
    }
}

impl InitializableParser for RDBLenParser {
    fn init<'a>(_: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        Ok((input, Self))
    }
}
