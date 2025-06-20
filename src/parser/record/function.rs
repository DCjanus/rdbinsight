use anyhow::Context;

use crate::{
    helper::AnyResult,
    parser::{
        core::{
            buffer::{Buffer, skip_bytes},
            raw::read_rdb_len,
        },
        model::Item,
        state::traits::{InitializableParser, StateParser},
    },
};

pub struct Function2RecordParser {
    started: u64,
    remain: u64,
}

impl InitializableParser for Function2RecordParser {
    fn init<'a>(buffer: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let (input, size) = read_rdb_len(input)?;
        let remain = size.as_u64().context("function size should be a number")?;

        Ok((input, Self {
            started: buffer.tell(),
            remain,
        }))
    }
}

impl StateParser for Function2RecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        skip_bytes(buffer, &mut self.remain)?;
        let rdb_size = buffer.tell() - self.started;
        Ok(Item::FunctionRecord { rdb_size })
    }
}
