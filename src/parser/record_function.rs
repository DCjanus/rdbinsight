use anyhow::{Context, ensure};

use super::{
    buffer::{Buffer, skip_bytes},
    item::Item,
    state_parser::StateParser,
};
use crate::{
    helper::AnyResult,
    parser::rdb_parsers::{RDBStr, read_rdb_len, read_rdb_str},
};

pub struct Function2RecordParser {
    started: u64,
    remain: u64,
    name: Option<RDBStr>,
}

impl Function2RecordParser {
    pub fn init(started: u64, input: &[u8]) -> AnyResult<(&[u8], Self)> {
        let (input, size) = read_rdb_len(input)?;
        let total = size
            .as_simple()
            .context("function size should be a simple number")?;

        Ok((input, Self {
            started,
            remain: total,
            name: None,
        }))
    }
}

impl StateParser for Function2RecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        if self.name.is_none() {
            let input = buffer.as_ref();
            let begin_size = input.len();
            let (input, name) = read_rdb_str(input).context("read function name in function2")?;
            let end_size = input.len();
            let consumed = (begin_size - end_size) as u64;
            ensure!(
                consumed <= self.remain,
                "consumed cannot exceed remaining bytes"
            );
            buffer.consume_to(input.as_ptr());
            self.remain -= consumed;
            self.name = Some(name);
        }
        skip_bytes(buffer, &mut self.remain)?;
        let rdb_size = buffer.tell() - self.started;
        Ok(Item::FunctionRecord { rdb_size })
    }
}
