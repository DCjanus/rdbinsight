use crate::{
    helper::AnyResult,
    parser::{
        core::buffer::Buffer,
        model::Item,
        state::traits::{InitializableParser, StateParser},
        string::StringEncodingParser,
    },
};

pub struct Function2RecordParser {
    started: u64,
    entrust: StringEncodingParser,
}

impl InitializableParser for Function2RecordParser {
    fn init<'a>(buffer: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let (input, entrust) = StringEncodingParser::init(buffer, input)?;

        Ok((input, Self {
            started: buffer.tell(),
            entrust,
        }))
    }
}

impl StateParser for Function2RecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        self.entrust.call(buffer)?;
        let rdb_size = buffer.tell() - self.started;
        Ok(Item::FunctionRecord { rdb_size })
    }
}
