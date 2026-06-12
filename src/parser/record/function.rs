use crate::{
    helper::AnyResult,
    parser::{
        core::{
            buffer::Buffer,
            parse::{Parser, ParserInit},
        },
        model::Item,
        string::StringEncodingParser,
    },
};

pub struct Function2RecordParser {
    started: u64,
    entrust: StringEncodingParser,
}

impl ParserInit for Function2RecordParser {
    fn init<'a>(buffer: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let (input, entrust) = StringEncodingParser::init(buffer, input)?;

        Ok((input, Self {
            started: buffer.tell(),
            entrust,
        }))
    }
}

impl Parser for Function2RecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        self.entrust.call(buffer)?;
        let rdb_size = buffer.tell() - self.started;
        Ok(Item::FunctionRecord { rdb_size })
    }
}
