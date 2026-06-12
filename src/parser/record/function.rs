use crate::{
    helper::AnyResult,
    parse_try,
    parser::{
        core::{
            buffer::Buffer,
            parse::{ParseResult, Parser, ParserInit},
            view::View,
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
    fn init(view: &mut View<'_>) -> ParseResult<Self> {
        let entrust = parse_try!(view.init_parser::<StringEncodingParser>());
        ParseResult::Ok(Self {
            started: view.base_offset(),
            entrust,
        })
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
