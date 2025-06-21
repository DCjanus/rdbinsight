use anyhow::{Context, bail};

use crate::{
    helper::AnyResult,
    parser::{
        core::{
            buffer::Buffer,
            raw::{RDBStr, read_rdb_len, read_rdb_str},
        },
        model::{Item, StreamEncoding},
        record::string::StringEncodingParser,
        state::{
            combinators::ReduceParser,
            traits::{InitializableParser, StateParser},
        },
    },
};

pub struct StreamListPackRecordParser<const ENC: StreamEncoding> {
    key: RDBStr,
    lp_parser: Option<ReduceParser<StringEncodingParser, ()>>,
    message_count: Option<u64>,
    remain_meta: u8,
    started: u64,
}

impl<const ENC: StreamEncoding> InitializableParser for StreamListPackRecordParser<ENC> {
    fn init<'a>(buffer: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let (input, key) = read_rdb_str(input).context("read key")?;
        let (input, lp_count) = read_rdb_len(input).context("read listpack count")?;
        let lp_count = lp_count
            .as_u64()
            .context("listpack count should be a number")?;
        let lp_parser: ReduceParser<StringEncodingParser, ()> =
            ReduceParser::new(lp_count * 2, (), |_, _| ());
        let remain_meta = match ENC {
            StreamEncoding::ListPacks => 2,
            StreamEncoding::ListPacks2 => 7,
            StreamEncoding::ListPacks3 => 7,
        };
        Ok((input, Self {
            key,
            lp_parser: Some(lp_parser),
            remain_meta,
            message_count: None,
            started: buffer.tell(),
        }))
    }
}

impl<const ENC: StreamEncoding> StateParser for StreamListPackRecordParser<ENC> {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        if let Some(lp_parser) = self.lp_parser.as_mut() {
            lp_parser.call(buffer)?;
            self.lp_parser = None;
        }

        if self.message_count.is_none() {
            let (input, message_count) = read_rdb_len(buffer.as_ref())?;
            let message_count = message_count
                .as_u64()
                .context("message count should be a number")?;
            self.message_count = Some(message_count);
            buffer.consume_to(input.as_ptr());
        }

        while self.remain_meta > 0 {
            let (input, _) = read_rdb_len(buffer.as_ref())?;
            buffer.consume_to(input.as_ptr());
            self.remain_meta -= 1;
        }

        let (input, group_count) = read_rdb_len(buffer.as_ref())?;
        let group_count = group_count
            .as_u64()
            .context("group count should be a number")?;
        buffer.consume_to(input.as_ptr());
        if group_count != 0 {
            // TODO: support consumer groups
            bail!("streams with consumer groups are not supported yet");
        }

        Ok(Item::StreamRecord {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            encoding: ENC,
            message_count: self.message_count.unwrap(),
        })
    }
}
