use anyhow::Context;

use crate::{
    helper::AnyResult,
    parser::{
        core::{
            buffer::{Buffer, skip_bytes},
            raw::{RDBStr, read_rdb_len, read_rdb_str},
        },
        model::{Item, StreamEncoding},
        record::string::StringEncodingParser,
        state::{
            combinators::{
                RDBLenParser, ReduceParser, Seq2Parser, Seq3Parser, Seq4Parser, Seq5Parser,
            },
            traits::{InitializableParser, StateParser},
        },
    },
};

pub struct StreamListPackRecordParser<const ENC: StreamEncoding> {
    key: RDBStr,
    started: u64,
    entrust: Seq4Parser<
        ListPackEntriesParser,
        RDBLenParser,
        StreamMetaParser<ENC>,
        StreamGroupsParser<ENC>,
    >,
}

impl<const ENC: StreamEncoding> InitializableParser for StreamListPackRecordParser<ENC> {
    fn init<'a>(buffer: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let (input, key) = read_rdb_str(input).context("read key")?;
        let (input, entrust) = <Seq4Parser<
            ListPackEntriesParser,
            RDBLenParser,
            StreamMetaParser<ENC>,
            StreamGroupsParser<ENC>,
        > as InitializableParser>::init(buffer, input)?;

        Ok((input, Self {
            key,
            started: buffer.tell(),
            entrust,
        }))
    }
}

impl<const ENC: StreamEncoding> StateParser for StreamListPackRecordParser<ENC> {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        let (_, message_count, _, _) = self.entrust.call(buffer)?;
        Ok(Item::StreamRecord {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            encoding: ENC,
            message_count: message_count
                .as_u64()
                .context("message count should be a number")?,
        })
    }
}

struct EntriesReadParser<const ENC: StreamEncoding> {
    inner: Option<RDBLenParser>,
}

impl<const ENC: StreamEncoding> InitializableParser for EntriesReadParser<ENC> {
    fn init<'a>(buf: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        // `entries_read` field only exists in v2/v3, not in v1 (ListPacks).
        match ENC {
            StreamEncoding::ListPacks => Ok((input, Self { inner: None })),
            StreamEncoding::ListPacks2 | StreamEncoding::ListPacks3 => {
                let (input, parser) = RDBLenParser::init(buf, input)?;
                Ok((input, Self {
                    inner: Some(parser),
                }))
            }
        }
    }
}

impl<const ENC: StreamEncoding> StateParser for EntriesReadParser<ENC> {
    type Output = ();

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        if let Some(ref mut parser) = self.inner {
            let _ = parser.call(buffer)?;
            self.inner = None;
        }
        Ok(())
    }
}

struct StreamConsumersParser<const ENC: StreamEncoding> {
    entrust: ReduceParser<StreamConsumerParser<ENC>, ()>,
}

impl<const ENC: StreamEncoding> InitializableParser for StreamConsumersParser<ENC> {
    fn init<'a>(_: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        // Read the consumer count first.
        let (input, count) = read_rdb_len(input)?;
        let count = count.as_u64().context("consumer count should be numeric")?;

        let entrust: ReduceParser<StreamConsumerParser<ENC>, ()> =
            ReduceParser::new(count, (), |_, _| ());

        Ok((input, Self { entrust }))
    }
}

impl<const ENC: StreamEncoding> StateParser for StreamConsumersParser<ENC> {
    type Output = ();

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        self.entrust.call(buffer)?;
        Ok(())
    }
}

type StreamGroupParser<const ENC: StreamEncoding> = Seq5Parser<
    StringEncodingParser,
    Seq2Parser<RDBLenParser, RDBLenParser>,
    EntriesReadParser<ENC>,
    StreamPELParser<true>,
    StreamConsumersParser<ENC>,
>;

// helper parser that skips seen_time(8 bytes) and optionally active_time(8 bytes)
struct ConsumerTimeParser<const ENC: StreamEncoding> {
    remain: u64,
}

impl<const ENC: StreamEncoding> InitializableParser for ConsumerTimeParser<ENC> {
    fn init<'a>(_: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        Ok((input, Self {
            remain: match ENC {
                StreamEncoding::ListPacks3 => 16, // seen_time + active_time
                StreamEncoding::ListPacks | StreamEncoding::ListPacks2 => 8, // only seen_time
            },
        }))
    }
}

impl<const ENC: StreamEncoding> StateParser for ConsumerTimeParser<ENC> {
    type Output = ();

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        skip_bytes(buffer, &mut self.remain)?;
        Ok(())
    }
}

type StreamConsumerParser<const ENC: StreamEncoding> =
    Seq3Parser<StringEncodingParser, ConsumerTimeParser<ENC>, StreamPELParser<false>>;

struct StreamPELParser<const WITH_NACK: bool> {
    entrust: ReduceParser<PELEntryParser<WITH_NACK>, ()>,
}

impl<const WITH_NACK: bool> InitializableParser for StreamPELParser<WITH_NACK> {
    fn init<'a>(_: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let (input, len) = read_rdb_len(input)?;
        let len = len.as_u64().context("PEL length should be numeric")?;

        let entrust: ReduceParser<PELEntryParser<WITH_NACK>, ()> =
            ReduceParser::new(len, (), |_, _| ());

        Ok((input, Self { entrust }))
    }
}

impl<const WITH_NACK: bool> StateParser for StreamPELParser<WITH_NACK> {
    type Output = ();

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        self.entrust.call(buffer)?;
        Ok(())
    }
}

struct PELEntryParser<const WITH_NACK: bool> {
    remain: u64,
    need_read_varint: bool,
}

impl<const WITH_NACK: bool> InitializableParser for PELEntryParser<WITH_NACK> {
    fn init<'a>(_: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        // Each PEL entry is represented individually by StreamPELParser, so here we only
        // need to consume the bytes of **one** entry (id + optional nack fields).
        let remain = 16 + if WITH_NACK { 8 } else { 0 }; // id + delivery_time

        Ok((input, Self {
            remain,
            need_read_varint: WITH_NACK, // delivery_count (varint) when WITH_NACK
        }))
    }
}

impl<const WITH_NACK: bool> StateParser for PELEntryParser<WITH_NACK> {
    type Output = ();

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        skip_bytes(buffer, &mut self.remain)?;
        if self.need_read_varint {
            let (input, _) = read_rdb_len(buffer.as_ref())?;
            buffer.consume_to(input.as_ptr());
            self.need_read_varint = false;
        }
        Ok(())
    }
}

struct ListPackEntriesParser {
    entrust: ReduceParser<StringEncodingParser, ()>,
}

impl InitializableParser for ListPackEntriesParser {
    fn init<'a>(_: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let (input, lp_count) = read_rdb_len(input).context("read listpack count")?;
        let lp_count = lp_count
            .as_u64()
            .context("listpack count should be a number")?;

        let entrust: ReduceParser<StringEncodingParser, ()> =
            ReduceParser::new(lp_count * 2, (), |_, _| ());

        Ok((input, Self { entrust }))
    }
}

impl StateParser for ListPackEntriesParser {
    type Output = ();

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        self.entrust.call(buffer)?;
        Ok(())
    }
}

struct StreamMetaParser<const ENC: StreamEncoding> {
    entrust: ReduceParser<RDBLenParser, ()>,
}

impl<const ENC: StreamEncoding> InitializableParser for StreamMetaParser<ENC> {
    fn init<'a>(_: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let remain = match ENC {
            StreamEncoding::ListPacks => 2,  // last_id.ms + last_id.seq
            StreamEncoding::ListPacks2 => 7, /* last_id + first_id + max_deleted_id (each 2 varints) */
            StreamEncoding::ListPacks3 => 7, // v2 meta + entries_added
        };
        let entrust: ReduceParser<RDBLenParser, ()> = ReduceParser::new(remain, (), |_, _| ());
        Ok((input, Self { entrust }))
    }
}

impl<const ENC: StreamEncoding> StateParser for StreamMetaParser<ENC> {
    type Output = ();

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        self.entrust.call(buffer)?;
        Ok(())
    }
}

struct StreamGroupsParser<const ENC: StreamEncoding> {
    entrust: ReduceParser<StreamGroupParser<ENC>, ()>,
}

impl<const ENC: StreamEncoding> InitializableParser for StreamGroupsParser<ENC> {
    fn init<'a>(_: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let (input, group_count) = read_rdb_len(input)?;
        let group_count = group_count
            .as_u64()
            .context("group count should be a number")?;

        let entrust: ReduceParser<StreamGroupParser<ENC>, ()> =
            ReduceParser::new(group_count, (), |_, _| ());

        Ok((input, Self { entrust }))
    }
}

impl<const ENC: StreamEncoding> StateParser for StreamGroupsParser<ENC> {
    type Output = ();

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        self.entrust.call(buffer)?;
        Ok(())
    }
}
