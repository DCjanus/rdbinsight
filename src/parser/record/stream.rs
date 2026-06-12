use anyhow::Context;

use crate::{
    helper::AnyResult,
    parse_try,
    parser::{
        core::{
            buffer::{Buffer, skip_bytes},
            cursor::Cursor,
            parse::{ParseResult, Parser, ParserInit},
            raw::{RDBStr, read_rdb_len, read_rdb_str},
            view::View,
        },
        model::{Item, StreamEncoding},
        record::string::StringEncodingParser,
        runtime::combinators::{
            RDBLenParser, ReduceParser, Seq2Parser, Seq3Parser, Seq4Parser, Seq5Parser,
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
    idmp: StreamIdmpParser<ENC>,
    message_count: Option<u64>,
}

impl<const ENC: StreamEncoding> ParserInit for StreamListPackRecordParser<ENC> {
    fn init(view: &mut View<'_>) -> ParseResult<Self> {
        let started = view.base_offset();
        let key = parse_try!(view.parse_init(|_, input| {
            let (input, key) = read_rdb_str(input).context("read key")?;
            Ok((input, key))
        }));
        let entrust = parse_try!(view.init_parser::<Seq4Parser<
            ListPackEntriesParser,
            RDBLenParser,
            StreamMetaParser<ENC>,
            StreamGroupsParser<ENC>,
        >>());
        let idmp = parse_try!(view.init_parser::<StreamIdmpParser<ENC>>());

        ParseResult::Ok(Self {
            key,
            started,
            entrust,
            idmp,
            message_count: None,
        })
    }
}

impl<const ENC: StreamEncoding> Parser for StreamListPackRecordParser<ENC> {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        if self.message_count.is_none() {
            let (_, message_count, _, _) = self.entrust.call(buffer)?;
            self.message_count = Some(
                message_count
                    .as_u64()
                    .context("message count should be a number")?,
            );
        }
        self.idmp.call(buffer)?;
        Ok(Item::StreamRecord {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            encoding: ENC,
            message_count: self
                .message_count
                .take()
                .context("stream message count should be parsed")?,
        })
    }
}

struct EntriesReadParser<const ENC: StreamEncoding> {
    inner: Option<RDBLenParser>,
}

impl<const ENC: StreamEncoding> ParserInit for EntriesReadParser<ENC> {
    fn init(view: &mut View<'_>) -> ParseResult<Self> {
        // `entries_read` field only exists in v2+, not in v1 (ListPacks).
        match ENC {
            StreamEncoding::ListPacks => ParseResult::Ok(Self { inner: None }),
            StreamEncoding::ListPacks2
            | StreamEncoding::ListPacks3
            | StreamEncoding::ListPacks4
            | StreamEncoding::ListPacks5 => {
                let parser = parse_try!(view.init_parser::<RDBLenParser>());
                ParseResult::Ok(Self {
                    inner: Some(parser),
                })
            }
        }
    }
}

impl<const ENC: StreamEncoding> Parser for EntriesReadParser<ENC> {
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

impl<const ENC: StreamEncoding> ParserInit for StreamConsumersParser<ENC> {
    fn init(view: &mut View<'_>) -> ParseResult<Self> {
        view.parse_init(|_buffer, input| {
            // Read the consumer count first.
            let (input, count) = read_rdb_len(input)?;
            let count = count.as_u64().context("consumer count should be numeric")?;

            let entrust: ReduceParser<StreamConsumerParser<ENC>, ()> =
                ReduceParser::new(count, (), |_, _| ());

            Ok((input, Self { entrust }))
        })
    }
}

impl<const ENC: StreamEncoding> Parser for StreamConsumersParser<ENC> {
    type Output = ();

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        self.entrust.call(buffer)?;
        Ok(())
    }
}

struct StreamGroupParser<const ENC: StreamEncoding> {
    entrust: Seq5Parser<
        StringEncodingParser,
        Seq2Parser<RDBLenParser, RDBLenParser>,
        EntriesReadParser<ENC>,
        StreamPELParser<true>,
        StreamConsumersParser<ENC>,
    >,
    entrust_done: bool,
    nacks: StreamNackZoneParser<ENC>,
}

impl<const ENC: StreamEncoding> ParserInit for StreamGroupParser<ENC> {
    fn init(view: &mut View<'_>) -> ParseResult<Self> {
        let entrust = parse_try!(view.init_parser::<Seq5Parser<
            StringEncodingParser,
            Seq2Parser<RDBLenParser, RDBLenParser>,
            EntriesReadParser<ENC>,
            StreamPELParser<true>,
            StreamConsumersParser<ENC>,
        >>());
        let nacks = parse_try!(view.init_parser::<StreamNackZoneParser<ENC>>());
        ParseResult::Ok(Self {
            entrust,
            entrust_done: false,
            nacks,
        })
    }
}

impl<const ENC: StreamEncoding> Parser for StreamGroupParser<ENC> {
    type Output = ();

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        if !self.entrust_done {
            self.entrust.call(buffer)?;
            self.entrust_done = true;
        }
        self.nacks.call(buffer)?;
        Ok(())
    }
}

// helper parser that skips seen_time(8 bytes) and optionally active_time(8 bytes)
struct ConsumerTimeParser<const ENC: StreamEncoding> {
    remain: u64,
}

impl<const ENC: StreamEncoding> ParserInit for ConsumerTimeParser<ENC> {
    fn init(view: &mut View<'_>) -> ParseResult<Self> {
        view.parse_init(|_buffer, input| {
            Ok((input, Self {
                remain: match ENC {
                    StreamEncoding::ListPacks3
                    | StreamEncoding::ListPacks4
                    | StreamEncoding::ListPacks5 => 16, // seen_time + active_time
                    StreamEncoding::ListPacks | StreamEncoding::ListPacks2 => 8, // only seen_time
                },
            }))
        })
    }
}

impl<const ENC: StreamEncoding> Parser for ConsumerTimeParser<ENC> {
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

impl<const WITH_NACK: bool> ParserInit for StreamPELParser<WITH_NACK> {
    fn init(view: &mut View<'_>) -> ParseResult<Self> {
        view.parse_init(|_buffer, input| {
            let (input, len) = read_rdb_len(input)?;
            let len = len.as_u64().context("PEL length should be numeric")?;

            let entrust: ReduceParser<PELEntryParser<WITH_NACK>, ()> =
                ReduceParser::new(len, (), |_, _| ());

            Ok((input, Self { entrust }))
        })
    }
}

impl<const WITH_NACK: bool> Parser for StreamPELParser<WITH_NACK> {
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

impl<const WITH_NACK: bool> ParserInit for PELEntryParser<WITH_NACK> {
    fn init(view: &mut View<'_>) -> ParseResult<Self> {
        view.parse_init(|_buffer, input| {
            // Each PEL entry is represented individually by StreamPELParser, so here we only
            // need to consume the bytes of **one** entry (id + optional nack fields).
            let remain = 16 + if WITH_NACK { 8 } else { 0 }; // id + delivery_time

            Ok((input, Self {
                remain,
                need_read_varint: WITH_NACK, // delivery_count (varint) when WITH_NACK
            }))
        })
    }
}

impl<const WITH_NACK: bool> Parser for PELEntryParser<WITH_NACK> {
    type Output = ();

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        skip_bytes(buffer, &mut self.remain)?;
        if self.need_read_varint {
            let (input, _) = read_rdb_len(buffer.as_slice())?;
            buffer.consume_to(input.as_ptr());
            self.need_read_varint = false;
        }
        Ok(())
    }
}

struct StreamNackZoneParser<const ENC: StreamEncoding> {
    enabled: bool,
    entries: Option<ReduceParser<StreamIdParser, ()>>,
}

impl<const ENC: StreamEncoding> ParserInit for StreamNackZoneParser<ENC> {
    fn init(_: &mut View<'_>) -> ParseResult<Self> {
        ParseResult::Ok(Self {
            enabled: ENC == StreamEncoding::ListPacks5,
            entries: None,
        })
    }
}

impl<const ENC: StreamEncoding> Parser for StreamNackZoneParser<ENC> {
    type Output = ();

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        if !self.enabled {
            return Ok(());
        }

        if self.entries.is_none() {
            let (input, count) =
                read_rdb_len(buffer.as_slice()).context("read stream NACK zone count")?;
            buffer.consume_to(input.as_ptr());
            let count = count
                .as_u64()
                .context("stream NACK zone count should be numeric")?;
            self.entries = Some(ReduceParser::new(count, (), ignore_unit as fn((), ())));
        }

        if let Some(ref mut entries) = self.entries {
            entries.call(buffer)?;
        }
        self.entries = None;
        self.enabled = false;
        Ok(())
    }
}

struct StreamIdParser {
    remain: u64,
}

impl ParserInit for StreamIdParser {
    fn init(_: &mut View<'_>) -> ParseResult<Self> {
        ParseResult::Ok(Self { remain: 16 })
    }
}

impl Parser for StreamIdParser {
    type Output = ();

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        skip_bytes(buffer, &mut self.remain)?;
        Ok(())
    }
}

struct ListPackEntriesParser {
    entrust: ReduceParser<StringEncodingParser, ()>,
}

impl ParserInit for ListPackEntriesParser {
    fn init(view: &mut View<'_>) -> ParseResult<Self> {
        view.parse_init(|_buffer, input| {
            let (input, lp_count) = read_rdb_len(input).context("read listpack count")?;
            let lp_count = lp_count
                .as_u64()
                .context("listpack count should be a number")?;

            let entrust: ReduceParser<StringEncodingParser, ()> =
                ReduceParser::new(lp_count * 2, (), |_, _| ());

            Ok((input, Self { entrust }))
        })
    }
}

impl Parser for ListPackEntriesParser {
    type Output = ();

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        self.entrust.call(buffer)?;
        Ok(())
    }
}

struct StreamMetaParser<const ENC: StreamEncoding> {
    entrust: ReduceParser<RDBLenParser, ()>,
}

impl<const ENC: StreamEncoding> ParserInit for StreamMetaParser<ENC> {
    fn init(view: &mut View<'_>) -> ParseResult<Self> {
        view.parse_init(|_buffer, input| {
            let remain = match ENC {
                StreamEncoding::ListPacks => 2,  // last_id.ms + last_id.seq
                StreamEncoding::ListPacks2 => 7, /* last_id + first_id + max_deleted_id (each 2 varints) */
                StreamEncoding::ListPacks3
                | StreamEncoding::ListPacks4
                | StreamEncoding::ListPacks5 => 7, // v2 meta + entries_added
            };
            let entrust: ReduceParser<RDBLenParser, ()> = ReduceParser::new(remain, (), |_, _| ());
            Ok((input, Self { entrust }))
        })
    }
}

impl<const ENC: StreamEncoding> Parser for StreamMetaParser<ENC> {
    type Output = ();

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        self.entrust.call(buffer)?;
        Ok(())
    }
}

struct StreamGroupsParser<const ENC: StreamEncoding> {
    entrust: ReduceParser<StreamGroupParser<ENC>, ()>,
}

impl<const ENC: StreamEncoding> ParserInit for StreamGroupsParser<ENC> {
    fn init(view: &mut View<'_>) -> ParseResult<Self> {
        view.parse_init(|_buffer, input| {
            let (input, group_count) = read_rdb_len(input)?;
            let group_count = group_count
                .as_u64()
                .context("group count should be a number")?;

            let entrust: ReduceParser<StreamGroupParser<ENC>, ()> =
                ReduceParser::new(group_count, (), |_, _| ());

            Ok((input, Self { entrust }))
        })
    }
}

impl<const ENC: StreamEncoding> Parser for StreamGroupsParser<ENC> {
    type Output = ();

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        self.entrust.call(buffer)?;
        Ok(())
    }
}

struct StreamIdmpEntryParser {
    entrust: Seq3Parser<StringEncodingParser, RDBLenParser, RDBLenParser>,
}

fn ignore_unit(_: (), _: ()) {}

impl ParserInit for StreamIdmpEntryParser {
    fn init(view: &mut View<'_>) -> ParseResult<Self> {
        let entrust = parse_try!(
            view.init_parser::<Seq3Parser<StringEncodingParser, RDBLenParser, RDBLenParser>>()
        );
        ParseResult::Ok(Self { entrust })
    }
}

impl Parser for StreamIdmpEntryParser {
    type Output = ();

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        self.entrust.call(buffer)?;
        Ok(())
    }
}

struct StreamIdmpProducerParser {
    producer_id: StringEncodingParser,
    producer_id_done: bool,
    entries: Option<ReduceParser<StreamIdmpEntryParser, ()>>,
}

impl ParserInit for StreamIdmpProducerParser {
    fn init(view: &mut View<'_>) -> ParseResult<Self> {
        let producer_id = parse_try!(view.init_parser::<StringEncodingParser>());
        ParseResult::Ok(Self {
            producer_id,
            producer_id_done: false,
            entries: None,
        })
    }
}

impl Parser for StreamIdmpProducerParser {
    type Output = ();

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        if !self.producer_id_done {
            self.producer_id.call(buffer)?;
            self.producer_id_done = true;
        }

        if self.entries.is_none() {
            let (input, entry_count) =
                read_rdb_len(buffer.as_slice()).context("read stream IDMP entry count")?;
            buffer.consume_to(input.as_ptr());
            let entry_count = entry_count
                .as_u64()
                .context("stream IDMP entry count should be numeric")?;
            self.entries = Some(ReduceParser::new(
                entry_count,
                (),
                ignore_unit as fn((), ()),
            ));
        }

        if let Some(ref mut entries) = self.entries {
            entries.call(buffer)?;
        }
        Ok(())
    }
}

struct StreamIdmpProducersParser {
    producers: ReduceParser<StreamIdmpProducerParser, ()>,
}

impl ParserInit for StreamIdmpProducersParser {
    fn init(view: &mut View<'_>) -> ParseResult<Self> {
        view.parse_init(|_buffer, input| {
            let (input, producer_count) =
                read_rdb_len(input).context("read stream IDMP producer count")?;
            let producer_count = producer_count
                .as_u64()
                .context("stream IDMP producer count should be numeric")?;
            Ok((input, Self {
                producers: ReduceParser::new(producer_count, (), ignore_unit as fn((), ())),
            }))
        })
    }
}

impl Parser for StreamIdmpProducersParser {
    type Output = ();

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        self.producers.call(buffer)?;
        Ok(())
    }
}

struct StreamIdmpParser<const ENC: StreamEncoding> {
    enabled: bool,
    entrust: Option<
        Seq5Parser<
            RDBLenParser,
            RDBLenParser,
            StreamIdmpProducersParser,
            RDBLenParser,
            RDBLenParser,
        >,
    >,
}

impl<const ENC: StreamEncoding> ParserInit for StreamIdmpParser<ENC> {
    fn init(view: &mut View<'_>) -> ParseResult<Self> {
        view.parse_init(|_buffer, input| {
            Ok((input, Self {
                enabled: ENC == StreamEncoding::ListPacks4 || ENC == StreamEncoding::ListPacks5,
                entrust: None,
            }))
        })
    }
}

impl<const ENC: StreamEncoding> Parser for StreamIdmpParser<ENC> {
    type Output = ();

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        if !self.enabled {
            return Ok(());
        }

        if self.entrust.is_none() {
            let entrust = {
                let mut cursor = Cursor::new(buffer);
                cursor.init_commit::<Seq5Parser<
                    RDBLenParser,
                    RDBLenParser,
                    StreamIdmpProducersParser,
                    RDBLenParser,
                    RDBLenParser,
                >>()?
            };
            self.entrust = Some(entrust);
        }

        if let Some(ref mut entrust) = self.entrust {
            entrust.call(buffer)?;
        }
        self.entrust = None;
        self.enabled = false;
        Ok(())
    }
}
