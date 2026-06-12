use anyhow::{Context, bail};

use crate::{
    helper::AnyResult,
    parse_try,
    parser::{
        core::{
            buffer::{Buffer, skip_bytes},
            cursor::Cursor,
            parse::{ParseResult, Parser, ParserInit},
            raw::{RDBLen, RDBStr, read_rdb_len, read_rdb_str},
            view::View,
        },
        model::Item,
        record::string::StringEncodingParser,
    },
};

const AR_RDB_TAG_SDS: u64 = 0;
const AR_RDB_TAG_INT: u64 = 1;
const AR_RDB_TAG_FLOAT: u64 = 2;
const AR_RDB_TAG_SMALLSTR: u64 = 3;

pub struct ArrayRecordParser {
    started: u64,
    key: RDBStr,
    member_count: u64,
    entries: ArrayEntriesParser,
}

impl ParserInit for ArrayRecordParser {
    fn init(view: &mut View<'_>) -> ParseResult<Self> {
        let started = view.base_offset();
        view.parse_init(|_, input| {
            let (input, key) = read_rdb_str(input).context("read array key")?;
            let (input, member_count) = read_rdb_len(input).context("read array element count")?;
            let member_count = member_count
                .as_u64()
                .context("array element count should be numeric")?;

            let (input, insert_idx_flag) =
                read_rdb_len(input).context("read array insert index flag")?;
            let input = match insert_idx_flag
                .as_u64()
                .context("array insert index flag should be numeric")?
            {
                0 => input,
                1 => {
                    let (input, _) = read_rdb_len(input).context("read array insert index")?;
                    input
                }
                other => bail!("invalid array insert index flag: {other}"),
            };

            Ok((input, Self {
                started,
                key,
                member_count,
                entries: ArrayEntriesParser::new(member_count),
            }))
        })
    }
}

impl Parser for ArrayRecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        self.entries.call(buffer)?;
        Ok(Item::ArrayRecord {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            member_count: self.member_count,
        })
    }
}

struct ArrayEntriesParser {
    remain: u64,
    entry: Option<ArrayElementParser>,
}

impl ArrayEntriesParser {
    fn new(remain: u64) -> Self {
        Self {
            remain,
            entry: None,
        }
    }
}

impl Parser for ArrayEntriesParser {
    type Output = ();

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        loop {
            if let Some(entry) = self.entry.as_mut() {
                entry.call(buffer)?;
                self.entry = None;
                self.remain -= 1;
            }

            if self.remain == 0 {
                return Ok(());
            }

            let entry = {
                let mut cursor = Cursor::new(buffer);
                cursor.init_commit::<ArrayElementParser>()?
            };
            self.entry = Some(entry);
        }
    }
}

enum ArrayElementValueParser {
    Skip { remain: u64 },
    String(StringEncodingParser),
}

impl Parser for ArrayElementValueParser {
    type Output = ();

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        match self {
            Self::Skip { remain } => skip_bytes(buffer, remain),
            Self::String(parser) => {
                parser.call(buffer)?;
                Ok(())
            }
        }
    }
}

struct ArrayElementParser {
    value: ArrayElementValueParser,
}

impl ParserInit for ArrayElementParser {
    fn init(view: &mut View<'_>) -> ParseResult<Self> {
        let tag = parse_try!(view.parse_init(|_, input| {
            let (input, index) = read_rdb_len(input).context("read array element index")?;
            if !matches!(index, RDBLen::Simple(_)) {
                bail!("array element index should be a plain length");
            }

            let (input, tag) = read_rdb_len(input).context("read array element type tag")?;
            let tag = tag
                .as_u64()
                .context("array element type tag should be numeric")?;

            Ok((input, tag))
        }));

        let value = match tag {
            AR_RDB_TAG_INT | AR_RDB_TAG_FLOAT => ArrayElementValueParser::Skip { remain: 8 },
            AR_RDB_TAG_SMALLSTR | AR_RDB_TAG_SDS => ArrayElementValueParser::String(parse_try!(
                view.init_parser::<StringEncodingParser>()
            )),
            other => {
                return crate::parser::core::parse::fatal(anyhow::anyhow!(
                    "unknown array element type tag: {other}"
                ));
            }
        };

        ParseResult::Ok(Self { value })
    }
}

impl Parser for ArrayElementParser {
    type Output = ();

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        self.value.call(buffer)
    }
}
