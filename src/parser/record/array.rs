use anyhow::{Context, bail};

use crate::{
    helper::AnyResult,
    parser::{
        core::{
            buffer::{Buffer, skip_bytes},
            raw::{RDBLen, RDBStr, read_rdb_len, read_rdb_str},
        },
        model::Item,
        record::string::StringEncodingParser,
        state::traits::{InitializableParser, StateParser},
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

impl InitializableParser for ArrayRecordParser {
    fn init<'a>(buffer: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let (input, key) = read_rdb_str(input).context("read array key")?;
        let (input, member_count) = read_rdb_len(input).context("read array element count")?;
        let member_count = member_count
            .as_u64()
            .context("array element count should be numeric")?;

        let (input, insert_idx_flag) =
            read_rdb_len(input).context("read array insert index flag")?;
        match insert_idx_flag
            .as_u64()
            .context("array insert index flag should be numeric")?
        {
            0 => {}
            1 => {
                let (next, _) = read_rdb_len(input).context("read array insert index")?;
                return Ok((next, Self {
                    started: buffer.tell(),
                    key,
                    member_count,
                    entries: ArrayEntriesParser::new(member_count),
                }));
            }
            other => bail!("invalid array insert index flag: {other}"),
        }

        Ok((input, Self {
            started: buffer.tell(),
            key,
            member_count,
            entries: ArrayEntriesParser::new(member_count),
        }))
    }
}

impl StateParser for ArrayRecordParser {
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

impl StateParser for ArrayEntriesParser {
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

            let (input, entry) = ArrayElementParser::init(buffer, buffer.as_slice())?;
            buffer.consume_to(input.as_ptr());
            self.entry = Some(entry);
        }
    }
}

enum ArrayElementValueParser {
    Skip { remain: u64 },
    String(StringEncodingParser),
}

impl StateParser for ArrayElementValueParser {
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

impl InitializableParser for ArrayElementParser {
    fn init<'a>(buffer: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let (input, index) = read_rdb_len(input).context("read array element index")?;
        if !matches!(index, RDBLen::Simple(_)) {
            bail!("array element index should be a plain length");
        }

        let (input, tag) = read_rdb_len(input).context("read array element type tag")?;
        let tag = tag
            .as_u64()
            .context("array element type tag should be numeric")?;

        let (input, value) = match tag {
            AR_RDB_TAG_INT | AR_RDB_TAG_FLOAT => {
                (input, ArrayElementValueParser::Skip { remain: 8 })
            }
            AR_RDB_TAG_SMALLSTR | AR_RDB_TAG_SDS => {
                let (input, parser) = StringEncodingParser::init(buffer, input)?;
                (input, ArrayElementValueParser::String(parser))
            }
            other => bail!("unknown array element type tag: {other}"),
        };

        Ok((input, Self { value }))
    }
}

impl StateParser for ArrayElementParser {
    type Output = ();

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        self.value.call(buffer)
    }
}
