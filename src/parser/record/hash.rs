use anyhow::{Context, anyhow, ensure};

use crate::{
    helper::AnyResult,
    parser::{
        core::{
            buffer::{Buffer, skip_bytes},
            combinators::{read_be_u32, read_u8},
            raw::{RDBStr, read_rdb_len, read_rdb_str},
        },
        model::{HashEncoding, Item},
        record::{
            list::ZipListLengthParser, set::ListPackLengthParser, string::StringEncodingParser,
        },
        state::{
            combinators::RDBStrBox,
            traits::{InitializableParser, StateParser},
        },
    },
};

pub struct HashRecordParser {
    started: u64,
    key: RDBStr,
    field_count: u64,
    remain: u64,
    entrust: Option<HashFieldParser>,
}

impl HashRecordParser {
    pub fn init(started: u64, input: &[u8]) -> AnyResult<(&[u8], Self)> {
        let (input, key) = read_rdb_str(input).context("read key")?;
        let (input, field_count) = read_rdb_len(input).context("read hash length")?;
        let field_count = field_count
            .as_u64()
            .context("hash length should be a number")?;
        Ok((input, Self {
            started,
            key,
            field_count,
            remain: field_count,
            entrust: None,
        }))
    }
}

impl StateParser for HashRecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        loop {
            if let Some(parser) = self.entrust.as_mut() {
                parser.call(buffer)?;
                self.entrust = None;
                self.remain -= 1;
            }
            if self.remain == 0 {
                break;
            }
            let (input, entrust) = HashFieldParser::init(buffer.as_ref())?;
            buffer.consume_to(input.as_ptr());
            self.entrust = Some(entrust);
        }

        Ok(Item::HashRecord {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            encoding: HashEncoding::Raw,
            field_count: self.field_count,
        })
    }
}

// Parser for a single field/value pair inside a raw hash table.
struct HashFieldParser {
    remain: u8, // 2 components: field and value strings
    entrust: Option<StringEncodingParser>,
}

impl HashFieldParser {
    fn init(input: &[u8]) -> AnyResult<(&[u8], Self)> {
        Ok((input, Self {
            remain: 2,
            entrust: None,
        }))
    }
}

impl StateParser for HashFieldParser {
    type Output = ();

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        loop {
            if let Some(parser) = self.entrust.as_mut() {
                let _ = parser.call(buffer)?;
                self.remain -= 1;
                self.entrust = None;
            }
            if self.remain == 0 {
                return Ok(());
            }
            let (input, entrust) = StringEncodingParser::init(buffer, buffer.as_ref())?;
            buffer.consume_to(input.as_ptr());
            self.entrust = Some(entrust);
        }
    }
}

// ------------------------- ZipList Encoding (id = 13) ------------------------

pub struct HashZipListRecordParser {
    started: u64,
    key: RDBStr,
    entrust: RDBStrBox<ZipListLengthParser>,
}

impl InitializableParser for HashZipListRecordParser {
    fn init<'a>(buf: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let (input, key) = read_rdb_str(input).context("read key")?;
        let (input, entrust) = RDBStrBox::<ZipListLengthParser>::init(buf, input)?;
        Ok((input, Self {
            started: buf.tell(),
            key,
            entrust,
        }))
    }
}

impl StateParser for HashZipListRecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        let entry_count = self.entrust.call(buffer)?;
        ensure!(
            entry_count % 2 == 0,
            "ziplist entry count should be even for hash"
        );
        let field_count = entry_count / 2;

        Ok(Item::HashRecord {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            encoding: HashEncoding::ZipList,
            field_count,
        })
    }
}

// ------------------------ ListPack Encoding (id = 16) ------------------------

pub struct HashListPackRecordParser {
    started: u64,
    key: RDBStr,
    entrust: RDBStrBox<ListPackLengthParser>,
}

impl InitializableParser for HashListPackRecordParser {
    fn init<'a>(buffer: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let (input, key) = read_rdb_str(input).context("read key")?;
        let (input, entrust) = RDBStrBox::<ListPackLengthParser>::init(buffer, input)?;
        Ok((input, Self {
            started: buffer.tell(),
            key,
            entrust,
        }))
    }
}

impl StateParser for HashListPackRecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        let entry_count = self.entrust.call(buffer)?;
        ensure!(
            entry_count % 2 == 0,
            "listpack entry count should be even for hash"
        );
        let field_count = entry_count / 2;

        Ok(Item::HashRecord {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            encoding: HashEncoding::ListPack,
            field_count,
        })
    }
}

pub struct HashZipMapRecordParser {
    started: u64,
    key: RDBStr,
    entrust: RDBStrBox<ZipMapPairCountParser>,
}

impl InitializableParser for HashZipMapRecordParser {
    fn init<'a>(buffer: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let started = buffer.tell();
        let (input, key) = read_rdb_str(input).context("read key")?;
        let (input, entrust) = RDBStrBox::<ZipMapPairCountParser>::init(buffer, input)?;
        if entrust.is_lzf() {
            crate::parser_trace!("hash.zipmap.lzf");
        } else {
            crate::parser_trace!("hash.zipmap.raw");
        }
        Ok((input, Self {
            started,
            key,
            entrust,
        }))
    }
}
impl StateParser for HashZipMapRecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        let field_count = self.entrust.call(buffer)?;
        Ok(Item::HashRecord {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            encoding: HashEncoding::ZipMap,
            field_count,
        })
    }
}

// TODO: replace with ReduceParser but in condition
struct ZipMapPairCountParser {
    entrust: Option<IsEndZipMapPairParser>,
    member_count: u64,
}

impl InitializableParser for ZipMapPairCountParser {
    fn init<'a>(_: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let (input, _) = read_u8(input)?;
        Ok((input, Self {
            entrust: None,
            member_count: 0,
        }))
    }
}

impl StateParser for ZipMapPairCountParser {
    type Output = u64;
    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        loop {
            if let Some(parser) = self.entrust.as_mut() {
                if parser.call(buffer)? {
                    return Ok(self.member_count);
                }
                self.member_count += 1;
                self.entrust = None;
            }

            let (input, entrust) = IsEndZipMapPairParser::init(buffer, buffer.as_ref())?;
            buffer.consume_to(input.as_ptr());
            self.entrust = Some(entrust);
        }
    }
}

// TODO: replace with SeqParser
enum IsEndZipMapPairParser {
    ReadingKey { remain: u64 },
    ReadingValue { remain: u64 },
    EndOfList,
}

impl InitializableParser for IsEndZipMapPairParser {
    fn init<'a>(_: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let (input, size) = read_zipmap_size(input)?;
        match size {
            Some(key_len) => Ok((input, Self::ReadingKey { remain: key_len })),
            None => Ok((input, Self::EndOfList)),
        }
    }
}

fn read_zipmap_size(input: &[u8]) -> AnyResult<(&[u8], Option<u64>)> {
    let (input, flag) = read_u8(input)?;
    if flag == 0xFF {
        return Ok((input, None));
    }
    if flag != 0xFE {
        return Ok((input, Some(flag as u64)));
    }

    let (input, key_len) = read_be_u32(input)?;
    Ok((input, Some(key_len as u64)))
}

impl StateParser for IsEndZipMapPairParser {
    type Output = bool;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        loop {
            match self {
                IsEndZipMapPairParser::ReadingKey { remain } => {
                    skip_bytes(buffer, remain)?;

                    let input = buffer.as_ref();
                    let (input, value_size) = read_zipmap_size(input)?;
                    let value_size = value_size.ok_or_else(|| anyhow!("value size not found"))?;

                    let (input, free_size) = read_u8(input)?;
                    let remain = value_size + free_size as u64;

                    *self = IsEndZipMapPairParser::ReadingValue { remain };
                    buffer.consume_to(input.as_ptr());
                }
                IsEndZipMapPairParser::ReadingValue { remain } => {
                    skip_bytes(buffer, remain)?;
                    return Ok(false);
                }
                IsEndZipMapPairParser::EndOfList => {
                    return Ok(true);
                }
            }
        }
    }
}
