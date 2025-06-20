use anyhow::{Context, bail, ensure};

use crate::{
    helper::AnyResult,
    parser::{
        StringEncoding,
        core::{
            buffer::{Buffer, skip_bytes},
            combinators::{read_exact, read_u8},
            raw::{RDBStr, read_rdb_len, read_rdb_str},
        },
        model::{Item, SetEncoding},
        record::string::StringEncodingParser,
        state::{
            combinators::{RDBStrBox, ReduceParser},
            traits::{InitializableParser, StateParser},
        },
    },
};

pub struct SetRecordParser {
    started: u64,
    key: RDBStr,
    entrust: ReduceParser<StringEncodingParser, u64>,
}

impl InitializableParser for SetRecordParser {
    fn init<'a>(buffer: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let (input, key) = read_rdb_str(input).context("read key")?;
        let (input, member_count) = read_rdb_len(input).context("read set length")?;
        let member_count = member_count
            .as_u64()
            .context("set length should be a number")?;

        let entrust: ReduceParser<StringEncodingParser, u64> =
            ReduceParser::new(member_count, 0, |acc, _: StringEncoding| acc + 1);
        Ok((input, Self {
            started: buffer.tell(),
            key,
            entrust,
        }))
    }
}

impl StateParser for SetRecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        let member_count = self.entrust.call(buffer)?;

        Ok(Item::SetRecord {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            encoding: SetEncoding::Raw,
            member_count,
        })
    }
}

pub struct SetIntSetRecordParser {
    started: u64,
    key: RDBStr,
    to_skip: u64,
    member_count: u64,
}

impl InitializableParser for SetIntSetRecordParser {
    fn init<'a>(buffer: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let (input, key) = read_rdb_str(input).context("read key")?;
        let (input, blob_len) = read_rdb_len(input).context("read intset blob len")?;
        let blob_len = blob_len
            .as_u64()
            .context("intset blob len should be a number")?;
        ensure!(
            blob_len >= 8,
            "intset blob should be at least 8 bytes (header)"
        );
        let (input, header) = read_exact(input, 8)?;
        let member_count = u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as u64;
        Ok((input, Self {
            started: buffer.tell(),
            key,
            to_skip: blob_len - 8,
            member_count,
        }))
    }
}

impl StateParser for SetIntSetRecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        crate::parser_trace!("intset.raw");

        skip_bytes(buffer, &mut self.to_skip)?;

        Ok(Item::SetRecord {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            encoding: SetEncoding::IntSet,
            member_count: self.member_count,
        })
    }
}

pub struct SetListPackRecordParser {
    started: u64,
    key: RDBStr,
    entrust: RDBStrBox<ListPackLengthParser>,
}

impl InitializableParser for SetListPackRecordParser {
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

impl StateParser for SetListPackRecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        let member_count = self.entrust.call(buffer)?;

        Ok(Item::SetRecord {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            encoding: SetEncoding::ListPack,
            member_count,
        })
    }
}

pub struct ListPackLengthParser {
    entrust: Option<IsEndListPackEntryParser>,
    counted: u64,
}

impl InitializableParser for ListPackLengthParser {
    fn init<'a>(_: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let (input, _header) = read_exact(input, 6).context("read listpack header")?;
        Ok((input, Self {
            entrust: None,
            counted: 0,
        }))
    }
}

impl StateParser for ListPackLengthParser {
    type Output = u64;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        loop {
            if let Some(parser) = self.entrust.as_mut() {
                if parser.call(buffer)? {
                    return Ok(self.counted);
                }
                self.entrust = None;
                self.counted += 1;
            }

            let (input, parser) = IsEndListPackEntryParser::init(buffer.tell(), buffer.as_ref())?;
            buffer.consume_to(input.as_ptr());
            self.entrust = Some(parser);
        }
    }
}

pub struct IsEndListPackEntryParser {
    started: u64,
    before_backlen: u64, // bytes to skip before backlen
    is_eol: bool,
}

impl IsEndListPackEntryParser {
    fn init(started: u64, input: &[u8]) -> AnyResult<(&[u8], Self)> {
        let (input, flag) = read_u8(input)?;

        let (input, to_skip) = match flag {
            0x80..=0xBF => Self::init_6bit_str(input, flag)?,
            0xE0..=0xEF => Self::init_12bit_str(input, flag)?,
            0xF0 => Self::init_32bit_str(input, flag)?,
            0x00..=0x7F => Self::init_7bit_uint(input)?,
            0xC0..=0xDF => Self::init_13bit_uint(input)?,
            0xF1 => Self::init_16bit_int(input)?,
            0xF2 => Self::init_24bit_int(input)?,
            0xF3 => Self::init_32bit_int(input)?,
            0xF4 => Self::init_64bit_int(input)?,
            0xFF => Self::init_eol(input)?,
            _ => bail!("unknown listpack entry flag: {:02x}", flag),
        };

        Ok((input, Self {
            started,
            before_backlen: to_skip,
            is_eol: flag == 0xFF,
        }))
    }

    /// flag: 0b10xxxxxx
    fn init_6bit_str(input: &[u8], flag: u8) -> AnyResult<(&[u8], u64)> {
        let len: u8 = flag & 0x3F;
        Ok((input, len as u64))
    }

    /// 12-bit string, 0b1110xxxx
    fn init_12bit_str(input: &[u8], flag: u8) -> AnyResult<(&[u8], u64)> {
        let first = flag & 0x0F;
        let (input, second) = read_u8(input)?;
        let len = u16::from_be_bytes([first, second]);
        Ok((input, len as u64))
    }

    /// 32-bit string, 0b11110000
    fn init_32bit_str(input: &[u8], _flag: u8) -> AnyResult<(&[u8], u64)> {
        let (input, len) = read_exact(input, 4)?;
        let len = u32::from_le_bytes([len[0], len[1], len[2], len[3]]);
        Ok((input, len as u64))
    }

    /// 7-bit unsigned int, 0b0xxxxxxx
    fn init_7bit_uint(input: &[u8]) -> AnyResult<(&[u8], u64)> {
        Ok((input, 0))
    }

    /// 13-bit unsigned int, 0b110xxxxx yyyyyyyy
    fn init_13bit_uint(input: &[u8]) -> AnyResult<(&[u8], u64)> {
        Ok((input, 1))
    }

    /// 16-bit signed int, 0b11110001
    fn init_16bit_int(input: &[u8]) -> AnyResult<(&[u8], u64)> {
        Ok((input, 2))
    }

    /// 24-bit signed int, 0b11110010
    fn init_24bit_int(input: &[u8]) -> AnyResult<(&[u8], u64)> {
        Ok((input, 3))
    }

    /// 32-bit signed int, 0b11110011
    fn init_32bit_int(input: &[u8]) -> AnyResult<(&[u8], u64)> {
        Ok((input, 4))
    }

    /// 64-bit signed int, 0b11110100
    fn init_64bit_int(input: &[u8]) -> AnyResult<(&[u8], u64)> {
        Ok((input, 8))
    }

    /// End of the listpack, 0b11111111
    fn init_eol(input: &[u8]) -> AnyResult<(&[u8], u64)> {
        Ok((input, 0))
    }
}

impl StateParser for IsEndListPackEntryParser {
    type Output = bool;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        skip_bytes(buffer, &mut self.before_backlen)?;
        if self.is_eol {
            return Ok(true);
        }

        let mut offset = buffer.tell() - self.started;
        let mut to_skip = 0;
        while offset != 0 {
            offset >>= 7;
            to_skip += 1;
        }

        let (input, _) = read_exact(buffer.as_ref(), to_skip)?;
        buffer.consume_to(input.as_ptr());

        Ok(self.is_eol)
    }
}
