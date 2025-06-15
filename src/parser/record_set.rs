use anyhow::{Context, bail, ensure};

use super::{
    buffer::{Buffer, skip_bytes},
    item::{Item, SetEncoding},
    record_string::StringEncodingParser,
    state_parser::StateParser,
};
use crate::{
    helper::AnyResult,
    parser::{
        combinators::{read_exact, read_u8},
        rdb_parsers::{RDBStr, read_rdb_len, read_rdb_str},
    },
};

pub struct SetRecordParser {
    started: u64,
    key: RDBStr,
    member_count: u64,
    remain: u64,
    entrust: Option<StringEncodingParser>,
}

impl SetRecordParser {
    pub fn init(started: u64, input: &[u8]) -> AnyResult<(&[u8], Self)> {
        let (input, key) = read_rdb_str(input).context("read key")?;
        let (input, member_count) = read_rdb_len(input).context("read set length")?;
        let member_count = member_count
            .as_simple()
            .context("set length should be a simple number")?;
        Ok((input, Self {
            started,
            key,
            member_count,
            remain: member_count,
            entrust: None,
        }))
    }
}

impl StateParser for SetRecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        loop {
            if let Some(parser) = self.entrust.as_mut() {
                let _ = parser.call(buffer)?;
                self.entrust = None;
                self.remain -= 1;
            }

            if self.remain == 0 {
                break;
            }

            let (input, parser) = StringEncodingParser::init(buffer.as_ref())?;
            buffer.consume_to(input.as_ptr());
            self.entrust = Some(parser);
        }

        Ok(Item::SetRecord {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            encoding: SetEncoding::Raw,
            member_count: self.member_count,
        })
    }
}

pub struct SetIntSetRecordParser {
    started: u64,
    key: RDBStr,
    to_skip: u64,
    header_read: bool,
    member_count: u64,
}

impl SetIntSetRecordParser {
    pub fn init(started: u64, input: &[u8]) -> AnyResult<(&[u8], Self)> {
        let (input, key) = read_rdb_str(input).context("read key")?;
        let (input, blob_len) = read_rdb_len(input).context("read intset blob len")?;
        // TODO: add test case when blob_len is not a simple number
        let blob_len = blob_len
            .as_simple()
            .context("intset blob len should be a simple number")?;
        ensure!(
            blob_len >= 8,
            "intset blob should be at least 8 bytes (header)"
        );
        Ok((input, Self {
            started,
            key,
            to_skip: blob_len,
            header_read: false,
            member_count: 0,
        }))
    }
}

impl StateParser for SetIntSetRecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        if !self.header_read {
            crate::parser_trace!("intset.raw");
            let (_input, header) = read_exact(buffer.as_ref(), 8)?;
            // bytes 0..=3: encoding (unused), 4..=7: length (little endian u32)
            self.member_count =
                u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as u64;
            self.header_read = true;
        }

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
    /// If `Some`, a raw listpack parser to count members.
    entrust: Option<ListPackLengthParser>,
    /// If `Some`, the member count was pre-computed (e.g., for LZF-compressed blobs).
    precomputed_count: Option<u64>,
}

impl SetListPackRecordParser {
    pub fn init(started: u64, input: &[u8]) -> AnyResult<(&[u8], Self)> {
        use crate::parser::rdb_parsers::RDBLen;

        let (mut input, key) = read_rdb_str(input).context("read key")?;

        // Read the blob length indicator – could be raw (Simple / IntStr) or LZF-encoded.
        let (after_len, blob_len_enc) = read_rdb_len(input).context("read listpack blob len")?;

        match blob_len_enc {
            RDBLen::Simple(raw_len) | RDBLen::IntStr(raw_len) => {
                ensure!(
                    raw_len >= 6,
                    "listpack blob should be at least 6 bytes (header)"
                );
                let (after_blob_len, entrust) = ListPackLengthParser::init(after_len, raw_len)?;
                input = after_blob_len;

                Ok((input, Self {
                    started,
                    key,
                    entrust: Some(entrust),
                    precomputed_count: None,
                }))
            }
            RDBLen::LZFStr => {
                // ----------------- LZF compressed listpack -----------------
                // <in_len> <out_len> <compressed payload>

                let (input_l1, in_len_enc) =
                    read_rdb_len(after_len).context("read compressed in_len")?;
                let in_len = in_len_enc
                    .as_simple()
                    .context("compressed in_len must be simple")?;

                let (input_l2, out_len_enc) =
                    read_rdb_len(input_l1).context("read compressed out_len")?;
                let out_len = out_len_enc
                    .as_simple()
                    .context("compressed out_len must be simple")?;

                ensure!(
                    out_len >= 6,
                    "decompressed listpack should be at least 6 bytes (header)"
                );

                // Read compressed payload.
                let (after_payload, compressed_raw) =
                    read_exact(input_l2, in_len as usize).context("read compressed data")?;

                // Clone into Vec to break borrow to `buffer`.
                let compressed_vec = compressed_raw.to_vec();

                // Perform decompression.
                let decompressed = lzf::decompress(&compressed_vec, out_len as usize)
                    .map_err(|e| anyhow::anyhow!("decompress listpack blob: {e}"))?;

                // Count elements by running an in-memory parser.
                let member_count = {
                    let mut dec_buf = Buffer::new(decompressed.len());
                    dec_buf.extend(&decompressed)?;
                    let dec_input = dec_buf.as_ref();
                    let (dec_input, mut lp_parser) =
                        ListPackLengthParser::init(dec_input, out_len)?;
                    dec_buf.consume_to(dec_input.as_ptr());
                    let count = lp_parser.call(&mut dec_buf)?;
                    ensure!(
                        dec_buf.is_empty(),
                        "decompressed listpack buffer should be fully consumed after parsing"
                    );
                    count
                };

                // Return parser with pre-computed count; parsing is finished.
                Ok((after_payload, Self {
                    started,
                    key,
                    entrust: None,
                    precomputed_count: Some(member_count),
                }))
            }
        }
    }
}

impl StateParser for SetListPackRecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        // Determine member count (either by delegating to the raw parser or returning the pre-computed value).
        let member_count = if let Some(parser) = self.entrust.as_mut() {
            parser.call(buffer)?
        } else if let Some(count) = self.precomputed_count {
            count
        } else {
            unreachable!("SetListPackRecordParser: neither entrust nor precomputed_count available")
        };

        Ok(Item::SetRecord {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            encoding: SetEncoding::ListPack,
            member_count,
        })
    }
}

pub enum ListPackLengthParser {
    Fast {
        to_skip: u64,
        member_count: u64,
    },
    Slow {
        entrust: Option<IsEndListPackEntryParser>,
        counted: u64,
    },
}

impl ListPackLengthParser {
    pub fn init(input: &[u8], raw_len: u64) -> AnyResult<(&[u8], Self)> {
        let (input, header) = read_exact(input, 6).context("read listpack header")?;
        let member_count = u16::from_le_bytes([header[4], header[5]]);

        if member_count != u16::MAX {
            Ok((input, Self::Fast {
                to_skip: raw_len - 6,
                member_count: member_count as u64,
            }))
        } else {
            Ok((input, Self::Slow {
                entrust: None,
                counted: 0,
            }))
        }
    }
}

impl StateParser for ListPackLengthParser {
    type Output = u64;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        match self {
            Self::Fast {
                to_skip,
                member_count,
            } => {
                skip_bytes(buffer, to_skip)?;
                Ok(*member_count)
            }
            Self::Slow { entrust, counted } => loop {
                if let Some(parser) = entrust.as_mut() {
                    if parser.call(buffer)? {
                        return Ok(*counted);
                    }
                    *entrust = None;
                    *counted += 1;
                }

                let (input, parser) =
                    IsEndListPackEntryParser::init(buffer.tell(), buffer.as_ref())?;
                buffer.consume_to(input.as_ptr());
                *entrust = Some(parser);
            },
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
