use anyhow::{Context, ensure};

use crate::{
    helper::AnyResult,
    parser::{
        core::{
            buffer::{Buffer, skip_bytes},
            combinators::{read_be_u32, read_exact, read_u8},
            raw::{RDBLen, RDBStr, read_rdb_len, read_rdb_str},
        },
        model::{Item, ListEncoding, StringEncoding},
        record::{set::ListPackLengthParser, string::StringEncodingParser},
        state::{
            combinators::ReduceParser,
            traits::{InitializableParser, StateParser},
        },
    },
};

pub struct ListRecordParser {
    started: u64,
    key: RDBStr,
    entrust: ReduceParser<StringEncodingParser, u64, fn(u64, StringEncoding) -> u64>,
}

impl ListRecordParser {
    pub fn init(started: u64, input: &[u8]) -> AnyResult<(&[u8], Self)> {
        let (input, key) = read_rdb_str(input).context("read key")?;
        let (input, member_count) = read_rdb_len(input).context("read list length")?;
        let member_count = member_count
            .as_u64()
            .context("list length should be a number")?;
        let entrust: ReduceParser<StringEncodingParser, u64, fn(u64, StringEncoding) -> u64> =
            ReduceParser::new(member_count, 0, |acc, _item: StringEncoding| acc + 1);
        Ok((input, Self {
            started,
            key,
            entrust,
        }))
    }
}

impl StateParser for ListRecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        let member_count = self.entrust.call(buffer)?;
        Ok(Item::ListRecord {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            encoding: ListEncoding::List,
            member_count,
        })
    }
}

pub struct ListZipListRecordParser {
    entrust: ZipListLengthParser,
    key: RDBStr,
    started: u64,
}

impl ListZipListRecordParser {
    pub fn init(started: u64, input: &[u8]) -> AnyResult<(&[u8], Self)> {
        let (input, key) = read_rdb_str(input).context("read key")?;
        let (input, entrust) = ZipListLengthParser::init(input)?;
        Ok((input, Self {
            entrust,
            key,
            started,
        }))
    }
}

impl StateParser for ListZipListRecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        let member_count = self.entrust.call(buffer)?;
        Ok(Item::ListRecord {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            encoding: ListEncoding::ZipList,
            member_count,
        })
    }
}

pub enum ZipListLengthParser {
    Fast {
        to_skip: u64,
        member_count: u64,
    },
    Slow {
        entrust: Option<IsEndZipListEntryParser>,
        counted: u64,
    },
}

impl ZipListLengthParser {
    pub fn init(input: &[u8]) -> AnyResult<(&[u8], Self)> {
        let (input, raw_len) = read_rdb_len(input).context("read ziplist len")?;
        match raw_len {
            RDBLen::Simple(raw_len) => Self::raw_init(input, raw_len),
            RDBLen::LZFStr => Self::lzf_init(input),
            _ => anyhow::bail!("ziplist len should be simple or lzf"),
        }
    }

    fn raw_init(input: &[u8], raw_len: u64) -> AnyResult<(&[u8], Self)> {
        crate::parser_trace!("quicklist.ziplist.raw");
        let (input, header) = read_exact(input, 10).context("read ziplist header")?;
        let member_count = u16::from_le_bytes([header[8], header[9]]);
        if member_count != u16::MAX {
            Ok((input, Self::Fast {
                to_skip: raw_len - 10,
                member_count: member_count as u64,
            }))
        } else {
            Ok((input, Self::Slow {
                entrust: None,
                counted: 0,
            }))
        }
    }

    fn lzf_init(input: &[u8]) -> AnyResult<(&[u8], Self)> {
        crate::parser_trace!("quicklist.ziplist.lzf");
        let (input, in_len) = read_rdb_len(input).context("read compressed in_len")?;
        let in_len = in_len
            .as_u64()
            .context("compressed in_len must be a number")?;
        let (input, out_len) = read_rdb_len(input).context("read compressed out_len")?;
        let out_len = out_len
            .as_u64()
            .context("compressed out_len must be a number")?;

        ensure!(
            out_len < 4 * 1024 * 1024 * 1024,
            "compressed out_len must be <4GB"
        );

        let (input, compressed) =
            read_exact(input, in_len as usize).context("read compressed data")?;
        let decompressed = lzf::decompress(compressed, out_len as usize)
            .map_err(|e| anyhow::anyhow!("decompress quicklist node: {e}"))?;

        let mut decompressed_buffer = Buffer::new(decompressed.len());
        decompressed_buffer.extend(&decompressed)?;
        let decompress_input = decompressed_buffer.as_ref();

        let (decompress_input, mut entrust) = Self::raw_init(decompress_input, out_len)?;
        decompressed_buffer.consume_to(decompress_input.as_ptr());

        let member_count = entrust.call(&mut decompressed_buffer)?;
        ensure!(
            decompressed_buffer.is_empty(),
            "decompressed ziplist buffer should be empty after parsing"
        );

        Ok((input, Self::Fast {
            to_skip: 0,
            member_count,
        }))
    }
}

impl StateParser for ZipListLengthParser {
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

                let (input, parser) = IsEndZipListEntryParser::init(buffer.as_ref())?;
                buffer.consume_to(input.as_ptr());
                *entrust = Some(parser);
            },
        }
    }
}

pub struct IsEndZipListEntryParser {
    to_skip: u64,
    is_enl: bool,
}

impl IsEndZipListEntryParser {
    fn init(input: &[u8]) -> AnyResult<(&[u8], Self)> {
        let (mut input, prevlen_flag) = read_u8(input).context("read prevlen")?;
        if prevlen_flag == 0xFF {
            return Ok((input, Self {
                to_skip: 0,
                is_enl: true,
            }));
        }
        if prevlen_flag == 0xFE {
            let (rest, _) = read_exact(input, 4).context("skip prevlen4")?;
            input = rest;
        }

        let (input, to_skip) = Self::read_to_skip(input)?;

        Ok((input, Self {
            to_skip,
            is_enl: false,
        }))
    }

    fn read_to_skip(input: &[u8]) -> AnyResult<(&[u8], u64)> {
        let (input, encoding_byte) = read_u8(input).context("read encoding byte")?;
        let encoding_type = encoding_byte >> 6;
        match encoding_type {
            0 => {
                let content_len = encoding_byte & 0b0011_1111;
                Ok((input, content_len as u64))
            }
            1 => {
                let (input, low_byte) = read_u8(input).context("read second byte")?;
                let high_byte = encoding_byte & 0b0011_1111;
                let content_len = u64::from_be_bytes([0, 0, 0, 0, 0, 0, high_byte, low_byte]);
                Ok((input, content_len))
            }
            2 => {
                let (input, content_len) = read_be_u32(input)?;
                Ok((input, content_len as u64))
            }
            3 => {
                let to_skip = match encoding_byte {
                    0b1100_0000 => 2,
                    0b1101_0000 => 4,
                    0b1110_0000 => 8,
                    0b1111_0000 => 3,
                    0b1111_1110 => 1,
                    _ => 0,
                };
                Ok((input, to_skip))
            }
            _ => unreachable!("invalid encoding type: {:#04x}", encoding_byte),
        }
    }
}

impl StateParser for IsEndZipListEntryParser {
    type Output = bool;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        skip_bytes(buffer, &mut self.to_skip)?;
        Ok(self.is_enl)
    }
}

pub struct ListQuickListRecordParser {
    started: u64,
    key: RDBStr,
    entrust: QuickListLengthParser,
}

impl ListQuickListRecordParser {
    pub fn init(started: u64, input: &[u8]) -> AnyResult<(&[u8], Self)> {
        let (input, key) = read_rdb_str(input).context("read key")?;
        let (input, entrust) = QuickListLengthParser::init(input)?;
        Ok((input, Self {
            started,
            key,
            entrust,
        }))
    }
}

impl StateParser for ListQuickListRecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        let member_count = self.entrust.call(buffer)?;
        Ok(Item::ListRecord {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            encoding: ListEncoding::QuickList,
            member_count,
        })
    }
}

struct QuickListLengthParser {
    nodes_remain: u64,
    count: u64,
    entrust: Option<ZipListLengthParser>,
}

impl QuickListLengthParser {
    fn init(input: &[u8]) -> AnyResult<(&[u8], Self)> {
        let (input, len) = read_rdb_len(input).context("read quicklist len")?;
        let len = len.as_u64().context("quicklist len should be a number")?;
        Ok((input, Self {
            nodes_remain: len,
            count: 0,
            entrust: None,
        }))
    }
}

impl StateParser for QuickListLengthParser {
    type Output = u64;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        loop {
            if let Some(parser) = self.entrust.as_mut() {
                self.count += parser.call(buffer)?;
                self.nodes_remain -= 1;
                self.entrust = None;
            }
            if self.nodes_remain == 0 {
                break;
            }
            let (input, entrust) = ZipListLengthParser::init(buffer.as_ref())?;
            buffer.consume_to(input.as_ptr());
            self.entrust = Some(entrust);
        }
        Ok(self.count)
    }
}

// ---------------------- QuickList2 (Redis 7.0+) ----------------------

/// Parser for `RDB_TYPE_LIST_QUICKLIST_2` records (Redis ≥ 7.0).
pub struct ListQuickList2RecordParser {
    started: u64,
    key: RDBStr,
    entrust: QuickList2LengthParser,
}

impl ListQuickList2RecordParser {
    pub fn init(started: u64, input: &[u8]) -> AnyResult<(&[u8], Self)> {
        let (input, key) = read_rdb_str(input).context("read key")?;
        let (input, entrust) = QuickList2LengthParser::init(input)?;
        Ok((input, Self {
            started,
            key,
            entrust,
        }))
    }
}

impl StateParser for ListQuickList2RecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        let member_count = self.entrust.call(buffer)?;
        Ok(Item::ListRecord {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            encoding: ListEncoding::QuickList2,
            member_count,
        })
    }
}

// --------------------------- node counter ---------------------------

enum QuickList2NodeParser {
    Plain(StringEncodingParser),
    Packed(ListPackLengthParser),
}

impl StateParser for QuickList2NodeParser {
    type Output = u64; // element count produced by this node

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        match self {
            Self::Plain(parser) => {
                crate::parser_trace!("quicklist2.plain");
                let _ = parser.call(buffer)?;
                Ok(1)
            }
            Self::Packed(parser) => {
                crate::parser_trace!("quicklist2.packed.raw");
                parser.call(buffer)
            }
        }
    }
}

struct QuickList2LengthParser {
    nodes_remain: u64,
    member_count: u64,
    entrust: Option<QuickList2NodeParser>,
}

impl QuickList2LengthParser {
    fn init(input: &[u8]) -> AnyResult<(&[u8], Self)> {
        let (input, nodes_count) = read_rdb_len(input).context("read quicklist node count")?;
        let nodes_count = nodes_count
            .as_u64()
            .context("quicklist node count should be a number")?;
        Ok((input, Self {
            nodes_remain: nodes_count,
            member_count: 0,
            entrust: None,
        }))
    }
}

impl StateParser for QuickList2LengthParser {
    type Output = u64;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        loop {
            // If we are currently delegated to a node parser, drive it first.
            if let Some(parser) = self.entrust.as_mut() {
                self.member_count += parser.call(buffer)?;
                self.nodes_remain -= 1;
                self.entrust = None;
            }

            if self.nodes_remain == 0 {
                return Ok(self.member_count);
            }

            let input = buffer.as_ref();
            let (input, container_len) =
                read_rdb_len(input).context("read quicklist2 container flag")?;
            let container = container_len
                .as_u64()
                .context("quicklist2 container flag should be a number")?;

            // Build appropriate parser for the node.
            let (input_after_flag, node_parser) = match container {
                1 => {
                    let (input, entrust) = StringEncodingParser::init(buffer, input)?;
                    (input, QuickList2NodeParser::Plain(entrust))
                }
                2 => {
                    use crate::parser::core::raw::RDBLen;
                    // First, read the blob length indicator (may be simple, intstr or LZFStr).
                    let (mut input_after_len, blob_len_enc) =
                        read_rdb_len(input).context("read listpack blob len")?;

                    match blob_len_enc {
                        RDBLen::Simple(raw_len) | RDBLen::IntStr(raw_len) => {
                            ensure!(
                                raw_len >= 6,
                                "listpack blob should be at least 6 bytes (header)"
                            );
                            let (input, entrust) =
                                ListPackLengthParser::init(buffer, input_after_len)?;
                            input_after_len = input;
                            (input_after_len, QuickList2NodeParser::Packed(entrust))
                        }
                        RDBLen::LZFStr => {
                            crate::parser_trace!("quicklist2.packed.lzf");
                            // ----------------- LZF compressed listpack -----------------
                            // <in_len> <out_len> <compressed payload>
                            let (end_ptr, node_count) = {
                                // Borrow input slice immutably within this block.
                                let (input_l1, in_len_enc) = read_rdb_len(input_after_len)
                                    .context("read compressed in_len")?;
                                let in_len = in_len_enc
                                    .as_u64()
                                    .context("compressed in_len must be a number")?;

                                let (input_l2, out_len_enc) =
                                    read_rdb_len(input_l1).context("read compressed out_len")?;
                                let out_len = out_len_enc
                                    .as_u64()
                                    .context("compressed out_len must be a number")?;

                                ensure!(
                                    out_len >= 6,
                                    "decompressed listpack should be at least 6 bytes (header)"
                                );

                                // Read compressed payload.
                                let (after_payload, compressed_raw) =
                                    read_exact(input_l2, in_len as usize)
                                        .context("read compressed data")?;

                                // Clone into Vec to break borrow to `buffer`.
                                let compressed_vec = compressed_raw.to_vec();

                                // Perform decompression.
                                let decompressed =
                                    lzf::decompress(&compressed_vec, out_len as usize).map_err(
                                        |e| anyhow::anyhow!("decompress listpack node: {e}"),
                                    )?;

                                // Count elements.
                                let mut dec_buf = Buffer::new(decompressed.len());
                                dec_buf.extend(&decompressed)?;

                                let (dec_input, mut lp_parser) =
                                    ListPackLengthParser::init(&dec_buf, dec_buf.as_ref())?;
                                dec_buf.consume_to(dec_input.as_ptr());
                                let count = lp_parser.call(&mut dec_buf)?;
                                ensure!(
                                    dec_buf.is_empty(),
                                    "decompressed listpack buffer should be fully consumed after parsing"
                                );

                                (after_payload.as_ptr(), count)
                            };

                            // Mutably advance buffer after immutable borrows are dropped.
                            buffer.consume_to(end_ptr);

                            // Update counters.
                            self.member_count += node_count;
                            self.nodes_remain -= 1;

                            // Continue to next iteration, skipping the standard entrust path.
                            continue;
                        }
                    }
                }
                _ => anyhow::bail!("unknown quicklist2 container type: {}", container),
            };

            // Advance buffer to the position returned by above parser creation.
            buffer.consume_to(input_after_flag.as_ptr());
            self.entrust = Some(node_parser);
        }
    }
}
