use anyhow::{Context, ensure};

use super::{
    buffer::Buffer,
    item::{HashEncoding, Item},
    record_list::ZipListLengthParser,
    record_set::ListPackLengthParser,
    record_string::StringEncodingParser,
    state_parser::StateParser,
};
use crate::{
    helper::AnyResult,
    parser::{
        combinators::read_exact,
        rdb_parsers::{RDBStr, read_rdb_len, read_rdb_str},
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
            .as_simple()
            .context("hash length should be simple number")?;
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
            let (input, entrust) = StringEncodingParser::init(buffer.as_ref())?;
            buffer.consume_to(input.as_ptr());
            self.entrust = Some(entrust);
        }
    }
}

// ------------------------- ZipList Encoding (id = 13) ------------------------

pub struct HashZipListRecordParser {
    started: u64,
    key: RDBStr,
    entrust: ZipListLengthParser,
}

impl HashZipListRecordParser {
    pub fn init(started: u64, input: &[u8]) -> AnyResult<(&[u8], Self)> {
        let (input, key) = read_rdb_str(input).context("read key")?;
        let (input, entrust) = ZipListLengthParser::init(input).context("init ziplist parser")?;
        Ok((input, Self {
            started,
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
    /// Either a parser for raw listpack, or None if count pre-computed (e.g., LZF compressed).
    entrust: Option<ListPackLengthParser>,
    precomputed_count: Option<u64>,
}

impl HashListPackRecordParser {
    pub fn init(started: u64, input: &[u8]) -> AnyResult<(&[u8], Self)> {
        use crate::parser::rdb_parsers::RDBLen;

        let (mut input, key) = read_rdb_str(input).context("read key")?;

        // Read blob length indicator (raw or LZF).
        let (after_len, blob_len_enc) = read_rdb_len(input).context("read listpack blob len")?;

        match blob_len_enc {
            RDBLen::Simple(raw_len) | RDBLen::IntStr(raw_len) => {
                ensure!(raw_len >= 6, "listpack blob should be at least 6 bytes");
                let (after_blob, entrust) = ListPackLengthParser::init(after_len, raw_len)?;
                input = after_blob;
                Ok((input, Self {
                    started,
                    key,
                    entrust: Some(entrust),
                    precomputed_count: None,
                }))
            }
            RDBLen::LZFStr => {
                // LZF header: <in_len> <out_len> <compressed payload>
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
                    "decompressed listpack should be at least 6 bytes"
                );

                let (after_payload, compressed_raw) =
                    read_exact(input_l2, in_len as usize).context("read compressed data")?;
                let decompressed = lzf::decompress(compressed_raw, out_len as usize)
                    .map_err(|e| anyhow::anyhow!("decompress listpack blob: {e}"))?;

                // Count entries.
                let field_count = {
                    let mut dec_buf = Buffer::new(decompressed.len());
                    dec_buf.extend(&decompressed)?;
                    let dec_input = dec_buf.as_ref();
                    let (dec_input, mut parser) = ListPackLengthParser::init(dec_input, out_len)?;
                    dec_buf.consume_to(dec_input.as_ptr());
                    let entry_count = parser.call(&mut dec_buf)?;
                    ensure!(
                        dec_buf.is_empty(),
                        "decompressed listpack buffer not fully consumed"
                    );
                    ensure!(
                        entry_count % 2 == 0,
                        "listpack entry count should be even for hash"
                    );
                    entry_count / 2
                };

                Ok((after_payload, Self {
                    started,
                    key,
                    entrust: None,
                    precomputed_count: Some(field_count),
                }))
            }
        }
    }
}

impl StateParser for HashListPackRecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        let field_count = if let Some(parser) = self.entrust.as_mut() {
            let entry_count = parser.call(buffer)?;
            ensure!(
                entry_count % 2 == 0,
                "listpack entry count should be even for hash"
            );
            entry_count / 2
        } else if let Some(count) = self.precomputed_count {
            count
        } else {
            unreachable!("HashListPackRecordParser: missing parser and precomputed count")
        };

        Ok(Item::HashRecord {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            encoding: HashEncoding::ListPack,
            field_count,
        })
    }
}

// ------------------------- ZipMap Encoding (id = 9) --------------------------

pub struct HashZipMapRecordParser {
    started: u64,
    key: RDBStr,
    field_count: u64,
}

impl HashZipMapRecordParser {
    pub fn init(started: u64, input: &[u8]) -> AnyResult<(&[u8], Self)> {
        use crate::parser::rdb_parsers::RDBLen;
        let (input, key) = read_rdb_str(input).context("read key")?;

        // Read blob length indicator (raw or LZF string).
        let (after_len, blob_len_enc) = read_rdb_len(input).context("read zipmap blob len")?;

        let (after_blob, field_count) = match blob_len_enc {
            RDBLen::Simple(raw_len) | RDBLen::IntStr(raw_len) => {
                let (after_blob, raw_bytes) =
                    read_exact(after_len, raw_len as usize).context("read zipmap raw bytes")?;
                let count = count_zipmap_pairs(raw_bytes)?;
                (after_blob, count)
            }
            RDBLen::LZFStr => {
                // LZF header: <in_len> <out_len> <compressed payload>
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

                let (after_payload, compressed_raw) =
                    read_exact(input_l2, in_len as usize).context("read compressed payload")?;
                let decompressed = lzf::decompress(compressed_raw, out_len as usize)
                    .map_err(|e| anyhow::anyhow!("decompress zipmap blob: {e}"))?;
                let count = count_zipmap_pairs(&decompressed)?;
                (after_payload, count)
            }
        };

        // Trace point for ZipMap raw path (regardless of compression)
        crate::parser_trace!("hash.zipmap.raw");

        Ok((after_blob, Self {
            started,
            key,
            field_count,
        }))
    }
}

impl StateParser for HashZipMapRecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        // All blob bytes were consumed during init, nothing to drive here.
        Ok(Item::HashRecord {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            encoding: HashEncoding::ZipMap,
            field_count: self.field_count,
        })
    }
}

// --------------------------- ZipMap helper -----------------------------------

fn read_zipmap_len(input: &[u8]) -> AnyResult<(&[u8], usize)> {
    use crate::parser::combinators::read_u8;
    let (input, len_byte) = read_u8(input)?;
    if len_byte < 254 {
        Ok((input, len_byte as usize))
    } else {
        // 4-byte big endian length follows.
        use crate::parser::combinators::read_exact as read_exact_bytes;
        let (input, len_bytes) = read_exact_bytes(input, 4)?;
        let len =
            u32::from_be_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as usize;
        Ok((input, len))
    }
}

fn count_zipmap_pairs(bytes: &[u8]) -> AnyResult<u64> {
    use crate::parser::combinators::read_u8;

    if bytes.is_empty() {
        return Ok(0);
    }

    // Skip zmlen (overall length/pair count hint).
    let (mut input, _) = read_u8(bytes)?;
    let mut pair_count = 0u64;

    loop {
        // Read key length or end marker 0xFF.
        let (inp, key_len_byte) = read_u8(input)?;
        input = inp;
        if key_len_byte == 255 {
            break;
        }

        // Decode key length.
        let (inp, key_len) = if key_len_byte < 254 {
            (input, key_len_byte as usize)
        } else {
            read_zipmap_len(input)?
        };
        input = inp;

        // Skip key bytes.
        ensure!(input.len() >= key_len, "zipmap truncated key bytes");
        input = &input[key_len..];

        // Value length.
        let (inp, val_len_byte) = read_u8(input)?;
        input = inp;
        let (inp, val_len) = if val_len_byte < 254 {
            (input, val_len_byte as usize)
        } else {
            read_zipmap_len(input)?
        };
        input = inp;

        // Free bytes after value.
        let (inp, free_byte) = read_u8(input)?;
        input = inp;

        // Skip value bytes plus free bytes.
        ensure!(
            input.len() >= val_len + free_byte as usize,
            "zipmap truncated value bytes"
        );
        input = &input[val_len + free_byte as usize..];

        pair_count += 1;
    }

    Ok(pair_count)
}
