use anyhow::{Context, ensure};

use crate::{
    helper::AnyResult,
    parser::{
        core::{
            buffer::Buffer,
            combinators::read_exact,
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

// ------------------------- ZipMap Encoding (id = 9) --------------------------

pub struct HashZipMapRecordParser {
    started: u64,
    key: RDBStr,
    field_count: u64,
}

impl HashZipMapRecordParser {
    pub fn init(started: u64, input: &[u8]) -> AnyResult<(&[u8], Self)> {
        use crate::parser::core::raw::RDBLen;

        let (mut input, key) = read_rdb_str(input).context("read key")?;
        let (after_len, blob_len_enc) = read_rdb_len(input).context("read zipmap blob len")?;

        let (field_count, after_blob) = match blob_len_enc {
            RDBLen::Simple(raw_len) | RDBLen::IntStr(raw_len) => {
                let (after_blob, blob) = read_exact(after_len, raw_len as usize)?;
                let count = count_zipmap_pairs(blob)?;
                (count, after_blob)
            }
            RDBLen::LZFStr => {
                let (input_l1, in_len_enc) =
                    read_rdb_len(after_len).context("read compressed in_len")?;
                let in_len = in_len_enc
                    .as_u64()
                    .context("compressed in_len must be a number")?;

                let (input_l2, out_len_enc) =
                    read_rdb_len(input_l1).context("read compressed out_len")?;
                let out_len = out_len_enc
                    .as_u64()
                    .context("compressed out_len must be a number")?;

                let (after_payload, compressed_raw) =
                    read_exact(input_l2, in_len as usize).context("read compressed payload")?;
                let decompressed = lzf::decompress(compressed_raw, out_len as usize)
                    .map_err(|e| anyhow::anyhow!("decompress zipmap blob: {}", e))?;
                let count = count_zipmap_pairs(&decompressed)?;
                (count, after_payload)
            }
        };

        crate::parser_trace!("hash.zipmap.raw");

        input = after_blob;
        Ok((input, Self {
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
    use crate::parser::core::combinators::read_u8;
    let (input, len_byte) = read_u8(input)?;
    if len_byte < 254 {
        Ok((input, len_byte as usize))
    } else {
        // 4-byte big endian length follows.
        use crate::parser::core::combinators::read_exact as read_exact_bytes;
        let (input, len_bytes) = read_exact_bytes(input, 4)?;
        let len =
            u32::from_be_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as usize;
        Ok((input, len))
    }
}

fn count_zipmap_pairs(bytes: &[u8]) -> AnyResult<u64> {
    use crate::parser::core::combinators::read_u8;

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
