use anyhow::{Context, ensure};

use super::{
    buffer::Buffer,
    item::{Item, ZSetEncoding},
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

pub struct ZSetRecordParser {
    started: u64,
    key: RDBStr,
    member_count: u64,
    remain: u64,
    entrust: Option<ZSetSkipListEntryParser>,
}

impl ZSetRecordParser {
    pub fn init(started: u64, input: &[u8]) -> AnyResult<(&[u8], Self)> {
        let (input, key) = read_rdb_str(input).context("read key")?;
        let (input, member_count) = read_rdb_len(input).context("read zset length")?;
        let member_count = member_count
            .as_simple()
            .context("zset length should be a simple number")?;

        // Trace point.
        crate::parser_trace!("zset.skiplist");

        Ok((input, Self {
            started,
            key,
            member_count,
            remain: member_count,
            entrust: None,
        }))
    }
}

impl StateParser for ZSetRecordParser {
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

            let (input, entrust) = ZSetSkipListEntryParser::init(buffer.as_ref())?;
            buffer.consume_to(input.as_ptr());
            self.entrust = Some(entrust);
        }

        Ok(Item::ZSetRecord {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            encoding: ZSetEncoding::SkipList,
            member_count: self.member_count,
        })
    }
}

pub struct ZSetZipListRecordParser {
    started: u64,
    key: RDBStr,
    entrust: ZipListLengthParser,
}

impl ZSetZipListRecordParser {
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

impl StateParser for ZSetZipListRecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        let entry_count = self.entrust.call(buffer)?;
        ensure!(
            entry_count % 2 == 0,
            "zset ziplist entry count should be even"
        );
        let member_count = entry_count / 2;

        Ok(Item::ZSetRecord {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            encoding: ZSetEncoding::ZipList,
            member_count,
        })
    }
}

struct ZSetSkipListEntryParser {
    remain: u8, // 2 components: member and score
    entrust: Option<StringEncodingParser>,
}

impl ZSetSkipListEntryParser {
    fn init(input: &[u8]) -> AnyResult<(&[u8], Self)> {
        Ok((input, Self {
            remain: 2,
            entrust: None,
        }))
    }
}

impl StateParser for ZSetSkipListEntryParser {
    type Output = ();

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        loop {
            if let Some(entrust) = self.entrust.as_mut() {
                let _ = entrust.call(buffer)?;
                self.remain -= 1;
                self.entrust = None;
            }
            if self.remain == 0 {
                return Ok(());
            }
            let input = buffer.as_ref();
            let (input, entrust) = StringEncodingParser::init(input)?;
            buffer.consume_to(input.as_ptr());
            self.entrust = Some(entrust);
        }
    }
}

pub struct ZSetListPackRecordParser {
    started: u64,
    key: RDBStr,
    entrust: Option<ListPackLengthParser>,
    precomputed_count: Option<u64>,
}

impl ZSetListPackRecordParser {
    pub fn init(started: u64, input: &[u8]) -> AnyResult<(&[u8], Self)> {
        use crate::parser::rdb_parsers::RDBLen;

        let (mut input, key) = read_rdb_str(input).context("read key")?;

        // The blob length indicator – raw (Simple / IntStr) or LZF encoded.
        let (after_len, blob_len_enc) = read_rdb_len(input).context("read listpack blob len")?;

        match blob_len_enc {
            RDBLen::Simple(raw_len) | RDBLen::IntStr(raw_len) => {
                crate::parser_trace!("zset.listpack.raw");
                ensure!(
                    raw_len >= 6,
                    "listpack blob should be at least 6 bytes (header)"
                );

                let (after_blob, entrust) = ListPackLengthParser::init(after_len, raw_len)
                    .context("init listpack parser")?;
                input = after_blob;

                Ok((input, Self {
                    started,
                    key,
                    entrust: Some(entrust),
                    precomputed_count: None,
                }))
            }
            RDBLen::LZFStr => {
                crate::parser_trace!("zset.listpack.lzf");
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

                let (after_payload, compressed_raw) =
                    read_exact(input_l2, in_len as usize).context("read compressed data")?;

                let compressed_vec = compressed_raw.to_vec();

                let decompressed = lzf::decompress(&compressed_vec, out_len as usize)
                    .map_err(|e| anyhow::anyhow!("decompress listpack blob: {e}"))?;

                let member_count = {
                    let mut dec_buf = Buffer::new(decompressed.len());
                    dec_buf.extend(&decompressed)?;
                    let dec_input = dec_buf.as_ref();
                    let (dec_input_after, mut lp_parser) =
                        ListPackLengthParser::init(dec_input, out_len)
                            .context("init listpack parser on decompressed")?;
                    dec_buf.consume_to(dec_input_after.as_ptr());
                    let entry_cnt = lp_parser.call(&mut dec_buf)?;

                    ensure!(
                        entry_cnt % 2 == 0,
                        "zset listpack entry count should be even"
                    );
                    entry_cnt / 2
                };

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

impl StateParser for ZSetListPackRecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        // Determine member count.
        let member_count = if let Some(parser) = self.entrust.as_mut() {
            let entry_cnt = parser.call(buffer)?;
            ensure!(
                entry_cnt % 2 == 0,
                "zset listpack entry count should be even"
            );
            entry_cnt / 2
        } else if let Some(count) = self.precomputed_count {
            count
        } else {
            unreachable!("ZSetListPackRecordParser: neither entrust nor precomputed_count")
        };

        Ok(Item::ZSetRecord {
            key: self.key.clone(),
            rdb_size: buffer.tell() - self.started,
            encoding: ZSetEncoding::ListPack,
            member_count,
        })
    }
}
