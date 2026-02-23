use std::convert::TryFrom;

use anyhow::{Context, anyhow, ensure};
use bytes::Bytes;
use tracing::debug;

use crate::parser::{
    combinator::{
        choice::alt2,
        primitive::{byte, exact, le_u32, le_u64, read_rdb_len, read_rdb_str, tag},
        repeat::fold_n,
    },
    core::{
        cursor::Cursor,
        parse::{ParseError, ParseResult, fatal, need_more, ok},
        raw::RDBLen,
    },
    model::{
        HashEncoding, Item, ListEncoding, RDBModuleOpcode, RDBOpcode, RDBType, SetEncoding,
        StreamEncoding, StringEncoding, ZSetEncoding,
    },
    registry,
};

enum Flag {
    Opcode(RDBOpcode),
    Type(RDBType),
}

fn parse_err_to_any(err: ParseError) -> anyhow::Error {
    match err {
        ParseError::Recoverable(e) | ParseError::Fatal(e) => e,
    }
}

fn parse_flag(cursor: &mut Cursor<'_>, flag: u8) -> ParseResult<Flag> {
    alt2(
        cursor,
        |_| match registry::resolve_opcode(flag) {
            Some(opcode) => ok(Flag::Opcode(opcode)),
            None => ParseResult::Err(ParseError::recoverable(anyhow!("not opcode"))),
        },
        |_| match registry::resolve_type(flag) {
            Some(type_id) => ok(Flag::Type(type_id)),
            None => ParseResult::Err(ParseError::fatal(anyhow!("unknown flag: {flag:#04x}"))),
        },
    )
}

#[derive(Default)]
pub struct RDBFileParser {
    version: u64,
    current_db: Option<u64>,
    items_parsed: u64,
}

impl RDBFileParser {
    fn return_item(&mut self, item: Item) -> ParseResult<Option<Item>> {
        self.items_parsed += 1;
        ParseResult::Ok(Some(item))
    }

    fn parse_header(&mut self, cursor: &mut Cursor<'_>) -> ParseResult<()> {
        match tag(cursor, b"REDIS") {
            ParseResult::Ok(()) => {}
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        }
        let version = match exact(cursor, 4) {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        let version = std::str::from_utf8(version.as_slice()).context("version should be utf8");
        let version = match version {
            Ok(v) => v.parse().context("version should be a number"),
            Err(e) => Err(e),
        };
        let version: u64 = match version {
            Ok(v) => v,
            Err(e) => return fatal(e),
        };
        if version < 1 {
            return fatal(anyhow!("version should be >= 1"));
        }
        if version > 12 {
            return fatal(anyhow!("version should be <= 12"));
        }
        self.version = version;
        ok(())
    }

    fn parse_u64_len(&self, cursor: &mut Cursor<'_>, what: &'static str) -> ParseResult<u64> {
        let len = match read_rdb_len(cursor) {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        match len.as_u64() {
            Some(v) => ok(v),
            None => fatal(anyhow!("{what} should be a number")),
        }
    }

    fn parse_string_encoding(&self, cursor: &mut Cursor<'_>) -> ParseResult<StringEncoding> {
        let len = match read_rdb_len(cursor) {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        match len {
            RDBLen::Simple(raw_len) => {
                let raw_len = raw_len as usize;
                match exact(cursor, raw_len) {
                    ParseResult::Ok(_) => ok(StringEncoding::Raw),
                    ParseResult::NeedMore => need_more(),
                    ParseResult::Err(e) => ParseResult::Err(e),
                }
            }
            RDBLen::IntStr(_) => ok(StringEncoding::Int),
            RDBLen::LZFStr => {
                let in_len = match self.parse_u64_len(cursor, "lzf in_len") {
                    ParseResult::Ok(v) => v,
                    ParseResult::NeedMore => return need_more(),
                    ParseResult::Err(e) => return ParseResult::Err(e),
                };
                let _out_len = match self.parse_u64_len(cursor, "lzf out_len") {
                    ParseResult::Ok(v) => v,
                    ParseResult::NeedMore => return need_more(),
                    ParseResult::Err(e) => return ParseResult::Err(e),
                };
                match exact(cursor, in_len as usize) {
                    ParseResult::Ok(_) => ok(StringEncoding::LZF),
                    ParseResult::NeedMore => need_more(),
                    ParseResult::Err(e) => ParseResult::Err(e),
                }
            }
        }
    }

    fn parse_rdb_blob(
        &self,
        cursor: &mut Cursor<'_>,
        what: &'static str,
    ) -> ParseResult<(Bytes, StringEncoding)> {
        let len = match read_rdb_len(cursor) {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };

        match len {
            RDBLen::Simple(raw_len) => {
                let bytes = match exact(cursor, raw_len as usize) {
                    ParseResult::Ok(v) => v,
                    ParseResult::NeedMore => return need_more(),
                    ParseResult::Err(e) => return ParseResult::Err(e),
                };
                ok((
                    Bytes::copy_from_slice(bytes.as_slice()),
                    StringEncoding::Raw,
                ))
            }
            RDBLen::LZFStr => {
                let in_len = match self.parse_u64_len(cursor, "lzf in_len") {
                    ParseResult::Ok(v) => v,
                    ParseResult::NeedMore => return need_more(),
                    ParseResult::Err(e) => return ParseResult::Err(e),
                };
                let out_len = match self.parse_u64_len(cursor, "lzf out_len") {
                    ParseResult::Ok(v) => v,
                    ParseResult::NeedMore => return need_more(),
                    ParseResult::Err(e) => return ParseResult::Err(e),
                };
                let compressed = match exact(cursor, in_len as usize) {
                    ParseResult::Ok(v) => v,
                    ParseResult::NeedMore => return need_more(),
                    ParseResult::Err(e) => return ParseResult::Err(e),
                };
                match lzf::decompress(compressed.as_slice(), out_len as usize) {
                    Ok(v) => ok((Bytes::from(v), StringEncoding::LZF)),
                    Err(e) => fatal(anyhow!("failed to decompress {what}: {e}")),
                }
            }
            RDBLen::IntStr(_) => fatal(anyhow!("{what} should be encoded as raw/lzf string")),
        }
    }

    fn parse_string_record(&mut self, cursor: &mut Cursor<'_>) -> ParseResult<Option<Item>> {
        let started = cursor.offset();
        let key = match read_rdb_str(cursor) {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return ParseResult::NeedMore,
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        let encoding = match self.parse_string_encoding(cursor) {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return ParseResult::NeedMore,
            ParseResult::Err(e) => return ParseResult::Err(e),
        };

        let rdb_size = cursor.offset() - started;
        cursor.commit();
        self.return_item(Item::StringRecord {
            key,
            rdb_size,
            encoding,
        })
    }

    fn parse_raw_list_record(&mut self, cursor: &mut Cursor<'_>) -> ParseResult<Option<Item>> {
        let started = cursor.offset();
        let key = match read_rdb_str(cursor) {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        let member_count = match self.parse_u64_len(cursor, "list length") {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        let parse_members = fold_n(
            cursor,
            member_count as usize,
            (),
            |cur| self.parse_string_encoding(cur),
            |_, _| (),
        );
        match parse_members {
            ParseResult::Ok(()) => {}
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        }

        let rdb_size = cursor.offset() - started;
        cursor.commit();
        self.return_item(Item::ListRecord {
            key,
            rdb_size,
            encoding: ListEncoding::List,
            member_count,
        })
    }

    fn parse_list_ziplist_record(&mut self, cursor: &mut Cursor<'_>) -> ParseResult<Option<Item>> {
        let started = cursor.offset();
        let key = match read_rdb_str(cursor) {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        let (ziplist, _enc) = match self.parse_rdb_blob(cursor, "list ziplist") {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        let member_count = match count_ziplist_entries(&ziplist) {
            Ok(v) => v,
            Err(e) => return fatal(e),
        };

        let rdb_size = cursor.offset() - started;
        cursor.commit();
        self.return_item(Item::ListRecord {
            key,
            rdb_size,
            encoding: ListEncoding::ZipList,
            member_count,
        })
    }

    fn parse_list_quicklist_record(
        &mut self,
        cursor: &mut Cursor<'_>,
    ) -> ParseResult<Option<Item>> {
        let started = cursor.offset();
        let key = match read_rdb_str(cursor) {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        let node_count = match self.parse_u64_len(cursor, "quicklist node count") {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };

        let mut member_count = 0u64;
        for _ in 0..node_count {
            let (ziplist, enc) = match self.parse_rdb_blob(cursor, "quicklist ziplist") {
                ParseResult::Ok(v) => v,
                ParseResult::NeedMore => return need_more(),
                ParseResult::Err(e) => return ParseResult::Err(e),
            };
            if enc == StringEncoding::LZF {
                crate::parser_trace!("quicklist.ziplist.lzf");
            } else {
                crate::parser_trace!("quicklist.ziplist.raw");
            }
            let node_members = match count_ziplist_entries(&ziplist) {
                Ok(v) => v,
                Err(e) => return fatal(e),
            };
            member_count += node_members;
        }

        let rdb_size = cursor.offset() - started;
        cursor.commit();
        self.return_item(Item::ListRecord {
            key,
            rdb_size,
            encoding: ListEncoding::QuickList,
            member_count,
        })
    }

    fn parse_list_quicklist2_record(
        &mut self,
        cursor: &mut Cursor<'_>,
    ) -> ParseResult<Option<Item>> {
        let started = cursor.offset();
        let key = match read_rdb_str(cursor) {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        let node_count = match self.parse_u64_len(cursor, "quicklist2 node count") {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };

        let mut member_count = 0u64;
        for _ in 0..node_count {
            let flag = match byte(cursor) {
                ParseResult::Ok(v) => v,
                ParseResult::NeedMore => return need_more(),
                ParseResult::Err(e) => return ParseResult::Err(e),
            };
            match flag {
                1 => {
                    crate::parser_trace!("quicklist2.plain");
                    match self.parse_string_encoding(cursor) {
                        ParseResult::Ok(_) => member_count += 1,
                        ParseResult::NeedMore => return need_more(),
                        ParseResult::Err(e) => return ParseResult::Err(e),
                    }
                }
                2 => {
                    let (listpack, enc) =
                        match self.parse_rdb_blob(cursor, "quicklist2 packed listpack") {
                            ParseResult::Ok(v) => v,
                            ParseResult::NeedMore => return need_more(),
                            ParseResult::Err(e) => return ParseResult::Err(e),
                        };
                    if enc == StringEncoding::LZF {
                        crate::parser_trace!("quicklist2.packed.lzf");
                    } else {
                        crate::parser_trace!("quicklist2.packed.raw");
                    }
                    let node_members = match count_listpack_entries(&listpack) {
                        Ok(v) => v,
                        Err(e) => return fatal(e),
                    };
                    member_count += node_members;
                }
                _ => return fatal(anyhow!("unknown quicklist2 node flag: {flag:#04x}")),
            }
        }

        let rdb_size = cursor.offset() - started;
        cursor.commit();
        self.return_item(Item::ListRecord {
            key,
            rdb_size,
            encoding: ListEncoding::QuickList2,
            member_count,
        })
    }

    fn parse_set_record(&mut self, cursor: &mut Cursor<'_>) -> ParseResult<Option<Item>> {
        let started = cursor.offset();
        let key = match read_rdb_str(cursor) {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        let member_count = match self.parse_u64_len(cursor, "set length") {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        let parse_members = fold_n(
            cursor,
            member_count as usize,
            (),
            |cur| self.parse_string_encoding(cur),
            |_, _| (),
        );
        match parse_members {
            ParseResult::Ok(()) => {}
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        }

        let rdb_size = cursor.offset() - started;
        cursor.commit();
        self.return_item(Item::SetRecord {
            key,
            rdb_size,
            encoding: SetEncoding::Raw,
            member_count,
        })
    }

    fn parse_set_intset_record(&mut self, cursor: &mut Cursor<'_>) -> ParseResult<Option<Item>> {
        let started = cursor.offset();
        let key = match read_rdb_str(cursor) {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        crate::parser_trace!("intset.raw");
        let (intset, _enc) = match self.parse_rdb_blob(cursor, "intset") {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        let member_count = match count_intset_members(&intset) {
            Ok(v) => v,
            Err(e) => return fatal(e),
        };

        let rdb_size = cursor.offset() - started;
        cursor.commit();
        self.return_item(Item::SetRecord {
            key,
            rdb_size,
            encoding: SetEncoding::IntSet,
            member_count,
        })
    }

    fn parse_set_listpack_record(&mut self, cursor: &mut Cursor<'_>) -> ParseResult<Option<Item>> {
        let started = cursor.offset();
        let key = match read_rdb_str(cursor) {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };

        let (listpack, _enc) = match self.parse_rdb_blob(cursor, "set listpack") {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        if listpack.len() < 6 {
            return fatal(anyhow!("set listpack too short"));
        }
        let quick_count = u16::from_le_bytes([listpack[4], listpack[5]]) as u64;
        let member_count = if quick_count == u16::MAX as u64 {
            match count_listpack_entries(&listpack) {
                Ok(v) => v,
                Err(e) => return fatal(e),
            }
        } else {
            quick_count
        };

        let rdb_size = cursor.offset() - started;
        cursor.commit();
        self.return_item(Item::SetRecord {
            key,
            rdb_size,
            encoding: SetEncoding::ListPack,
            member_count,
        })
    }

    fn parse_zset_record(&mut self, cursor: &mut Cursor<'_>) -> ParseResult<Option<Item>> {
        let started = cursor.offset();
        let key = match read_rdb_str(cursor) {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        let member_count = match self.parse_u64_len(cursor, "zset length") {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        crate::parser_trace!("zset.skiplist");

        for _ in 0..member_count {
            match self.parse_string_encoding(cursor) {
                ParseResult::Ok(_) => {}
                ParseResult::NeedMore => return need_more(),
                ParseResult::Err(e) => return ParseResult::Err(e),
            }
            match self.parse_string_encoding(cursor) {
                ParseResult::Ok(_) => {}
                ParseResult::NeedMore => return need_more(),
                ParseResult::Err(e) => return ParseResult::Err(e),
            }
        }

        let rdb_size = cursor.offset() - started;
        cursor.commit();
        self.return_item(Item::ZSetRecord {
            key,
            rdb_size,
            encoding: ZSetEncoding::SkipList,
            member_count,
        })
    }

    fn parse_zset2_record(&mut self, cursor: &mut Cursor<'_>) -> ParseResult<Option<Item>> {
        let started = cursor.offset();
        let key = match read_rdb_str(cursor) {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        let member_count = match self.parse_u64_len(cursor, "zset2 length") {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        crate::parser_trace!("zset2.skiplist");

        for _ in 0..member_count {
            match self.parse_string_encoding(cursor) {
                ParseResult::Ok(_) => {}
                ParseResult::NeedMore => return need_more(),
                ParseResult::Err(e) => return ParseResult::Err(e),
            }
            match exact(cursor, 8) {
                ParseResult::Ok(_) => {}
                ParseResult::NeedMore => return need_more(),
                ParseResult::Err(e) => return ParseResult::Err(e),
            }
        }

        let rdb_size = cursor.offset() - started;
        cursor.commit();
        self.return_item(Item::ZSet2Record {
            key,
            rdb_size,
            encoding: ZSetEncoding::SkipList,
            member_count,
        })
    }

    fn parse_zset_ziplist_record(&mut self, cursor: &mut Cursor<'_>) -> ParseResult<Option<Item>> {
        let started = cursor.offset();
        let key = match read_rdb_str(cursor) {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        let (ziplist, _enc) = match self.parse_rdb_blob(cursor, "zset ziplist") {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        let entry_count = match count_ziplist_entries(&ziplist) {
            Ok(v) => v,
            Err(e) => return fatal(e),
        };
        if !entry_count.is_multiple_of(2) {
            return fatal(anyhow!("zset ziplist entry count should be even"));
        }
        let member_count = entry_count / 2;

        let rdb_size = cursor.offset() - started;
        cursor.commit();
        self.return_item(Item::ZSetRecord {
            key,
            rdb_size,
            encoding: ZSetEncoding::ZipList,
            member_count,
        })
    }

    fn parse_zset_listpack_record(&mut self, cursor: &mut Cursor<'_>) -> ParseResult<Option<Item>> {
        let started = cursor.offset();
        let key = match read_rdb_str(cursor) {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        let (listpack, enc) = match self.parse_rdb_blob(cursor, "zset listpack") {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        if enc == StringEncoding::LZF {
            crate::parser_trace!("zset.listpack.lzf");
        } else {
            crate::parser_trace!("zset.listpack.raw");
        }

        let entry_count = match count_listpack_entries(&listpack) {
            Ok(v) => v,
            Err(e) => return fatal(e),
        };
        if !entry_count.is_multiple_of(2) {
            return fatal(anyhow!("zset listpack entry count should be even"));
        }
        let member_count = entry_count / 2;

        let rdb_size = cursor.offset() - started;
        cursor.commit();
        self.return_item(Item::ZSetRecord {
            key,
            rdb_size,
            encoding: ZSetEncoding::ListPack,
            member_count,
        })
    }

    fn parse_hash_record(&mut self, cursor: &mut Cursor<'_>) -> ParseResult<Option<Item>> {
        let started = cursor.offset();
        let key = match read_rdb_str(cursor) {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        let pair_count = match self.parse_u64_len(cursor, "hash length") {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        let parse_pairs = fold_n(
            cursor,
            (pair_count * 2) as usize,
            (),
            |cur| self.parse_string_encoding(cur),
            |_, _| (),
        );
        match parse_pairs {
            ParseResult::Ok(()) => {}
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        }

        let rdb_size = cursor.offset() - started;
        cursor.commit();
        self.return_item(Item::HashRecord {
            key,
            rdb_size,
            encoding: HashEncoding::Raw,
            pair_count,
        })
    }

    fn parse_hash_zipmap_record(&mut self, cursor: &mut Cursor<'_>) -> ParseResult<Option<Item>> {
        let started = cursor.offset();
        let key = match read_rdb_str(cursor) {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        let (zipmap, enc) = match self.parse_rdb_blob(cursor, "hash zipmap") {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        if enc == StringEncoding::LZF {
            crate::parser_trace!("hash.zipmap.lzf");
        } else {
            crate::parser_trace!("hash.zipmap.raw");
        }
        let pair_count = match count_zipmap_pairs(&zipmap) {
            Ok(v) => v,
            Err(e) => return fatal(e),
        };

        let rdb_size = cursor.offset() - started;
        cursor.commit();
        self.return_item(Item::HashRecord {
            key,
            rdb_size,
            encoding: HashEncoding::ZipMap,
            pair_count,
        })
    }

    fn parse_hash_ziplist_record(&mut self, cursor: &mut Cursor<'_>) -> ParseResult<Option<Item>> {
        let started = cursor.offset();
        let key = match read_rdb_str(cursor) {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        let (ziplist, _enc) = match self.parse_rdb_blob(cursor, "hash ziplist") {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        let entry_count = match count_ziplist_entries(&ziplist) {
            Ok(v) => v,
            Err(e) => return fatal(e),
        };
        if !entry_count.is_multiple_of(2) {
            return fatal(anyhow!("ziplist entry count should be even for hash"));
        }
        let pair_count = entry_count / 2;

        let rdb_size = cursor.offset() - started;
        cursor.commit();
        self.return_item(Item::HashRecord {
            key,
            rdb_size,
            encoding: HashEncoding::ZipList,
            pair_count,
        })
    }

    fn parse_hash_listpack_record(&mut self, cursor: &mut Cursor<'_>) -> ParseResult<Option<Item>> {
        let started = cursor.offset();
        let key = match read_rdb_str(cursor) {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        let (listpack, _enc) = match self.parse_rdb_blob(cursor, "hash listpack") {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        let entry_count = match count_listpack_entries(&listpack) {
            Ok(v) => v,
            Err(e) => return fatal(e),
        };
        if !entry_count.is_multiple_of(2) {
            return fatal(anyhow!("listpack entry count should be even for hash"));
        }
        let pair_count = entry_count / 2;

        let rdb_size = cursor.offset() - started;
        cursor.commit();
        self.return_item(Item::HashRecord {
            key,
            rdb_size,
            encoding: HashEncoding::ListPack,
            pair_count,
        })
    }

    fn parse_hash_listpack_ex_record(
        &mut self,
        cursor: &mut Cursor<'_>,
    ) -> ParseResult<Option<Item>> {
        let started = cursor.offset();
        let key = match read_rdb_str(cursor) {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        match le_u64(cursor) {
            ParseResult::Ok(_) => {}
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        }
        let (listpack, _enc) = match self.parse_rdb_blob(cursor, "hash listpack-ex") {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        let entry_count = match count_listpack_entries(&listpack) {
            Ok(v) => v,
            Err(e) => return fatal(e),
        };
        if !entry_count.is_multiple_of(3) {
            return fatal(anyhow!(
                "listpack entry count should be divisible by 3 for hash listpack-ex"
            ));
        }
        let pair_count = entry_count / 3;

        let rdb_size = cursor.offset() - started;
        cursor.commit();
        self.return_item(Item::HashRecord {
            key,
            rdb_size,
            encoding: HashEncoding::ListPackEx,
            pair_count,
        })
    }

    fn parse_hash_metadata_record(&mut self, cursor: &mut Cursor<'_>) -> ParseResult<Option<Item>> {
        let started = cursor.offset();
        let key = match read_rdb_str(cursor) {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        match le_u64(cursor) {
            ParseResult::Ok(_) => {}
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        }
        let pair_count = match self.parse_u64_len(cursor, "hash metadata pair count") {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        for _ in 0..pair_count {
            match self.parse_u64_len(cursor, "hash metadata field ttl") {
                ParseResult::Ok(_) => {}
                ParseResult::NeedMore => return need_more(),
                ParseResult::Err(e) => return ParseResult::Err(e),
            }
            match self.parse_string_encoding(cursor) {
                ParseResult::Ok(_) => {}
                ParseResult::NeedMore => return need_more(),
                ParseResult::Err(e) => return ParseResult::Err(e),
            }
            match self.parse_string_encoding(cursor) {
                ParseResult::Ok(_) => {}
                ParseResult::NeedMore => return need_more(),
                ParseResult::Err(e) => return ParseResult::Err(e),
            }
        }

        let rdb_size = cursor.offset() - started;
        cursor.commit();
        self.return_item(Item::HashRecord {
            key,
            rdb_size,
            encoding: HashEncoding::Metadata,
            pair_count,
        })
    }

    fn parse_stream_record(
        &mut self,
        cursor: &mut Cursor<'_>,
        encoding: StreamEncoding,
    ) -> ParseResult<Option<Item>> {
        let started = cursor.offset();
        let key = match read_rdb_str(cursor) {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };

        let listpack_count = match self.parse_u64_len(cursor, "stream listpack count") {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        for _ in 0..listpack_count {
            match self.parse_string_encoding(cursor) {
                ParseResult::Ok(_) => {}
                ParseResult::NeedMore => return need_more(),
                ParseResult::Err(e) => return ParseResult::Err(e),
            }
            match self.parse_string_encoding(cursor) {
                ParseResult::Ok(_) => {}
                ParseResult::NeedMore => return need_more(),
                ParseResult::Err(e) => return ParseResult::Err(e),
            }
        }

        let message_count = match self.parse_u64_len(cursor, "stream message count") {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };

        let meta_len_count = match encoding {
            StreamEncoding::ListPacks => 2,
            StreamEncoding::ListPacks2 | StreamEncoding::ListPacks3 => 7,
        };
        for _ in 0..meta_len_count {
            match self.parse_u64_len(cursor, "stream meta len") {
                ParseResult::Ok(_) => {}
                ParseResult::NeedMore => return need_more(),
                ParseResult::Err(e) => return ParseResult::Err(e),
            }
        }

        let group_count = match self.parse_u64_len(cursor, "stream group count") {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        for _ in 0..group_count {
            match self.parse_string_encoding(cursor) {
                ParseResult::Ok(_) => {}
                ParseResult::NeedMore => return need_more(),
                ParseResult::Err(e) => return ParseResult::Err(e),
            }
            match self.parse_u64_len(cursor, "stream group last_id ms") {
                ParseResult::Ok(_) => {}
                ParseResult::NeedMore => return need_more(),
                ParseResult::Err(e) => return ParseResult::Err(e),
            }
            match self.parse_u64_len(cursor, "stream group last_id seq") {
                ParseResult::Ok(_) => {}
                ParseResult::NeedMore => return need_more(),
                ParseResult::Err(e) => return ParseResult::Err(e),
            }

            if encoding != StreamEncoding::ListPacks {
                match self.parse_u64_len(cursor, "stream group entries_read") {
                    ParseResult::Ok(_) => {}
                    ParseResult::NeedMore => return need_more(),
                    ParseResult::Err(e) => return ParseResult::Err(e),
                }
            }

            match self.parse_stream_pel(cursor, true) {
                ParseResult::Ok(()) => {}
                ParseResult::NeedMore => return need_more(),
                ParseResult::Err(e) => return ParseResult::Err(e),
            }

            let consumer_count = match self.parse_u64_len(cursor, "stream consumer count") {
                ParseResult::Ok(v) => v,
                ParseResult::NeedMore => return need_more(),
                ParseResult::Err(e) => return ParseResult::Err(e),
            };
            for _ in 0..consumer_count {
                match self.parse_string_encoding(cursor) {
                    ParseResult::Ok(_) => {}
                    ParseResult::NeedMore => return need_more(),
                    ParseResult::Err(e) => return ParseResult::Err(e),
                }
                let tlen = if encoding == StreamEncoding::ListPacks3 {
                    16
                } else {
                    8
                };
                match exact(cursor, tlen) {
                    ParseResult::Ok(_) => {}
                    ParseResult::NeedMore => return need_more(),
                    ParseResult::Err(e) => return ParseResult::Err(e),
                }
                match self.parse_stream_pel(cursor, false) {
                    ParseResult::Ok(()) => {}
                    ParseResult::NeedMore => return need_more(),
                    ParseResult::Err(e) => return ParseResult::Err(e),
                }
            }
        }

        let rdb_size = cursor.offset() - started;
        cursor.commit();
        self.return_item(Item::StreamRecord {
            key,
            rdb_size,
            encoding,
            message_count,
        })
    }

    fn parse_stream_pel(&self, cursor: &mut Cursor<'_>, with_nack: bool) -> ParseResult<()> {
        let len = match self.parse_u64_len(cursor, "stream pel len") {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        for _ in 0..len {
            match exact(cursor, 16) {
                ParseResult::Ok(_) => {}
                ParseResult::NeedMore => return need_more(),
                ParseResult::Err(e) => return ParseResult::Err(e),
            }
            if with_nack {
                match exact(cursor, 8) {
                    ParseResult::Ok(_) => {}
                    ParseResult::NeedMore => return need_more(),
                    ParseResult::Err(e) => return ParseResult::Err(e),
                }
                match self.parse_u64_len(cursor, "stream pel delivery count") {
                    ParseResult::Ok(_) => {}
                    ParseResult::NeedMore => return need_more(),
                    ParseResult::Err(e) => return ParseResult::Err(e),
                }
            }
        }
        ok(())
    }

    fn parse_module_opcode(
        &self,
        cursor: &mut Cursor<'_>,
        what: &'static str,
    ) -> ParseResult<RDBModuleOpcode> {
        let opcode = match self.parse_u64_len(cursor, what) {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        let opcode_u8 = match u8::try_from(opcode) {
            Ok(v) => v,
            Err(_) => return fatal(anyhow!("unknown {what}: {opcode}")),
        };
        match RDBModuleOpcode::try_from(opcode_u8) {
            Ok(v) => ok(v),
            Err(_) => fatal(anyhow!("unknown {what}: {opcode}")),
        }
    }

    fn parse_module_value(&self, cursor: &mut Cursor<'_>) -> ParseResult<()> {
        loop {
            let opcode = match self.parse_module_opcode(cursor, "module opcode") {
                ParseResult::Ok(v) => v,
                ParseResult::NeedMore => return need_more(),
                ParseResult::Err(e) => return ParseResult::Err(e),
            };
            match opcode {
                RDBModuleOpcode::Eof => return ok(()),
                RDBModuleOpcode::SInt | RDBModuleOpcode::UInt => {
                    match self.parse_u64_len(cursor, "module int") {
                        ParseResult::Ok(_) => {}
                        ParseResult::NeedMore => return need_more(),
                        ParseResult::Err(e) => return ParseResult::Err(e),
                    }
                }
                RDBModuleOpcode::Float => match exact(cursor, 4) {
                    ParseResult::Ok(_) => {}
                    ParseResult::NeedMore => return need_more(),
                    ParseResult::Err(e) => return ParseResult::Err(e),
                },
                RDBModuleOpcode::Double => match exact(cursor, 8) {
                    ParseResult::Ok(_) => {}
                    ParseResult::NeedMore => return need_more(),
                    ParseResult::Err(e) => return ParseResult::Err(e),
                },
                RDBModuleOpcode::String => match self.parse_string_encoding(cursor) {
                    ParseResult::Ok(_) => {}
                    ParseResult::NeedMore => return need_more(),
                    ParseResult::Err(e) => return ParseResult::Err(e),
                },
            }
        }
    }

    fn parse_module2_record(&mut self, cursor: &mut Cursor<'_>) -> ParseResult<Option<Item>> {
        let started = cursor.offset();
        let key = match read_rdb_str(cursor) {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        match self.parse_u64_len(cursor, "module id") {
            ParseResult::Ok(_) => {}
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        }
        match self.parse_module_value(cursor) {
            ParseResult::Ok(()) => {}
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        }

        crate::parser_trace!("module2.raw");
        let rdb_size = cursor.offset() - started;
        cursor.commit();
        self.return_item(Item::ModuleRecord { key, rdb_size })
    }

    fn parse_function2_record(&mut self, cursor: &mut Cursor<'_>) -> ParseResult<Option<Item>> {
        let started = cursor.offset();
        match self.parse_string_encoding(cursor) {
            ParseResult::Ok(_) => {}
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        }
        let rdb_size = cursor.offset() - started;
        cursor.commit();
        self.return_item(Item::FunctionRecord { rdb_size })
    }

    fn parse_module_aux(&mut self, cursor: &mut Cursor<'_>) -> ParseResult<Option<Item>> {
        let started = cursor.offset();

        match self.parse_u64_len(cursor, "module aux module id") {
            ParseResult::Ok(_) => {}
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        }
        let opcode = match self.parse_module_opcode(cursor, "module aux opcode") {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        if opcode != RDBModuleOpcode::UInt {
            return fatal(anyhow!("module aux first opcode should be UINT"));
        }
        match self.parse_u64_len(cursor, "module aux when") {
            ParseResult::Ok(_) => {}
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        }

        match self.parse_module_value(cursor) {
            ParseResult::Ok(()) => {}
            ParseResult::NeedMore => return need_more(),
            ParseResult::Err(e) => return ParseResult::Err(e),
        }

        let rdb_size = cursor.offset() - started;
        cursor.commit();
        self.return_item(Item::ModuleAux { rdb_size })
    }

    fn dispatch_opcode(
        &mut self,
        opcode: RDBOpcode,
        cursor: &mut Cursor<'_>,
    ) -> ParseResult<Option<Item>> {
        match opcode {
            RDBOpcode::Aux => {
                let key = match read_rdb_str(cursor) {
                    ParseResult::Ok(v) => v,
                    ParseResult::NeedMore => return ParseResult::NeedMore,
                    ParseResult::Err(e) => return ParseResult::Err(e),
                };
                let val = match read_rdb_str(cursor) {
                    ParseResult::Ok(v) => v,
                    ParseResult::NeedMore => return ParseResult::NeedMore,
                    ParseResult::Err(e) => return ParseResult::Err(e),
                };
                cursor.commit();
                debug!("aux field: {key} = {val}");
                self.return_item(Item::Aux { key, val })
            }
            RDBOpcode::SelectDB => {
                let db = match self.parse_u64_len(cursor, "db") {
                    ParseResult::Ok(v) => v,
                    ParseResult::NeedMore => return ParseResult::NeedMore,
                    ParseResult::Err(e) => return ParseResult::Err(e),
                };
                cursor.commit();
                self.current_db = Some(db);
                self.return_item(Item::SelectDB { db })
            }
            RDBOpcode::ResizeDB => {
                let table_size = match self.parse_u64_len(cursor, "table size") {
                    ParseResult::Ok(v) => v,
                    ParseResult::NeedMore => return ParseResult::NeedMore,
                    ParseResult::Err(e) => return ParseResult::Err(e),
                };
                let ttl_table_size = match self.parse_u64_len(cursor, "ttl table size") {
                    ParseResult::Ok(v) => v,
                    ParseResult::NeedMore => return ParseResult::NeedMore,
                    ParseResult::Err(e) => return ParseResult::Err(e),
                };
                cursor.commit();
                self.return_item(Item::ResizeDB {
                    table_size,
                    ttl_table_size,
                })
            }
            RDBOpcode::Eof => {
                if cursor.is_finished() && cursor.remaining_len() == 0 {
                    cursor.commit();
                    return ParseResult::Ok(None);
                }
                match exact(cursor, 8) {
                    ParseResult::Ok(_) => {}
                    ParseResult::NeedMore => return ParseResult::NeedMore,
                    ParseResult::Err(e) => return ParseResult::Err(e),
                }
                if cursor.remaining_len() != 0 {
                    return fatal(anyhow!("input should be empty after EOF checksum"));
                }
                cursor.commit();
                ParseResult::Ok(None)
            }
            RDBOpcode::SlotInfo => {
                let slot_id = match self.parse_u64_len(cursor, "slot id") {
                    ParseResult::Ok(v) => v,
                    ParseResult::NeedMore => return ParseResult::NeedMore,
                    ParseResult::Err(e) => return ParseResult::Err(e),
                };
                let slot_size = match self.parse_u64_len(cursor, "slot size") {
                    ParseResult::Ok(v) => v,
                    ParseResult::NeedMore => return ParseResult::NeedMore,
                    ParseResult::Err(e) => return ParseResult::Err(e),
                };
                let expires_slot_size = match self.parse_u64_len(cursor, "expires slot size") {
                    ParseResult::Ok(v) => v,
                    ParseResult::NeedMore => return ParseResult::NeedMore,
                    ParseResult::Err(e) => return ParseResult::Err(e),
                };
                cursor.commit();
                self.return_item(Item::SlotInfo {
                    slot_id,
                    slot_size,
                    expires_slot_size,
                })
            }
            RDBOpcode::Function2 => self.parse_function2_record(cursor),
            RDBOpcode::FunctionPreGA => fatal(anyhow!("not supported opcode: FunctionPreGA")),
            RDBOpcode::ModuleAux => self.parse_module_aux(cursor),
            RDBOpcode::Idle => {
                let idle_seconds = match self.parse_u64_len(cursor, "idle seconds") {
                    ParseResult::Ok(v) => v,
                    ParseResult::NeedMore => return ParseResult::NeedMore,
                    ParseResult::Err(e) => return ParseResult::Err(e),
                };
                cursor.commit();
                self.return_item(Item::Idle { idle_seconds })
            }
            RDBOpcode::Freq => {
                let freq = match byte(cursor) {
                    ParseResult::Ok(v) => v,
                    ParseResult::NeedMore => return ParseResult::NeedMore,
                    ParseResult::Err(e) => return ParseResult::Err(e),
                };
                cursor.commit();
                self.return_item(Item::Freq { freq })
            }
            RDBOpcode::ExpireTimeMs => {
                let expire_at_ms = match le_u64(cursor) {
                    ParseResult::Ok(v) => v,
                    ParseResult::NeedMore => return ParseResult::NeedMore,
                    ParseResult::Err(e) => return ParseResult::Err(e),
                };
                cursor.commit();
                crate::parser_trace!("expiry.ms");
                self.return_item(Item::ExpiryMs { expire_at_ms })
            }
            RDBOpcode::ExpireTime => {
                let expire_at_s = match le_u32(cursor) {
                    ParseResult::Ok(v) => v,
                    ParseResult::NeedMore => return ParseResult::NeedMore,
                    ParseResult::Err(e) => return ParseResult::Err(e),
                };
                let expire_at_ms = expire_at_s as u64 * 1000;
                cursor.commit();
                crate::parser_trace!("expiry.s");
                self.return_item(Item::ExpiryMs { expire_at_ms })
            }
        }
    }

    fn dispatch_type(
        &mut self,
        type_id: RDBType,
        cursor: &mut Cursor<'_>,
    ) -> ParseResult<Option<Item>> {
        match type_id {
            RDBType::String => self.parse_string_record(cursor),
            RDBType::List => self.parse_raw_list_record(cursor),
            RDBType::ListZipList => self.parse_list_ziplist_record(cursor),
            RDBType::ListQuickList => self.parse_list_quicklist_record(cursor),
            RDBType::ListQuickList2 => self.parse_list_quicklist2_record(cursor),
            RDBType::Set => self.parse_set_record(cursor),
            RDBType::SetIntSet => self.parse_set_intset_record(cursor),
            RDBType::SetListPack => self.parse_set_listpack_record(cursor),
            RDBType::ZSet => self.parse_zset_record(cursor),
            RDBType::ZSet2 => self.parse_zset2_record(cursor),
            RDBType::ZSetZipList => self.parse_zset_ziplist_record(cursor),
            RDBType::ZSetListPack => self.parse_zset_listpack_record(cursor),
            RDBType::Hash => self.parse_hash_record(cursor),
            RDBType::HashZipMap => self.parse_hash_zipmap_record(cursor),
            RDBType::HashZipList => self.parse_hash_ziplist_record(cursor),
            RDBType::HashListPack => self.parse_hash_listpack_record(cursor),
            RDBType::HashListPackEx => self.parse_hash_listpack_ex_record(cursor),
            RDBType::HashMetadata => self.parse_hash_metadata_record(cursor),
            RDBType::Module2 => self.parse_module2_record(cursor),
            RDBType::StreamListPacks => self.parse_stream_record(cursor, StreamEncoding::ListPacks),
            RDBType::StreamListPacks2 => {
                self.parse_stream_record(cursor, StreamEncoding::ListPacks2)
            }
            RDBType::StreamListPacks3 => {
                self.parse_stream_record(cursor, StreamEncoding::ListPacks3)
            }
            RDBType::ModulePreGA => fatal(anyhow!("not supported type: ModulePreGA")),
            RDBType::HashMetadataPreGA => fatal(anyhow!("unsupported type: HashMetadataPreGA")),
            RDBType::HashListPackExPreGA => fatal(anyhow!("unsupported type: HashListPackExPreGA")),
        }
    }

    pub fn poll_next(
        &mut self,
        buffer: &mut crate::parser::core::buffer::Buffer,
    ) -> ParseResult<Option<Item>> {
        let mut cursor = Cursor::new(buffer);
        if self.version == 0 {
            match self.parse_header(&mut cursor) {
                ParseResult::Ok(()) => {}
                ParseResult::NeedMore => return ParseResult::NeedMore,
                ParseResult::Err(e) => return ParseResult::Err(e),
            }
            cursor.commit();
        }

        let flag = match byte(&mut cursor) {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return ParseResult::NeedMore,
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        let branch = match parse_flag(&mut cursor, flag) {
            ParseResult::Ok(v) => v,
            ParseResult::NeedMore => return ParseResult::NeedMore,
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        let result = match branch {
            Flag::Opcode(opcode) => self.dispatch_opcode(opcode, &mut cursor),
            Flag::Type(type_id) => self.dispatch_type(type_id, &mut cursor),
        };
        match result {
            ParseResult::Ok(v) => ParseResult::Ok(v),
            ParseResult::NeedMore => ParseResult::NeedMore,
            ParseResult::Err(e) => {
                let remaining_bytes = cursor.remaining_len();
                let context_bytes = &cursor.as_slice()[..remaining_bytes.min(16)];
                let hex_context = context_bytes
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<Vec<_>>()
                    .join(" ");
                let db_info = match self.current_db {
                    Some(db) => format!(" (current DB: {db})"),
                    None => " (no DB selected)".to_string(),
                };
                ParseResult::Err(ParseError::fatal(anyhow!(
                    "failed to parse RDB item at buffer position {}, RDB version: {}, items parsed: {}{}, remaining bytes: {}, context: [{}], err: {}",
                    cursor.offset(),
                    self.version,
                    self.items_parsed,
                    db_info,
                    remaining_bytes,
                    hex_context,
                    parse_err_to_any(e),
                )))
            }
        }
    }
}

fn count_ziplist_entries(mut input: &[u8]) -> anyhow::Result<u64> {
    ensure!(input.len() >= 10, "ziplist header too short");
    input = &input[10..];

    let mut count = 0u64;
    loop {
        let prevlen_flag = *input
            .first()
            .ok_or_else(|| anyhow!("unexpected end while reading ziplist prevlen"))?;
        input = &input[1..];

        if prevlen_flag == 0xFF {
            return Ok(count);
        }

        if prevlen_flag == 0xFE {
            ensure!(input.len() >= 4, "ziplist prevlen4 truncated");
            input = &input[4..];
        }

        let encoding_byte = *input
            .first()
            .ok_or_else(|| anyhow!("unexpected end while reading ziplist encoding"))?;
        input = &input[1..];

        let to_skip = match encoding_byte >> 6 {
            0 => (encoding_byte & 0b0011_1111) as usize,
            1 => {
                let low = *input
                    .first()
                    .ok_or_else(|| anyhow!("ziplist 14-bit length truncated"))?;
                input = &input[1..];
                (((encoding_byte & 0b0011_1111) as usize) << 8) | low as usize
            }
            2 => {
                ensure!(input.len() >= 4, "ziplist 32-bit length truncated");
                let len = u32::from_be_bytes([input[0], input[1], input[2], input[3]]) as usize;
                input = &input[4..];
                len
            }
            3 => match encoding_byte {
                0b1100_0000 => 2,
                0b1101_0000 => 4,
                0b1110_0000 => 8,
                0b1111_0000 => 3,
                0b1111_1110 => 1,
                _ => 0,
            },
            _ => unreachable!("invalid ziplist encoding byte"),
        };

        ensure!(input.len() >= to_skip, "ziplist entry payload truncated");
        input = &input[to_skip..];
        count += 1;
    }
}

fn count_listpack_entries(mut input: &[u8]) -> anyhow::Result<u64> {
    ensure!(input.len() >= 6, "listpack header too short");
    input = &input[6..];

    let mut count = 0u64;
    loop {
        let flag = *input
            .first()
            .ok_or_else(|| anyhow!("unexpected end while reading listpack flag"))?;
        input = &input[1..];

        let (extra, payload_len, is_eol) = match flag {
            0x80..=0xBF => (0usize, (flag & 0x3F) as usize, false),
            0xE0..=0xEF => {
                let second = *input
                    .first()
                    .ok_or_else(|| anyhow!("listpack 12-bit length truncated"))?;
                input = &input[1..];
                (
                    1usize,
                    ((((flag & 0x0F) as u16) << 8) | second as u16) as usize,
                    false,
                )
            }
            0xF0 => {
                ensure!(input.len() >= 4, "listpack 32-bit length truncated");
                let len = u32::from_le_bytes([input[0], input[1], input[2], input[3]]) as usize;
                input = &input[4..];
                (4usize, len, false)
            }
            0x00..=0x7F => (0usize, 0usize, false),
            0xC0..=0xDF => (0usize, 1usize, false),
            0xF1 => (0usize, 2usize, false),
            0xF2 => (0usize, 3usize, false),
            0xF3 => (0usize, 4usize, false),
            0xF4 => (0usize, 8usize, false),
            0xFF => (0usize, 0usize, true),
            _ => return Err(anyhow!("unknown listpack entry flag: {flag:02x}")),
        };

        if is_eol {
            return Ok(count);
        }

        ensure!(input.len() >= payload_len, "listpack payload truncated");
        input = &input[payload_len..];

        let entry_bytes_before_backlen = 1usize + extra + payload_len;
        let mut v = entry_bytes_before_backlen;
        let mut backlen = 0usize;
        while v != 0 {
            v >>= 7;
            backlen += 1;
        }
        ensure!(input.len() >= backlen, "listpack backlen truncated");
        input = &input[backlen..];
        count += 1;
    }
}

fn count_intset_members(input: &[u8]) -> anyhow::Result<u64> {
    ensure!(input.len() >= 8, "intset header too short");
    let encoding = u32::from_le_bytes([input[0], input[1], input[2], input[3]]);
    let member_count = u32::from_le_bytes([input[4], input[5], input[6], input[7]]) as u64;

    let elem_size = match encoding {
        2 => 2usize,
        4 => 4usize,
        8 => 8usize,
        _ => return Err(anyhow!("unknown intset encoding: {encoding}")),
    };

    let expected_len = 8usize
        .checked_add((member_count as usize).saturating_mul(elem_size))
        .ok_or_else(|| anyhow!("intset payload overflow"))?;
    ensure!(input.len() >= expected_len, "intset payload truncated");

    Ok(member_count)
}

fn read_zipmap_size(mut input: &[u8]) -> anyhow::Result<(&[u8], Option<u64>)> {
    let flag = *input
        .first()
        .ok_or_else(|| anyhow!("zipmap size flag truncated"))?;
    input = &input[1..];

    if flag == 0xFF {
        return Ok((input, None));
    }
    if flag != 0xFE {
        return Ok((input, Some(flag as u64)));
    }

    ensure!(input.len() >= 4, "zipmap 32-bit size truncated");
    let size = u32::from_be_bytes([input[0], input[1], input[2], input[3]]) as u64;
    input = &input[4..];
    Ok((input, Some(size)))
}

fn count_zipmap_pairs(mut input: &[u8]) -> anyhow::Result<u64> {
    let _zmlen = *input
        .first()
        .ok_or_else(|| anyhow!("zipmap header truncated"))?;
    input = &input[1..];

    let mut count = 0u64;
    loop {
        let (rest, key_size) = read_zipmap_size(input)?;
        input = rest;

        let Some(key_len) = key_size else {
            return Ok(count);
        };

        ensure!(input.len() >= key_len as usize, "zipmap key truncated");
        input = &input[key_len as usize..];

        let (rest, value_size) = read_zipmap_size(input)?;
        input = rest;
        let value_len = value_size.ok_or_else(|| anyhow!("zipmap value size missing"))?;

        let free = *input
            .first()
            .ok_or_else(|| anyhow!("zipmap free byte missing"))? as usize;
        input = &input[1..];

        let total = value_len as usize + free;
        ensure!(input.len() >= total, "zipmap value truncated");
        input = &input[total..];
        count += 1;
    }
}
