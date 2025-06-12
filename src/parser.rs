use std::task::Poll;

use anyhow::{Context, bail, ensure};
use bytes::{Buf, BytesMut};
use tracing::debug;

use crate::{
    helper::AnyResult,
    parser::{
        combinators::{read_at_most, read_exact, read_tag, read_u8},
        definitions::{RDBOpcode, RDBType},
        rdb_parsers::{RDBLen, RDBStr, read_aux, read_rdb_len, read_rdb_str},
    },
};

mod combinators;
mod definitions;
pub mod rdb_parsers;

#[derive(Debug, Clone)]
pub enum ParseTask {
    ReadHeader,
    WaitForItem,
    SkipBytes {
        remaining_len: u64,
    },
    SkipListItems {
        key: RDBStr,
        members_total: u64,
        members_skipped: u64,
        rdb_size_acc: u64,
        mem_size_acc: u64,
        elem_count_acc: u64,
        encoding: ListEncoding,
    },
}

#[derive(Debug, Clone)]
pub struct Parser {
    mem_limit: usize,
    buf: BytesMut,
    tasks: Vec<ParseTask>,
}

impl Default for Parser {
    fn default() -> Self {
        Self::new(1024 * 1024 * 16)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Item {
    Aux {
        key: RDBStr,
        val: RDBStr,
    },
    SelectDB {
        db: u64,
    },
    ResizeDB {
        table_size: u64,
        ttl_table_size: u64,
    },
    StringRecord {
        key: RDBStr,
        rdb_size: u64,
        mem_size: u64,
        encoding: StringEncoding,
    },
    ListRecord {
        key: RDBStr,
        members: u64,
        rdb_size: u64,
        mem_size: u64,
        encoding: ListEncoding,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StringEncoding {
    Raw,
    Int,
    LZF,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListEncoding {
    List,
    ZipList,
    QuickList,
    QuickList2,
}

impl Parser {
    pub fn new(mem_limit: usize) -> Self {
        Self {
            mem_limit,
            buf: BytesMut::new(),
            tasks: vec![ParseTask::ReadHeader],
        }
    }

    pub fn feed(&mut self, data: &[u8]) -> AnyResult {
        let mem_size = self.buf.len() + data.len();
        ensure!(
            mem_size <= self.mem_limit,
            "memory limit exceeded: {} > {}",
            mem_size,
            self.mem_limit
        );
        self.buf.extend_from_slice(data);
        Ok(())
    }

    pub fn parse_next(&mut self) -> AnyResult<Poll<Option<Item>>> {
        match self.tasks.last_mut() {
            Some(ParseTask::ReadHeader) => self.handle_read_header(),
            Some(ParseTask::WaitForItem) => self.handle_wait_for_item(),
            Some(ParseTask::SkipBytes { remaining_len }) => {
                let remaining_len = *remaining_len;
                self.handle_skip_bytes(remaining_len)
            }
            Some(ParseTask::SkipListItems { .. }) => self.handle_skip_list_items(),
            None => Ok(Poll::Ready(None)),
        }
    }

    fn handle_read_header(&mut self) -> AnyResult<Poll<Option<Item>>> {
        let input = self.buf.as_ref();

        let input = read_tag(input, b"REDIS").context("read RDB magic number")?;
        let (input, version) = read_exact(input, 4).context("read RDB version")?;
        let version: u32 = std::str::from_utf8(version)
            .context("RDB version should be a valid utf8 string")?
            .parse()
            .context("RDB version should be a valid number")?;

        ensure!(version <= 12, "unsupported RDB version: {}", version);

        self.advance_to(input.len())?;

        self.pop_task();
        self.push_task(ParseTask::WaitForItem);

        Ok(Poll::Pending)
    }

    fn handle_wait_for_item(&mut self) -> AnyResult<Poll<Option<Item>>> {
        let input = self.buf.as_ref();

        let (input, flag) = read_u8(input).context("read item flag")?;

        if let Ok(opcode) = RDBOpcode::try_from(flag) {
            return match opcode {
                RDBOpcode::Aux => {
                    let (input, (key, val)) = read_aux(input).context("read aux")?;
                    self.advance_to(input.len())?;
                    Ok(Poll::Ready(Some(Item::Aux { key, val })))
                }
                RDBOpcode::ResizeDB => {
                    let (input, table_size) =
                        read_rdb_len(input).context("read hash table size")?;
                    let (input, ttl_table_size) =
                        read_rdb_len(input).context("read ttl table size")?;
                    self.advance_to(input.len())?;
                    Ok(Poll::Ready(Some(Item::ResizeDB {
                        table_size: table_size
                            .as_simple()
                            .context("table size should be a simple number")?,
                        ttl_table_size: ttl_table_size
                            .as_simple()
                            .context("ttl table size should be a simple number")?,
                    })))
                }
                RDBOpcode::SelectDB => {
                    let (input, db) = read_rdb_len(input).context("read select db number")?;
                    self.advance_to(input.len())?;
                    Ok(Poll::Ready(Some(Item::SelectDB {
                        db: db
                            .as_simple()
                            .context("select db number should be a simple number")?,
                    })))
                }
                RDBOpcode::Eof => {
                    self.advance_to(input.len())?;
                    self.pop_task();
                    Ok(Poll::Ready(None))
                }
                _ => bail!("unexpected rdb opcode: {:?}", opcode),
            };
        }

        if let Ok(type_id) = RDBType::try_from(flag) {
            return match type_id {
                RDBType::String => {
                    let begin_size = input.len() as u64;
                    let (input, key) = read_rdb_str(input).context("read string key")?;
                    let (input, value_len) =
                        read_rdb_len(input).context("read string value length")?;
                    match value_len {
                        RDBLen::Simple(val_len) => {
                            let end_size = input.len() as u64;
                            let rdb_size = begin_size - end_size + val_len + 1; // +1 for the type id
                            let mem_size = key.mem_size() as u64 + val_len;

                            self.advance_to(input.len())?;

                            self.push_task(ParseTask::SkipBytes {
                                remaining_len: val_len,
                            });

                            Ok(Poll::Ready(Some(Item::StringRecord {
                                key,
                                rdb_size,
                                mem_size,
                                encoding: StringEncoding::Raw,
                            })))
                        }
                        RDBLen::IntStr(val) => {
                            let val = RDBStr::Int(val);

                            let end_size = input.len() as u64;
                            let rdb_size = begin_size - end_size + 1; // +1 for the type id
                            let mem_size = key.mem_size() as u64 + val.mem_size() as u64;

                            self.advance_to(input.len())?;
                            Ok(Poll::Ready(Some(Item::StringRecord {
                                key,
                                rdb_size,
                                mem_size,
                                encoding: StringEncoding::Int,
                            })))
                        }
                        RDBLen::LZFStr => {
                            let (input, in_len) =
                                read_rdb_len(input).context("read lzf str in len")?;
                            let (input, out_len) =
                                read_rdb_len(input).context("read lzf str out len")?;
                            let in_len = in_len
                                .as_simple()
                                .context("in len should be a simple number")?;
                            let out_len = out_len
                                .as_simple()
                                .context("out len should be a simple number")?;

                            let end_size = input.len() as u64;
                            let rdb_size = begin_size - end_size + in_len + 1; // +1 for the type id
                            let mem_size = key.mem_size() as u64 + out_len;

                            self.advance_to(input.len())?;

                            self.push_task(ParseTask::SkipBytes {
                                remaining_len: in_len,
                            });

                            Ok(Poll::Ready(Some(Item::StringRecord {
                                key,
                                rdb_size,
                                mem_size,
                                encoding: StringEncoding::LZF,
                            })))
                        }
                    }
                }
                RDBType::List => {
                    let begin_size = input.len() as u64;
                    let (input, key) = read_rdb_str(input).context("read list key")?;
                    let (input, members_total) = read_rdb_len(input).context("read list size")?;
                    let members_total = members_total
                        .as_simple()
                        .context("list size should be a simple number")?;

                    let end_size = input.len() as u64;
                    let rdb_size_acc = begin_size - end_size + 1; // +1 for the type id

                    self.advance_to(input.len())?;

                    // Replace current task with SkipListItems
                    self.pop_task();
                    self.push_task(ParseTask::SkipListItems {
                        key,
                        members_total,
                        members_skipped: 0,
                        rdb_size_acc,
                        mem_size_acc: 0,
                        elem_count_acc: 0,
                        encoding: ListEncoding::List,
                    });

                    Ok(Poll::Pending)
                }
                RDBType::ListZipList => {
                    bail!("TODO: support ZipList encoding (currently unsupported)");
                }
                RDBType::ListQuickList => {
                    let begin_size = input.len() as u64;
                    let (input, key) = read_rdb_str(input).context("read quicklist key")?;
                    let (input, nodes_total) =
                        read_rdb_len(input).context("read quicklist nodes count")?;
                    let nodes_total = nodes_total
                        .as_simple()
                        .context("quicklist nodes count should be a simple number")?;

                    let end_size = input.len() as u64;
                    let rdb_size_acc = begin_size - end_size + 1; // +1 for the type id

                    self.advance_to(input.len())?;

                    self.pop_task();
                    self.push_task(ParseTask::SkipListItems {
                        key,
                        members_total: nodes_total,
                        members_skipped: 0,
                        rdb_size_acc,
                        mem_size_acc: 0,
                        elem_count_acc: 0,
                        encoding: ListEncoding::QuickList,
                    });

                    Ok(Poll::Pending)
                }
                RDBType::ListQuickList2 => {
                    // QuickList2 format: each node contains a ziplist
                    let begin_size = input.len() as u64;
                    let (input, key) = read_rdb_str(input).context("read quicklist2 key")?;
                    let (input, nodes_total) =
                        read_rdb_len(input).context("read quicklist2 nodes count")?;
                    let nodes_total = nodes_total
                        .as_simple()
                        .context("quicklist2 nodes count should be a simple number")?;

                    let end_size = input.len() as u64;
                    let rdb_size_acc = begin_size - end_size + 1; // +1 for the type id

                    self.advance_to(input.len())?;

                    self.pop_task();
                    self.push_task(ParseTask::SkipListItems {
                        key,
                        members_total: nodes_total,
                        members_skipped: 0,
                        rdb_size_acc,
                        mem_size_acc: 0,
                        elem_count_acc: 0,
                        encoding: ListEncoding::QuickList2,
                    });

                    Ok(Poll::Pending)
                }
                RDBType::Set => {
                    bail!("Set type not yet implemented");
                }
                RDBType::ZSet => {
                    bail!("ZSet type not yet implemented");
                }
                RDBType::ZSet2 => todo!(),
                RDBType::ModulePreGA => todo!(),
                RDBType::Module2 => todo!(),
                RDBType::HashZipMap => todo!(),
                RDBType::SetIntSet => todo!(),
                RDBType::ZSetZipList => todo!(),
                RDBType::HashZipList => todo!(),
                RDBType::StreamListPacks => todo!(),
                RDBType::HashListPack => todo!(),
                RDBType::ZSetListPack => todo!(),
                RDBType::StreamListPacks2 => todo!(),
                RDBType::SetListPack => todo!(),
                RDBType::StreamListPacks3 => todo!(),
                RDBType::HashMetadataPreGA => todo!(),
                RDBType::HashListPackExPreGA => todo!(),
                RDBType::HashMetadata => todo!(),
                RDBType::HashListPackEx => todo!(),
                RDBType::Hash => todo!(),
            };
        }

        bail!("unexpected rdb flag: {:02x}", flag);
    }

    fn handle_skip_bytes(&mut self, remaining_len: u64) -> AnyResult<Poll<Option<Item>>> {
        let input = self.buf.as_ref();
        let (input, part_val) = read_at_most(input, remaining_len as usize)?;
        let remaining_len = remaining_len - part_val.len() as u64;

        self.advance_to(input.len())?;

        if remaining_len == 0 {
            self.pop_task();
            return Ok(Poll::Pending);
        }

        if let Some(ParseTask::SkipBytes { remaining_len: len }) = self.tasks.last_mut() {
            *len = remaining_len;
        }

        Ok(Poll::Pending)
    }

    fn handle_skip_list_items(&mut self) -> AnyResult<Poll<Option<Item>>> {
        let (
            key,
            members_total,
            members_skipped,
            rdb_size_acc,
            mem_size_acc,
            elem_count_acc,
            encoding,
        ) = if let Some(ParseTask::SkipListItems {
            key,
            members_total,
            members_skipped,
            rdb_size_acc,
            mem_size_acc,
            elem_count_acc,
            encoding,
        }) = self.tasks.last()
        {
            (
                key.clone(),
                *members_total,
                *members_skipped,
                *rdb_size_acc,
                *mem_size_acc,
                *elem_count_acc,
                encoding.clone(),
            )
        } else {
            bail!("handle_skip_list_items called without SkipListItems task");
        };

        if members_skipped >= members_total {
            let mem_size = key.mem_size() as u64 + mem_size_acc;

            const LIST_RECORD_MIN_MEM_THRESHOLD: u64 = 1024; // 1 KiB

            self.pop_task();
            self.push_task(ParseTask::WaitForItem);

            if mem_size < LIST_RECORD_MIN_MEM_THRESHOLD {
                return Ok(Poll::Pending);
            }

            return Ok(Poll::Ready(Some(Item::ListRecord {
                key,
                members: elem_count_acc,
                rdb_size: rdb_size_acc,
                mem_size,
                encoding,
            })));
        }

        match encoding {
            ListEncoding::QuickList2 => {
                let input = self.buf.as_ref();
                let begin_size = input.len() as u64;

                let (input, _container_type) = read_u8(input).context("read container type")?;

                let (input, ziplist_len_variant) =
                    read_rdb_len(input).context("read ziplist length")?;

                let (input, elem_rdb_size, elem_mem_size, ziplist_in_len) =
                    match ziplist_len_variant {
                        RDBLen::Simple(ziplist_len) => {
                            let end_size = input.len() as u64;
                            let elem_rdb_size = begin_size - end_size + ziplist_len;
                            let elem_mem_size = ziplist_len;
                            (input, elem_rdb_size, elem_mem_size, ziplist_len)
                        }
                        RDBLen::LZFStr => {
                            let (input, in_len) = read_rdb_len(input).context("read lzf in len")?;
                            let (input, out_len) =
                                read_rdb_len(input).context("read lzf out len")?;
                            let in_len = in_len
                                .as_simple()
                                .context("lzf in len should be a simple number")?;
                            let out_len = out_len
                                .as_simple()
                                .context("lzf out len should be a simple number")?;

                            let end_size = input.len() as u64;
                            let elem_rdb_size = begin_size - end_size + in_len;
                            let elem_mem_size = out_len;
                            (input, elem_rdb_size, elem_mem_size, in_len)
                        }
                        other => {
                            bail!("unsupported ziplist length variant: {:?}", other);
                        }
                    };

                self.advance_to(input.len())?;

                let node_payload = if self.buf.len() >= ziplist_in_len as usize {
                    &self.buf[..ziplist_in_len as usize]
                } else {
                    &[]
                };

                let elems_in_node = count_listpack_entries(node_payload).unwrap_or(0);

                if let Some(ParseTask::SkipListItems {
                    members_skipped,
                    rdb_size_acc,
                    mem_size_acc,
                    elem_count_acc,
                    ..
                }) = self.tasks.last_mut()
                {
                    *rdb_size_acc += elem_rdb_size;
                    *mem_size_acc += elem_mem_size;
                    *members_skipped += 1;
                    *elem_count_acc += elems_in_node;
                }

                // Skip the ziplist data
                self.push_task(ParseTask::SkipBytes {
                    remaining_len: ziplist_in_len,
                });

                Ok(Poll::Pending)
            }
            ListEncoding::QuickList => {
                // QuickList node: ziplist_length (or LZFStr) + ziplist_data (no container type prefix)
                let input = self.buf.as_ref();
                let begin_size = input.len() as u64;

                // Read the ziplist length
                let (input, ziplist_len_variant) =
                    read_rdb_len(input).context("read ziplist length")?;

                let (input, elem_rdb_size, elem_mem_size, ziplist_in_len) =
                    match ziplist_len_variant {
                        RDBLen::Simple(ziplist_len) => {
                            let end_size = input.len() as u64;
                            let elem_rdb_size = begin_size - end_size + ziplist_len;
                            let elem_mem_size = ziplist_len; // Rough estimate
                            (input, elem_rdb_size, elem_mem_size, ziplist_len)
                        }
                        RDBLen::LZFStr => {
                            // Compressed node
                            let (input, in_len) = read_rdb_len(input).context("read lzf in len")?;
                            let (input, out_len) =
                                read_rdb_len(input).context("read lzf out len")?;
                            let in_len = in_len
                                .as_simple()
                                .context("lzf in len should be a simple number")?;
                            let out_len = out_len
                                .as_simple()
                                .context("lzf out len should be a simple number")?;

                            let end_size = input.len() as u64;
                            let elem_rdb_size = begin_size - end_size + in_len;
                            let elem_mem_size = out_len;
                            (input, elem_rdb_size, elem_mem_size, in_len)
                        }
                        other => {
                            bail!("unsupported ziplist length variant: {:?}", other);
                        }
                    };

                self.advance_to(input.len())?;

                // Attempt to determine the number of entries in the ziplist.
                let node_payload = if self.buf.len() >= ziplist_in_len as usize {
                    &self.buf[..ziplist_in_len as usize]
                } else {
                    &[]
                };

                // If the node is compressed we can't inspect it unless we
                // decompress it first. For now, fall back to `0` in that case.
                let elems_in_node = count_ziplist_entries(node_payload).unwrap_or(0);

                if let Some(ParseTask::SkipListItems {
                    members_skipped,
                    rdb_size_acc,
                    mem_size_acc,
                    elem_count_acc,
                    ..
                }) = self.tasks.last_mut()
                {
                    *rdb_size_acc += elem_rdb_size;
                    *mem_size_acc += elem_mem_size;
                    *members_skipped += 1;
                    *elem_count_acc += elems_in_node;
                }

                // Skip the ziplist data
                self.push_task(ParseTask::SkipBytes {
                    remaining_len: ziplist_in_len,
                });

                Ok(Poll::Pending)
            }
            ListEncoding::List => {
                // Plain list item: an RDB string element
                let input = self.buf.as_ref();
                let begin_size = input.len() as u64;

                let (input, element) = read_rdb_str(input).context("read list element")?;

                let end_size = input.len() as u64;
                let elem_rdb_size = begin_size - end_size;
                let elem_mem_size = element.mem_size() as u64;

                self.advance_to(input.len())?;

                if let Some(ParseTask::SkipListItems {
                    members_skipped,
                    rdb_size_acc,
                    mem_size_acc,
                    elem_count_acc,
                    ..
                }) = self.tasks.last_mut()
                {
                    *rdb_size_acc += elem_rdb_size;
                    *mem_size_acc += elem_mem_size;
                    *members_skipped += 1;
                    *elem_count_acc += 1;
                }

                Ok(Poll::Pending)
            }
            ListEncoding::ZipList => {
                bail!("TODO: support ZipList encoding (currently unsupported)")
            }
        }
    }
}

impl Parser {
    fn push_task(&mut self, task: ParseTask) {
        debug!(name = "event.push_task", task = ?task);
        self.tasks.push(task);
    }

    fn pop_task(&mut self) -> Option<ParseTask> {
        debug!(name = "event.pop_task", task = ?self.tasks.last());
        self.tasks.pop()
    }

    fn advance_to(&mut self, expect: usize) -> AnyResult {
        let actual = self.buf.len();
        ensure!(actual >= expect, "not enough data to consume");
        let consumed = actual - expect;
        self.buf.advance(consumed);
        debug!(name = "event.advance", consumed = consumed);
        Ok(())
    }
}

fn count_ziplist_entries(buf: &[u8]) -> Option<u64> {
    if buf.len() < 10 {
        return None;
    }
    let zllen = u16::from_le_bytes([buf[8], buf[9]]) as u64;
    if zllen == 0xFFFF { None } else { Some(zllen) }
}

/// Parse a Redis ListPack buffer and return the number of entries it stores.
/// Returns `None` if the input buffer is too small or malformed.
fn count_listpack_entries(buf: &[u8]) -> Option<u64> {
    // A listpack starts with: <total-bytes: u32><num-elements: u16>...
    if buf.len() < 6 {
        return None;
    }
    let num_elems = u16::from_le_bytes([buf[4], buf[5]]) as u64;
    Some(num_elems)
}
