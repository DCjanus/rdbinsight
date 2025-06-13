use std::task::Poll;

use anyhow::{Context, anyhow, bail, ensure};
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
    RDBHeader,
    NextItem,
    SkipBytes {
        remaining_len: u64,
    },
    /// Stream-skip task used when parsing list-like containers (List, QuickList, QuickList2, ListZipList).
    /// Each *node* varies by encoding:
    ///   • List → single raw element
    ///   • QuickList → embedded ziplist (may be LZF)
    ///   • QuickList2 → embedded listpack
    ///   • ListZipList → the sole ziplist payload (thus `nodes_total == 1`)
    /// We skip each node and accumulate size / element stats on the fly.
    SkipListItems {
        /// List key.
        key: RDBStr,
        /// Expected nodes.
        nodes_total: u64,
        /// Nodes processed so far.
        nodes_skipped: u64,
        /// RDB bytes processed.
        rdb_size_acc: u64,
        /// Estimated heap bytes.
        mem_size_acc: u64,
        /// Elements counted so far.
        elem_count_acc: u64,
        /// Container encoding.
        encoding: ListEncoding,
    },
    /// Fast-Path / Scan-Path entry point: waiting for 10-byte ziplist header.
    ZipListHeader,

    /// Scan-Path: requires we already know entire list payload length.
    ZipListScan {
        total_len: usize,
    },

    /// LZF-compressed ziplist node; wait until `in_len` bytes are available to decompress.
    DecompressLzf {
        in_len: usize,
        out_len: usize,
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
            tasks: vec![ParseTask::RDBHeader],
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
            Some(ParseTask::RDBHeader) => self.handle_read_header(),
            Some(ParseTask::NextItem) => self.handle_wait_for_item(),
            Some(ParseTask::SkipBytes { remaining_len }) => {
                let remaining_len = *remaining_len;
                self.handle_skip_bytes(remaining_len)
            }
            Some(ParseTask::SkipListItems { .. }) => self.handle_skip_list_items(),
            Some(ParseTask::ZipListHeader) => self.handle_zip_list_header(),
            Some(ParseTask::ZipListScan { .. }) => self.handle_zip_list_scan(),
            Some(ParseTask::DecompressLzf { .. }) => self.handle_decompress_lzf(),
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
        self.push_task(ParseTask::NextItem);

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
                    let (input, nodes_total) = read_rdb_len(input).context("read list size")?;
                    let nodes_total = nodes_total
                        .as_simple()
                        .context("list size should be a simple number")?;

                    let end_size = input.len() as u64;
                    let rdb_size_acc = begin_size - end_size + 1; // +1 for the type id

                    self.advance_to(input.len())?;

                    // Replace current task with SkipListItems
                    self.pop_task();
                    self.push_task(ParseTask::SkipListItems {
                        key,
                        nodes_total,
                        nodes_skipped: 0,
                        rdb_size_acc,
                        mem_size_acc: 0,
                        elem_count_acc: 0,
                        encoding: ListEncoding::List,
                    });

                    Ok(Poll::Pending)
                }
                RDBType::ListZipList => {
                    // A ListZipList value contains exactly one ziplist payload that stores all
                    // list elements. We treat it as a list with a single "node" so that we can
                    // reuse the generic SkipListItems machinery.
                    let begin_size = input.len() as u64;
                    let (input, key) = read_rdb_str(input).context("read ziplist key")?;

                    // The total bytes of the list payload follow as an RDB string length. We don't
                    // consume it here – it will be parsed by `handle_skip_list_items` so that we
                    // can stream-parse the inner ziplist. We only need to advance past the key for
                    // now.
                    let end_size = input.len() as u64;
                    let rdb_size_acc = begin_size - end_size + 1; // +1 for the type id byte

                    self.advance_to(input.len())?;

                    self.pop_task();
                    self.push_task(ParseTask::SkipListItems {
                        key,
                        nodes_total: 1, // exactly one ziplist payload
                        nodes_skipped: 0,
                        rdb_size_acc,
                        mem_size_acc: 0,
                        elem_count_acc: 0,
                        encoding: ListEncoding::ZipList,
                    });

                    Ok(Poll::Pending)
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
                        nodes_total, // nodes count
                        nodes_skipped: 0,
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
                        nodes_total,
                        nodes_skipped: 0,
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
        // Local helper enum to describe how we should treat the upcoming node payload.
        enum Either {
            /// Payload is uncompressed; we simply skip the given number of bytes (possibly after
            /// Fast-Path header parsing).
            Skip(u64),
            /// Payload is LZF-compressed; we need to collect `in_len` bytes and then decompress to
            /// `out_len` bytes.
            Decompress { in_len: u64, out_len: u64 },
        }

        let (key, nodes_total, nodes_skipped, rdb_size_acc, mem_size_acc, elem_count_acc, encoding) =
            if let Some(ParseTask::SkipListItems {
                key,
                nodes_total,
                nodes_skipped,
                rdb_size_acc,
                mem_size_acc,
                elem_count_acc,
                encoding,
            }) = self.tasks.last()
            {
                (
                    key.clone(),
                    *nodes_total,
                    *nodes_skipped,
                    *rdb_size_acc,
                    *mem_size_acc,
                    *elem_count_acc,
                    encoding.clone(),
                )
            } else {
                bail!("handle_skip_list_items called without SkipListItems task");
            };

        if nodes_skipped >= nodes_total {
            let mem_size = key.mem_size() as u64 + mem_size_acc;

            const LIST_RECORD_MIN_MEM_THRESHOLD: u64 = 1024; // 1 KiB

            self.pop_task();
            self.push_task(ParseTask::NextItem);

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

                let (input, elem_rdb_size, elem_mem_size, action) = match ziplist_len_variant {
                    RDBLen::Simple(ziplist_len) => {
                        let end_size = input.len() as u64;
                        let elem_rdb_size = begin_size - end_size + ziplist_len;
                        let elem_mem_size = ziplist_len;
                        (
                            input,
                            elem_rdb_size,
                            elem_mem_size,
                            Either::Skip(ziplist_len),
                        )
                    }
                    RDBLen::LZFStr => {
                        let (input, in_len) = read_rdb_len(input).context("read lzf in len")?;
                        let (input, out_len) = read_rdb_len(input).context("read lzf out len")?;
                        let in_len = in_len
                            .as_simple()
                            .context("lzf in len should be a simple number")?;
                        let out_len = out_len
                            .as_simple()
                            .context("lzf out len should be a simple number")?;

                        let end_size = input.len() as u64;
                        let elem_rdb_size = begin_size - end_size + in_len;
                        let elem_mem_size = out_len;
                        (input, elem_rdb_size, elem_mem_size, Either::Decompress {
                            in_len,
                            out_len,
                        })
                    }
                    other => {
                        bail!("unsupported ziplist length variant: {:?}", other);
                    }
                };

                self.advance_to(input.len())?;

                let node_payload = if let Either::Skip(ziplist_in_len) = &action {
                    if self.buf.len() >= *ziplist_in_len as usize {
                        &self.buf[..*ziplist_in_len as usize]
                    } else {
                        &[]
                    }
                } else {
                    &[]
                };

                let elems_in_node = match &action {
                    Either::Skip(_) => count_listpack_entries(node_payload)?,
                    Either::Decompress { .. } => 0,
                };

                if let Some(ParseTask::SkipListItems {
                    nodes_skipped,
                    rdb_size_acc,
                    mem_size_acc,
                    elem_count_acc,
                    ..
                }) = self.tasks.last_mut()
                {
                    *rdb_size_acc += elem_rdb_size;
                    *mem_size_acc += elem_mem_size;
                    *nodes_skipped += 1;
                    *elem_count_acc += elems_in_node;
                }

                match action {
                    Either::Skip(len) => {
                        self.push_task(ParseTask::SkipBytes { remaining_len: len });
                    }
                    Either::Decompress { in_len, out_len } => {
                        self.push_task(ParseTask::DecompressLzf {
                            in_len: in_len as usize,
                            out_len: out_len as usize,
                        });
                    }
                }

                Ok(Poll::Pending)
            }
            ListEncoding::QuickList => {
                // QuickList node: ziplist_length (or LZFStr) + ziplist_data (no container type prefix)
                let input = self.buf.as_ref();
                let begin_size = input.len() as u64;

                // Read the ziplist length
                let (input, ziplist_len_variant) =
                    read_rdb_len(input).context("read ziplist length")?;

                let (input, elem_rdb_size, elem_mem_size, action) = match ziplist_len_variant {
                    RDBLen::Simple(ziplist_len) => {
                        let end_size = input.len() as u64;
                        let elem_rdb_size = begin_size - end_size + ziplist_len;
                        let elem_mem_size = ziplist_len; // Rough estimate
                        (
                            input,
                            elem_rdb_size,
                            elem_mem_size,
                            Either::Skip(ziplist_len),
                        )
                    }
                    RDBLen::LZFStr => {
                        // Compressed node
                        let (input, in_len) = read_rdb_len(input).context("read lzf in len")?;
                        let (input, out_len) = read_rdb_len(input).context("read lzf out len")?;
                        let in_len = in_len
                            .as_simple()
                            .context("lzf in len should be a simple number")?;
                        let out_len = out_len
                            .as_simple()
                            .context("lzf out len should be a simple number")?;

                        let end_size = input.len() as u64;
                        let elem_rdb_size = begin_size - end_size + in_len;
                        let elem_mem_size = out_len;
                        (input, elem_rdb_size, elem_mem_size, Either::Decompress {
                            in_len,
                            out_len,
                        })
                    }
                    other => {
                        bail!("unsupported ziplist length variant: {:?}", other);
                    }
                };

                self.advance_to(input.len())?;

                // For QuickList uncompressed node we defer element counting to the ZipListHeader
                // task to avoid double-counting. For compressed nodes we'll count after
                // decompression.
                let elems_in_node = 0u64;

                if let Some(ParseTask::SkipListItems {
                    nodes_skipped,
                    rdb_size_acc,
                    mem_size_acc,
                    elem_count_acc,
                    ..
                }) = self.tasks.last_mut()
                {
                    *rdb_size_acc += elem_rdb_size;
                    *mem_size_acc += elem_mem_size;
                    *nodes_skipped += 1;
                    *elem_count_acc += elems_in_node;
                }

                match action {
                    Either::Skip(len) if len >= 10 => {
                        self.push_task(ParseTask::SkipBytes {
                            remaining_len: len - 10,
                        });
                        self.push_task(ParseTask::ZipListHeader);
                    }
                    Either::Skip(len) => {
                        self.push_task(ParseTask::SkipBytes { remaining_len: len });
                    }
                    Either::Decompress { in_len, out_len } => {
                        self.push_task(ParseTask::DecompressLzf {
                            in_len: in_len as usize,
                            out_len: out_len as usize,
                        });
                    }
                }

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
                    nodes_skipped,
                    rdb_size_acc,
                    mem_size_acc,
                    elem_count_acc,
                    ..
                }) = self.tasks.last_mut()
                {
                    *rdb_size_acc += elem_rdb_size;
                    *mem_size_acc += elem_mem_size;
                    *nodes_skipped += 1;
                    *elem_count_acc += 1;
                }

                Ok(Poll::Pending)
            }
            ListEncoding::ZipList => {
                // A single ziplist payload that stores all elements in the list (Fast-Path only).
                let input = self.buf.as_ref();
                let begin_size = input.len() as u64;

                // The payload is encoded as an RDB string: <len><bytes>.
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
                        other => {
                            bail!("unsupported ziplist length variant: {:?}", other);
                        }
                    };

                self.advance_to(input.len())?;

                if let Some(ParseTask::SkipListItems {
                    nodes_skipped,
                    rdb_size_acc,
                    mem_size_acc,
                    ..
                }) = self.tasks.last_mut()
                {
                    *rdb_size_acc += elem_rdb_size;
                    *mem_size_acc += elem_mem_size;
                    *nodes_skipped += 1;
                }

                // Fast-Path: parse header first, then skip the remaining payload bytes.
                if ziplist_in_len >= 10 {
                    self.push_task(ParseTask::SkipBytes {
                        remaining_len: ziplist_in_len - 10,
                    });
                    self.push_task(ParseTask::ZipListHeader);
                } else {
                    self.push_task(ParseTask::SkipBytes {
                        remaining_len: ziplist_in_len,
                    });
                }

                Ok(Poll::Pending)
            }
        }
    }

    fn handle_zip_list_header(&mut self) -> AnyResult<Poll<Option<Item>>> {
        // Ensure we have at least 10 bytes (ziplist header size)
        if self.buf.len() < 10 {
            return Ok(Poll::Pending);
        }

        // Safe because we checked length >= 10
        let header = &self.buf[..10];
        let zlbytes = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        let zllen = u16::from_le_bytes([header[8], header[9]]);

        // Consume the 10-byte header
        self.advance_to(self.buf.len() - 10)?;

        // Pop the current task – we've handled the header.
        self.pop_task();

        if zllen != 0xFFFF {
            // Fast-Path – we know the exact number of elements. Add it to the nearest
            // `SkipListItems` accumulator (the first one we find walking backwards).
            if let Some(ParseTask::SkipListItems { elem_count_acc, .. }) = self
                .tasks
                .iter_mut()
                .rev()
                .find(|t| matches!(t, ParseTask::SkipListItems { .. }))
            {
                *elem_count_acc += zllen as u64;
            }
        } else {
            // Scan-Path – replace the pending `SkipBytes` task with a `ZipListScan` task
            // so we can iterate over each entry and count them.
            // The bytes remaining in the current ziplist payload equals `zlbytes - 10`.
            let remaining = zlbytes.saturating_sub(10) as usize;

            // Remove the `SkipBytes` task that was pushed before this header.
            if let Some(ParseTask::SkipBytes { .. }) = self.tasks.last() {
                self.pop_task();
            }

            // Push the scan task which will take care of consuming the payload and counting entries.
            self.push_task(ParseTask::ZipListScan {
                total_len: remaining,
            });
        }

        // After processing, continue parsing – no ready item yet.
        Ok(Poll::Pending)
    }

    fn handle_zip_list_scan(&mut self) -> AnyResult<Poll<Option<Item>>> {
        // Retrieve the expected payload length.
        let total_len = if let Some(ParseTask::ZipListScan { total_len }) = self.tasks.last() {
            *total_len
        } else {
            bail!("handle_zip_list_scan called without ZipListScan task");
        };

        // Wait until we have the full payload in the buffer.
        if self.buf.len() < total_len {
            return Ok(Poll::Pending);
        }

        let payload = &self.buf[..total_len];
        let entries = scan_ziplist_entries(payload).unwrap_or(0);

        // Consume the payload bytes we just processed.
        self.advance_to(self.buf.len() - total_len)?;

        // Pop the scan task.
        self.pop_task();

        // Update the nearest SkipListItems accumulator with the counted entries.
        if let Some(ParseTask::SkipListItems { elem_count_acc, .. }) = self
            .tasks
            .iter_mut()
            .rev()
            .find(|t| matches!(t, ParseTask::SkipListItems { .. }))
        {
            *elem_count_acc += entries;
        }

        Ok(Poll::Pending)
    }

    fn handle_decompress_lzf(&mut self) -> AnyResult<Poll<Option<Item>>> {
        // Extract parameters of current task
        let (in_len, out_len) =
            if let Some(ParseTask::DecompressLzf { in_len, out_len }) = self.tasks.last() {
                (*in_len, *out_len)
            } else {
                bail!("handle_decompress_lzf called without DecompressLzf task");
            };

        // Wait until compressed bytes are available
        if self.buf.len() < in_len {
            return Ok(Poll::Pending);
        }

        // Memory limit check: decompressed buffer will be allocated separately but we still make sure it's within limit
        ensure!(
            self.buf.len() - in_len + out_len <= self.mem_limit,
            "memory limit exceeded while decompressing LZF"
        );

        let compressed = &self.buf[..in_len];
        let decompressed = lzf::decompress(compressed, out_len)
            .map_err(|e| anyhow!("failed to decompress lzf: {}", e))?;

        ensure!(
            decompressed.len() == out_len,
            "decompressed length mismatched: expect {} got {}",
            out_len,
            decompressed.len()
        );

        // Consume compressed bytes from buffer
        self.advance_to(self.buf.len() - in_len)?;

        // Pop DecompressLzf task
        self.pop_task();

        // Determine element count based on decoded payload (ziplist or listpack)
        let mut entries = 0u64;

        if decompressed.len() >= 10 {
            // Attempt ziplist fast-path
            let zllen = u16::from_le_bytes([decompressed[8], decompressed[9]]);
            if zllen != 0xFFFF {
                entries = zllen as u64;
            } else {
                // Scan ziplist body
                let body = &decompressed[10..];
                if let Some(cnt) = scan_ziplist_entries(body) {
                    entries = cnt;
                }
            }
        }

        // If entries still unknown, try listpack header (QuickList2)
        if entries == 0 && decompressed.len() >= 6 {
            if let Ok(cnt) = count_listpack_entries(&decompressed) {
                entries = cnt;
            }
        }

        // Update accumulator in nearest SkipListItems
        if entries > 0 {
            if let Some(ParseTask::SkipListItems { elem_count_acc, .. }) = self
                .tasks
                .iter_mut()
                .rev()
                .find(|t| matches!(t, ParseTask::SkipListItems { .. }))
            {
                *elem_count_acc += entries;
            }
        }

        Ok(Poll::Pending)
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

/// Count the number of entries in a listpack.
fn count_listpack_entries(buf: &[u8]) -> AnyResult<u64> {
    let (_, buf) = read_exact(buf, 6)?;
    let num_elems = u16::from_le_bytes([buf[4], buf[5]]) as u64;
    Ok(num_elems)
}

fn scan_ziplist_entries(buf: &[u8]) -> Option<u64> {
    let mut pos = 0usize;
    let mut count = 0u64;
    while pos < buf.len() {
        // Reached ziplist end marker 0xFF.
        if buf[pos] == 0xFF {
            return Some(count);
        }

        // --- prevlen field ---
        if buf[pos] < 0xFE {
            // 1-byte prevlen
            pos += 1;
        } else if buf[pos] == 0xFE {
            // 5-byte prevlen (0xFE + 4-byte len)
            if pos + 5 > buf.len() {
                return None;
            }
            pos += 5;
        } else {
            return None; // Malformed prevlen
        }

        if pos >= buf.len() {
            return None;
        }

        // --- encoding / length field ---
        let encoding = buf[pos];

        // Helper to advance pointer safely
        let advance = |n: usize, pos: &mut usize, len: usize| -> bool {
            if *pos + n > len {
                false
            } else {
                *pos += n;
                true
            }
        };

        // Determine payload length based on encoding kind.
        if (encoding >> 6) == 0b00 {
            // 6-bit string
            let strlen = (encoding & 0x3F) as usize;
            if !advance(1 + strlen, &mut pos, buf.len()) {
                return None;
            }
        } else if (encoding >> 6) == 0b01 {
            // 14-bit string (2-byte header)
            if pos + 2 > buf.len() {
                return None;
            }
            let strlen = (((encoding & 0x3F) as usize) << 8) | (buf[pos + 1] as usize);
            if !advance(2 + strlen, &mut pos, buf.len()) {
                return None;
            }
        } else if (encoding >> 6) == 0b10 {
            // 32-bit string (5-byte header)
            if pos + 5 > buf.len() {
                return None;
            }
            let strlen =
                u32::from_be_bytes([buf[pos + 1], buf[pos + 2], buf[pos + 3], buf[pos + 4]])
                    as usize;
            if !advance(5 + strlen, &mut pos, buf.len()) {
                return None;
            }
        } else {
            // Integer encodings (high 2 bits == 0b11) according to Redis
            // ziplist specification. Note: 0xF1..=0xFD are 4-bit immediate
            // values stored in the header itself and hence have payload
            // length 0.
            let payload_len = match encoding {
                0xC0 => 2,        // 16-bit signed int  (int16)
                0xD0 => 4,        // 32-bit signed int  (int32)
                0xE0 => 8,        // 64-bit signed int  (int64)
                0xF0 => 3,        // 24-bit signed int  (int24)
                0xFE => 1,        // 8-bit  signed int  (int8)
                0xF1..=0xFD => 0, // 4-bit immediate integers (no payload)
                _ => return None, // Unknown / unsupported encoding
            };
            if !advance(1 + payload_len, &mut pos, buf.len()) {
                return None;
            }
        }

        count += 1;
    }
    None
}
