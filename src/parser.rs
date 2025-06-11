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
enum State {
    Init,
    WaitItem,
    // New state: currently skipping N bytes
    // This state is generic and can be used to skip any type of Value content in the future
    SkippingBytes {
        remaining_len: u64,
    },
    #[allow(dead_code)]
    Done,
}

impl Default for State {
    fn default() -> Self {
        Self::Init
    }
}

#[derive(Debug, Clone)]
pub struct Parser {
    mem_limit: usize,
    buf: BytesMut,
    state: State,
}

impl Default for Parser {
    fn default() -> Self {
        Self::new(1024 * 1024 * 100)
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
    },
}

impl Parser {
    pub fn new(mem_limit: usize) -> Self {
        Self {
            mem_limit,
            buf: BytesMut::new(),
            state: State::Init,
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
        match self.state {
            State::Init => self.on_init(),
            State::WaitItem => self.on_wait_item(),
            State::SkippingBytes { remaining_len } => self.on_skipping_bytes(remaining_len),
            State::Done => Ok(Poll::Ready(None)),
        }
    }

    fn on_init(&mut self) -> AnyResult<Poll<Option<Item>>> {
        let input = self.buf.as_ref();

        let input = read_tag(input, b"REDIS").context("read RDB magic number")?;
        let (input, version) = read_exact(input, 4).context("read RDB version")?;
        let version: u32 = std::str::from_utf8(version)
            .context("RDB version should be a valid utf8 string")?
            .parse()
            .context("RDB version should be a valid number")?;

        ensure!(version <= 12, "unsupported RDB version: {}", version);

        self.advance_to(input.len())?;
        self.transition(State::WaitItem);
        Ok(Poll::Pending)
    }

    fn on_wait_item(&mut self) -> AnyResult<Poll<Option<Item>>> {
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
                    self.transition(State::WaitItem);
                    Ok(Poll::Ready(Some(Item::SelectDB {
                        db: db
                            .as_simple()
                            .context("select db number should be a simple number")?,
                    })))
                }
                RDBOpcode::Eof => {
                    self.advance_to(input.len())?;
                    self.transition(State::Done);
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
                            self.transition(State::SkippingBytes {
                                remaining_len: val_len,
                            });

                            Ok(Poll::Ready(Some(Item::StringRecord {
                                key,
                                rdb_size,
                                mem_size,
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
                            self.transition(State::SkippingBytes {
                                remaining_len: in_len,
                            });

                            Ok(Poll::Ready(Some(Item::StringRecord {
                                key,
                                rdb_size,
                                mem_size,
                            })))
                        }
                    }
                }
                RDBType::List => todo!(),
                RDBType::Set => todo!(),
                RDBType::ZSet => todo!(),
                RDBType::Hash => todo!(),
                RDBType::ZSet2 => todo!(),
                RDBType::ModulePreGA => todo!(),
                RDBType::Module2 => todo!(),
                RDBType::HashZipMap => todo!(),
                RDBType::ListZipList => todo!(),
                RDBType::SetIntSet => todo!(),
                RDBType::ZSetZipList => todo!(),
                RDBType::HashZipList => todo!(),
                RDBType::ListQuickList => todo!(),
                RDBType::StreamListPacks => todo!(),
                RDBType::HashListPack => todo!(),
                RDBType::ZSetListPack => todo!(),
                RDBType::ListQuickList2 => todo!(),
                RDBType::StreamListPacks2 => todo!(),
                RDBType::SetListPack => todo!(),
                RDBType::StreamListPacks3 => todo!(),
                RDBType::HashMetadataPreGA => todo!(),
                RDBType::HashListPackExPreGA => todo!(),
                RDBType::HashMetadata => todo!(),
                RDBType::HashListPackEx => todo!(),
            };
        }

        bail!("unexpected rdb flag: {:02x}", flag);
    }

    fn on_skipping_bytes(&mut self, remaining_len: u64) -> AnyResult<Poll<Option<Item>>> {
        let input = self.buf.as_ref();
        let (input, part_val) = read_at_most(input, remaining_len as usize)?;
        let remaining_len = remaining_len - part_val.len() as u64;

        self.advance_to(input.len())?;

        if remaining_len > 0 {
            self.transition(State::SkippingBytes { remaining_len });
        } else {
            self.transition(State::WaitItem);
        }

        Ok(Poll::Pending)
    }
}

impl Parser {
    fn advance_to(&mut self, expect: usize) -> AnyResult {
        let actual = self.buf.len();
        ensure!(actual >= expect, "not enough data to consume");
        let consumed = actual - expect;
        self.buf.advance(consumed);
        debug!(name = "event.advance", consumed = consumed);
        Ok(())
    }

    fn transition(&mut self, state: State) {
        let old: State = std::mem::replace(&mut self.state, state);
        debug!(name = "event.transition", from = ?old, to = ?self.state);
    }
}
