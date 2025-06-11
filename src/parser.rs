use std::task::Poll;

use anyhow::{Context, bail, ensure};
use bytes::{Buf, BytesMut};
use tracing::debug;

use crate::{
    helper::AnyResult,
    parser::{
        combinators::{read_n, read_tag, read_u8},
        definitions::{RDBOpcode, RDBType},
        rdb_parsers::{RDBStr, read_aux, read_rdb_len, read_rdb_str},
    },
};

mod combinators;
mod definitions;
mod rdb_parsers;

#[derive(Debug, Clone)]
enum State {
    Init,
    WaitItem,
    #[allow(dead_code)]
    Done,
}

impl Default for State {
    fn default() -> Self {
        Self::Init
    }
}

#[derive(Debug, Clone, Default)]
pub struct Parser {
    buf: BytesMut,
    state: State,
}

#[derive(Debug, Clone)]
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

    // TODO: 不输出 Value，仅记录元信息，以应对大 Value 的情况
    StringRecord {
        key: RDBStr,
        val: RDBStr,
    },
}

impl Parser {
    pub fn feed(&mut self, data: &[u8]) -> AnyResult {
        self.buf.extend_from_slice(data);
        Ok(())
    }

    pub fn parse_next(&mut self) -> AnyResult<Poll<Option<Item>>> {
        match self.state {
            State::Init => self.on_init(),
            State::WaitItem => self.on_wait_item(),
            State::Done => Ok(Poll::Ready(None)),
        }
    }

    fn on_init(&mut self) -> AnyResult<Poll<Option<Item>>> {
        let input = self.buf.as_ref();

        let input = read_tag(input, b"REDIS").context("read RDB magic number")?;
        let (input, version) = read_n(input, 4).context("read RDB version")?;
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
                    let (input, key) = read_rdb_str(input).context("read string")?;
                    let (input, val) = read_rdb_str(input).context("read string")?;
                    self.advance_to(input.len())?;
                    Ok(Poll::Ready(Some(Item::StringRecord { key, val })))
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
