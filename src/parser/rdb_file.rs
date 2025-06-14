use anyhow::{Context, bail, ensure};
use spire_enum::prelude::{delegate_impl, delegated_enum};

use super::{
    buffer::Buffer,
    item::Item,
    record_list::{ListRecordParser, QuickListRecordParser, ZipListRecordParser},
    record_string::StringRecordParser,
    state_parser::StateParser,
};
use crate::{
    helper::AnyResult,
    parser::{
        combinators::{read_exact, read_tag, read_u8},
        definitions::{RDBOpcode, RDBType},
        rdb_parsers::{read_rdb_len, read_rdb_str},
    },
};

#[delegated_enum(impl_variants_into_enum)]
enum ItemParser {
    StringRecord(StringRecordParser),
    ListRecord(ListRecordParser),
    ZipListRecord(ZipListRecordParser),
    QuickListRecord(QuickListRecordParser),
}

#[delegate_impl]
impl StateParser for ItemParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output>;
}

/// Stateful, incremental parser for an entire RDB file.
#[derive(Default)]
pub struct RDBFileParser {
    version: u64,
    entrust: Option<ItemParser>,
}

impl RDBFileParser {
    fn read_header(&mut self, buffer: &mut Buffer) -> AnyResult<()> {
        let input = buffer.as_ref();
        let input = read_tag(input, b"REDIS").context("read magic number")?;
        let (input, version) = read_exact(input, 4)?;
        let version = std::str::from_utf8(version).context("version should be utf8")?;
        let version: u64 = version.parse().context("version should be a number")?;
        ensure!(version >= 1, "version should be >= 1");
        ensure!(version <= 12, "version should be <= 12");

        self.version = version;
        buffer.consume_to(input.as_ptr());
        Ok(())
    }

    // Execute a child parser immediately if possible, otherwise stash it for later.
    fn set_entrust<E>(&mut self, mut entrust: E, buffer: &mut Buffer) -> AnyResult<Item>
    where E: StateParser<Output = Item> + Into<ItemParser> {
        debug_assert!(self.entrust.is_none());
        match entrust.call(buffer) {
            Ok(item) => Ok(item),
            Err(e) => {
                self.entrust = Some(entrust.into());
                Err(e)
            }
        }
    }
}

impl RDBFileParser {
    /// Parse the next [`Item`], returning `Ok(None)` on `EOF`.
    pub fn poll_next(&mut self, buffer: &mut Buffer) -> AnyResult<Option<Item>> {
        // If we're currently waiting for a child parser to finish, give it a
        // chance to consume more data first.
        if let Some(entrust) = self.entrust.as_mut() {
            let item = entrust.call(buffer)?;
            self.entrust = None;
            return Ok(Some(item));
        }

        // Ensure the header has been parsed.
        if self.version == 0 {
            self.read_header(buffer).context("read header")?;
        }

        // Read the next flag byte from the stream.
        let input = buffer.as_ref();
        let (input, flag) = read_u8(input).context("read item flag")?;

        // First interpret it as an opcode (aux fields, select-db, etc.).
        if let Ok(opcode) = RDBOpcode::try_from(flag) {
            return match opcode {
                RDBOpcode::Aux => {
                    let (input, aux_key) = read_rdb_str(input).context("read aux key")?;
                    let (input, aux_val) = read_rdb_str(input).context("read aux val")?;
                    buffer.consume_to(input.as_ptr());
                    Ok(Some(Item::Aux {
                        key: aux_key,
                        val: aux_val,
                    }))
                }
                RDBOpcode::SelectDB => {
                    let (input, db) = read_rdb_len(input).context("read select db number")?;
                    let db = db.as_simple().context("db should be a simple number")?;
                    buffer.consume_to(input.as_ptr());
                    Ok(Some(Item::SelectDB { db }))
                }
                RDBOpcode::ResizeDB => {
                    let (input, table_size) =
                        read_rdb_len(input).context("read hash table size")?;
                    let (input, ttl_table_size) =
                        read_rdb_len(input).context("read ttl table size")?;
                    buffer.consume_to(input.as_ptr());
                    Ok(Some(Item::ResizeDB {
                        table_size: table_size
                            .as_simple()
                            .context("table size should be a simple number")?,
                        ttl_table_size: ttl_table_size
                            .as_simple()
                            .context("ttl table size should be a simple number")?,
                    }))
                }
                RDBOpcode::Eof => {
                    buffer.consume_to(input.as_ptr());
                    Ok(None)
                }
                _ => bail!("unsupported opcode: {:?} (raw: {:#04x})", opcode, flag),
            };
        }

        // If it's not an opcode, try to interpret it as a type ID.
        if let Ok(type_id) = RDBType::try_from(flag) {
            return match type_id {
                RDBType::String => {
                    let (input, entrust) = StringRecordParser::init(buffer.tell(), input)?;
                    buffer.consume_to(input.as_ptr());
                    let item = self.set_entrust(entrust, buffer)?;
                    Ok(Some(item))
                }
                RDBType::List => {
                    let (input, entrust) = ListRecordParser::init(buffer.tell(), input)?;
                    buffer.consume_to(input.as_ptr());
                    let item = self.set_entrust(entrust, buffer)?;
                    Ok(Some(item))
                }
                RDBType::ListZipList => {
                    let (input, entrust) = ZipListRecordParser::init(buffer.tell(), input)?;
                    buffer.consume_to(input.as_ptr());
                    let item = self.set_entrust(entrust, buffer)?;
                    Ok(Some(item))
                }
                RDBType::ListQuickList => {
                    let (input, entrust) = QuickListRecordParser::init(buffer.tell(), input)?;
                    buffer.consume_to(input.as_ptr());
                    let item = self.set_entrust(entrust, buffer)?;
                    Ok(Some(item))
                }
                _ => bail!("unsupported type: {:?} (raw: {:#04x})", type_id, flag),
            };
        }

        bail!("unknown RDB flag: {:#04x}", flag)
    }
}
