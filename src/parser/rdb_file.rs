use anyhow::{Context, bail, ensure};
use spire_enum::prelude::{delegate_impl, delegated_enum};

use crate::{
    helper::AnyResult,
    parser::{
        core::{
            buffer::Buffer,
            combinators::{read_exact, read_le_u32, read_le_u64, read_tag, read_u8},
            raw::{read_rdb_len, read_rdb_str},
        },
        model::{Item, RDBOpcode, RDBType},
        record::{
            function::Function2RecordParser,
            hash::{
                HashListPackRecordParser, HashRecordParser, HashZipListRecordParser,
                HashZipMapRecordParser,
            },
            list::{
                ListQuickList2RecordParser, ListQuickListRecordParser, ListRecordParser,
                ListZipListRecordParser,
            },
            module::{Module2RecordParser, ModuleAuxParser},
            set::{SetIntSetRecordParser, SetListPackRecordParser, SetRecordParser},
            string::StringRecordParser,
            zset::{
                ZSet2RecordParser, ZSetListPackRecordParser, ZSetRecordParser,
                ZSetZipListRecordParser,
            },
        },
        state::traits::{InitializableParser, StateParser},
    },
};

#[delegated_enum(impl_variants_into_enum)]
enum ItemParser {
    StringRecord(StringRecordParser),
    ListRecord(ListRecordParser),
    ListZipListRecord(ListZipListRecordParser),
    ListQuickListRecord(ListQuickListRecordParser),
    ListQuickList2Record(ListQuickList2RecordParser),
    SetRecord(SetRecordParser),
    SetIntSetRecord(SetIntSetRecordParser),
    SetListPackRecord(SetListPackRecordParser),
    ZSetRecord(ZSetRecordParser),
    ZSetZipListRecord(ZSetZipListRecordParser),
    ZSet2Record(ZSet2RecordParser),
    ZSetListPackRecord(ZSetListPackRecordParser),
    HashRecord(HashRecordParser),
    HashZipMapRecord(HashZipMapRecordParser),
    HashZipListRecord(HashZipListRecordParser),
    HashListPackRecord(HashListPackRecordParser),
    Module2Record(Module2RecordParser),
    ModuleAuxRecord(ModuleAuxParser),
    Function2Record(Function2RecordParser),
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
    pub fn poll_next(&mut self, buffer: &mut Buffer) -> AnyResult<Option<Item>> {
        if let Some(entrust) = self.entrust.as_mut() {
            let item = entrust.call(buffer)?;
            self.entrust = None;
            return Ok(Some(item));
        }

        if self.version == 0 {
            // TODO: init RDBFileParser with entrust with RDBFileHeaderParser, to skip this branch
            self.read_header(buffer).context("read header")?;
        }

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
                    let db = db.as_u64().context("db should be a number")?;
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
                            .as_u64()
                            .context("table size should be a number")?,
                        ttl_table_size: ttl_table_size
                            .as_u64()
                            .context("ttl table size should be a number")?,
                    }))
                }
                RDBOpcode::Eof => {
                    buffer.consume_to(input.as_ptr());
                    Ok(None)
                }
                RDBOpcode::SlotInfo => {
                    let (input, slot_id) = read_rdb_len(input).context("read slot count")?;
                    let slot_id = slot_id.as_u64().context("slot id should be a number")?;
                    let (input, slot_size) = read_rdb_len(input).context("read slot size")?;
                    let slot_size = slot_size.as_u64().context("slot size should be a number")?;
                    let (input, expires_slot_size) =
                        read_rdb_len(input).context("read expires slot size")?;
                    let expires_slot_size = expires_slot_size
                        .as_u64()
                        .context("expires slot size should be a number")?;
                    buffer.consume_to(input.as_ptr());
                    Ok(Some(Item::SlotInfo {
                        slot_id,
                        slot_size,
                        expires_slot_size,
                    }))
                }
                RDBOpcode::Function2 => {
                    let (input, entrust) = Function2RecordParser::init(buffer.tell(), input)?;
                    buffer.consume_to(input.as_ptr());
                    let item = self.set_entrust(entrust, buffer)?;
                    Ok(Some(item))
                }
                RDBOpcode::FunctionPreGA => bail!("not supported opcode: FunctionPreGA"),
                RDBOpcode::ModuleAux => {
                    let (input, entrust) = ModuleAuxParser::init(buffer.tell(), input)?;
                    buffer.consume_to(input.as_ptr());
                    let item = self.set_entrust(entrust, buffer)?;
                    Ok(Some(item))
                }
                RDBOpcode::Idle => {
                    let (input, idle_seconds) = read_rdb_len(input).context("read idle seconds")?;
                    let idle_seconds = idle_seconds
                        .as_u64()
                        .context("idle seconds should be a number")?;
                    buffer.consume_to(input.as_ptr());
                    Ok(Some(Item::Idle { idle_seconds }))
                }
                RDBOpcode::Freq => {
                    let (input, freq) = read_u8(input).context("read freq")?;
                    buffer.consume_to(input.as_ptr());
                    Ok(Some(Item::Freq { freq }))
                }
                RDBOpcode::ExpireTimeMs => {
                    let (input, expire_at_ms) =
                        read_le_u64(input).context("read expire time ms")?;
                    buffer.consume_to(input.as_ptr());
                    crate::parser_trace!("expiry.ms");
                    Ok(Some(Item::ExpiryMs { expire_at_ms }))
                }
                RDBOpcode::ExpireTime => {
                    // since RDB 3.0, Redis save expire time in milliseconds instead of seconds
                    // ref: https://github.com/redis/redis/commit/7dcc10b65e0075fccc90d93bac5b078baefdbb07#diff-c77a3d2b15213159471dad3359f23629c2297c3579861945e94ff05c34bb3d7dL572
                    let (input, expire_at_s) =
                        read_le_u32(input).context("read expire time seconds")?;
                    let expire_at_ms = expire_at_s as u64 * 1000;
                    buffer.consume_to(input.as_ptr());
                    crate::parser_trace!("expiry.s");
                    Ok(Some(Item::ExpiryMs { expire_at_ms }))
                }
            };
        }

        // If it's not an opcode, try to interpret it as a type ID.
        if let Ok(type_id) = RDBType::try_from(flag) {
            return match type_id {
                RDBType::String => {
                    let (input, entrust) = StringRecordParser::init(buffer, input)?;
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
                    let (input, entrust) = ListZipListRecordParser::init(buffer.tell(), input)?;
                    buffer.consume_to(input.as_ptr());
                    let item = self.set_entrust(entrust, buffer)?;
                    Ok(Some(item))
                }
                RDBType::ListQuickList => {
                    let (input, entrust) = ListQuickListRecordParser::init(buffer.tell(), input)?;
                    buffer.consume_to(input.as_ptr());
                    let item = self.set_entrust(entrust, buffer)?;
                    Ok(Some(item))
                }
                RDBType::ListQuickList2 => {
                    let (input, entrust) = ListQuickList2RecordParser::init(buffer, input)?;
                    buffer.consume_to(input.as_ptr());
                    let item = self.set_entrust(entrust, buffer)?;
                    Ok(Some(item))
                }
                RDBType::Set => {
                    let (input, entrust) = SetRecordParser::init(buffer.tell(), input)?;
                    buffer.consume_to(input.as_ptr());
                    let item = self.set_entrust(entrust, buffer)?;
                    Ok(Some(item))
                }
                RDBType::SetIntSet => {
                    let (input, entrust) = SetIntSetRecordParser::init(buffer.tell(), input)?;
                    buffer.consume_to(input.as_ptr());
                    let item = self.set_entrust(entrust, buffer)?;
                    Ok(Some(item))
                }
                RDBType::SetListPack => {
                    let (input, entrust) = SetListPackRecordParser::init(buffer, input)?;
                    buffer.consume_to(input.as_ptr());
                    let item = self.set_entrust(entrust, buffer)?;
                    Ok(Some(item))
                }
                RDBType::ZSet => {
                    let (input, entrust) = ZSetRecordParser::init(buffer.tell(), input)?;
                    buffer.consume_to(input.as_ptr());
                    let item = self.set_entrust(entrust, buffer)?;
                    Ok(Some(item))
                }
                RDBType::ZSet2 => {
                    let (input, entrust) = ZSet2RecordParser::init(buffer.tell(), input)?;
                    buffer.consume_to(input.as_ptr());
                    let item = self.set_entrust(entrust, buffer)?;
                    Ok(Some(item))
                }
                RDBType::ZSetListPack => {
                    let (input, entrust) = ZSetListPackRecordParser::init(buffer, input)?;
                    buffer.consume_to(input.as_ptr());
                    let item = self.set_entrust(entrust, buffer)?;
                    Ok(Some(item))
                }
                RDBType::Hash => {
                    let (input, entrust) = HashRecordParser::init(buffer.tell(), input)?;
                    buffer.consume_to(input.as_ptr());
                    let item = self.set_entrust(entrust, buffer)?;
                    Ok(Some(item))
                }
                RDBType::ModulePreGA => bail!("not supported type: ModulePreGA"),
                RDBType::Module2 => {
                    let (input, entrust) = Module2RecordParser::init(buffer.tell(), input)?;
                    buffer.consume_to(input.as_ptr());
                    let item = self.set_entrust(entrust, buffer)?;
                    Ok(Some(item))
                }
                RDBType::HashZipMap => {
                    let (input, entrust) = HashZipMapRecordParser::init(buffer, input)?;
                    buffer.consume_to(input.as_ptr());
                    let item = self.set_entrust(entrust, buffer)?;
                    Ok(Some(item))
                }
                RDBType::HashZipList => {
                    let (input, entrust) = HashZipListRecordParser::init(buffer.tell(), input)?;
                    buffer.consume_to(input.as_ptr());
                    let item = self.set_entrust(entrust, buffer)?;
                    Ok(Some(item))
                }
                RDBType::HashListPack => {
                    let (input, entrust) = HashListPackRecordParser::init(buffer, input)?;
                    buffer.consume_to(input.as_ptr());
                    let item = self.set_entrust(entrust, buffer)?;
                    Ok(Some(item))
                }
                RDBType::StreamListPacks => todo!("unsupported type: StreamListPacks"),
                RDBType::StreamListPacks2 => todo!("unsupported type: StreamListPacks2"),
                RDBType::StreamListPacks3 => todo!("unsupported type: StreamListPacks3"),
                RDBType::HashMetadataPreGA => todo!("unsupported type: HashMetadataPreGA"),
                RDBType::HashListPackExPreGA => todo!("unsupported type: HashListPackExPreGA"),
                RDBType::HashMetadata => todo!("unsupported type: HashMetadata"),
                RDBType::HashListPackEx => todo!("unsupported type: HashListPackEx"),
                RDBType::ZSetZipList => {
                    let (input, entrust) = ZSetZipListRecordParser::init(buffer.tell(), input)?;
                    buffer.consume_to(input.as_ptr());
                    let item = self.set_entrust(entrust, buffer)?;
                    Ok(Some(item))
                }
            };
        }

        bail!("unknown RDB flag: {:#04x}", flag)
    }
}
