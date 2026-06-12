use anyhow::{Context, bail, ensure};
use spire_enum::prelude::{delegate_impl, delegated_enum};
use tracing::debug;

use crate::{
    helper::AnyResult,
    parser::{
        core::{
            buffer::Buffer,
            combinators::{read_exact, read_le_u32, read_le_u64, read_tag, read_u8},
            cursor::Cursor,
            raw::{read_rdb_len, read_rdb_str},
        },
        model::{Item, RDBOpcode, RDBType, StreamEncoding},
        record::{
            function::Function2RecordParser,
            hash::{
                HashListPackExRecordParser, HashListPackRecordParser, HashMetadataRecordParser,
                HashRecordParser, HashZipListRecordParser, HashZipMapRecordParser,
            },
            list::{
                ListQuickList2RecordParser, ListQuickListRecordParser, ListRecordParser,
                ListZipListRecordParser,
            },
            module::{Module2RecordParser, ModuleAuxParser},
            set::{SetIntSetRecordParser, SetListPackRecordParser, SetRecordParser},
            stream::StreamListPackRecordParser,
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
    String(StringRecordParser),
    List(ListRecordParser),
    ListZipList(ListZipListRecordParser),
    ListQuickList(ListQuickListRecordParser),
    ListQuickList2(ListQuickList2RecordParser),
    StreamListPack(StreamListPackRecordParser<{ StreamEncoding::ListPacks }>),
    StreamListPack2(StreamListPackRecordParser<{ StreamEncoding::ListPacks2 }>),
    StreamListPack3(StreamListPackRecordParser<{ StreamEncoding::ListPacks3 }>),
    StreamListPack4(StreamListPackRecordParser<{ StreamEncoding::ListPacks4 }>),
    Set(SetRecordParser),
    SetIntSet(SetIntSetRecordParser),
    SetListPack(SetListPackRecordParser),
    ZSet(ZSetRecordParser),
    ZSetZipList(ZSetZipListRecordParser),
    ZSet2(ZSet2RecordParser),
    ZSetListPack(ZSetListPackRecordParser),
    Hash(HashRecordParser),
    HashZipMap(HashZipMapRecordParser),
    HashZipList(HashZipListRecordParser),
    HashListPack(HashListPackRecordParser),
    HashListPackEx(HashListPackExRecordParser),
    Module2(Module2RecordParser),
    ModuleAux(ModuleAuxParser),
    Function2(Function2RecordParser),
    HashMetadata(HashMetadataRecordParser),
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
    // Add state tracking for better error reporting
    current_db: Option<u64>,
    items_parsed: u64,
}

impl RDBFileParser {
    fn read_header(&mut self, buffer: &mut Buffer) -> AnyResult<()> {
        let input = buffer.as_slice();
        let input = read_tag(input, b"REDIS").context("read magic number")?;
        let (input, version) = read_exact(input, 4)?;
        let version = std::str::from_utf8(version).context("version should be utf8")?;
        let version: u64 = version.parse().context("version should be a number")?;
        ensure!(version >= 1, "version should be >= 1");
        ensure!(version <= 13, "version should be <= 13");

        self.version = version;
        buffer.consume_to(input.as_ptr());
        Ok(())
    }

    // Helper method to return an item and increment the counter
    fn return_item(&mut self, item: Item) -> AnyResult<Option<Item>> {
        self.items_parsed += 1;
        Ok(Some(item))
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

    fn init_and_run<E>(
        &mut self,
        buffer: &mut Buffer,
        input_offset: usize,
    ) -> AnyResult<Option<Item>>
    where
        E: InitializableParser + StateParser<Output = Item> + Into<ItemParser>,
    {
        let entrust = {
            let mut cursor = Cursor::new(buffer);
            cursor.init_commit_from_offset::<E>(input_offset)?
        };
        let item = self.set_entrust(entrust, buffer)?;
        self.return_item(item)
    }
}

impl RDBFileParser {
    pub fn poll_next(&mut self, buffer: &mut Buffer) -> AnyResult<Option<Item>> {
        if let Some(entrust) = self.entrust.as_mut() {
            let item = entrust.call(buffer)?;
            self.entrust = None;
            return self.return_item(item);
        }

        if self.version == 0 {
            // TODO: init RDBFileParser with entrust with RDBFileHeaderParser, to skip this branch
            self.read_header(buffer).context("read header")?;
        }

        let input = buffer.as_slice();
        let (input, flag) = read_u8(input).context("read item flag")?;
        let child_input_offset = buffer.len() - input.len();

        // First interpret it as an opcode (aux fields, select-db, etc.).
        if let Ok(opcode) = RDBOpcode::try_from(flag) {
            return match opcode {
                RDBOpcode::Aux => {
                    let (input, aux_key) = read_rdb_str(input).context("read aux key")?;
                    let (input, aux_val) = read_rdb_str(input).context("read aux val")?;
                    buffer.consume_to(input.as_ptr());
                    debug!("aux field: {aux_key} = {aux_val}");
                    self.return_item(Item::Aux {
                        key: aux_key,
                        val: aux_val,
                    })
                }
                RDBOpcode::SelectDB => {
                    let (input, db) = read_rdb_len(input).context("read select db number")?;
                    let db = db.as_u64().context("db should be a number")?;
                    buffer.consume_to(input.as_ptr());
                    self.current_db = Some(db);
                    self.return_item(Item::SelectDB { db })
                }
                RDBOpcode::ResizeDB => {
                    let (input, table_size) =
                        read_rdb_len(input).context("read hash table size")?;
                    let (input, ttl_table_size) =
                        read_rdb_len(input).context("read ttl table size")?;
                    buffer.consume_to(input.as_ptr());
                    self.return_item(Item::ResizeDB {
                        table_size: table_size
                            .as_u64()
                            .context("table size should be a number")?,
                        ttl_table_size: ttl_table_size
                            .as_u64()
                            .context("ttl table size should be a number")?,
                    })
                }
                RDBOpcode::Eof => {
                    if buffer.is_finished() && input.is_empty() {
                        debug!("EOF opcode, buffer is finished and input is empty");
                        ensure!(input.is_empty(), "input should be empty after EOF checksum");
                        buffer.consume_to(input.as_ptr());
                        return Ok(None);
                    }

                    let (input, _checksum) = read_exact(input, 8)?;
                    debug!(
                        operation = "rdb_eof_checksum",
                        checksum = ?_checksum,
                        "EOF opcode encountered with checksum"
                    );
                    ensure!(input.is_empty(), "input should be empty after EOF checksum");
                    // TODO: check checksum
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
                    self.return_item(Item::SlotInfo {
                        slot_id,
                        slot_size,
                        expires_slot_size,
                    })
                }
                RDBOpcode::Function2 => {
                    self.init_and_run::<Function2RecordParser>(buffer, child_input_offset)
                }
                RDBOpcode::FunctionPreGA => bail!("not supported opcode: FunctionPreGA"),
                RDBOpcode::ModuleAux => {
                    self.init_and_run::<ModuleAuxParser>(buffer, child_input_offset)
                }
                RDBOpcode::Idle => {
                    let (input, idle_seconds) = read_rdb_len(input).context("read idle seconds")?;
                    let idle_seconds = idle_seconds
                        .as_u64()
                        .context("idle seconds should be a number")?;
                    buffer.consume_to(input.as_ptr());
                    self.return_item(Item::Idle { idle_seconds })
                }
                RDBOpcode::Freq => {
                    let (input, freq) = read_u8(input).context("read freq")?;
                    buffer.consume_to(input.as_ptr());
                    self.return_item(Item::Freq { freq })
                }
                RDBOpcode::ExpireTimeMs => {
                    let (input, expire_at_ms) =
                        read_le_u64(input).context("read expire time ms")?;
                    buffer.consume_to(input.as_ptr());
                    crate::parser_trace!("expiry.ms");
                    self.return_item(Item::ExpiryMs { expire_at_ms })
                }
                RDBOpcode::ExpireTime => {
                    // since RDB 3.0, Redis save expire time in milliseconds instead of seconds
                    // ref: https://github.com/redis/redis/commit/7dcc10b65e0075fccc90d93bac5b078baefdbb07#diff-c77a3d2b15213159471dad3359f23629c2297c3579861945e94ff05c34bb3d7dL572
                    let (input, expire_at_s) =
                        read_le_u32(input).context("read expire time seconds")?;
                    let expire_at_ms = expire_at_s as u64 * 1000;
                    buffer.consume_to(input.as_ptr());
                    crate::parser_trace!("expiry.s");
                    self.return_item(Item::ExpiryMs { expire_at_ms })
                }
            };
        }

        // If it's not an opcode, try to interpret it as a type ID.
        if let Ok(type_id) = RDBType::try_from(flag) {
            return match type_id {
                RDBType::String => {
                    self.init_and_run::<StringRecordParser>(buffer, child_input_offset)
                }
                RDBType::List => self.init_and_run::<ListRecordParser>(buffer, child_input_offset),
                RDBType::ListZipList => {
                    self.init_and_run::<ListZipListRecordParser>(buffer, child_input_offset)
                }
                RDBType::ListQuickList => {
                    self.init_and_run::<ListQuickListRecordParser>(buffer, child_input_offset)
                }
                RDBType::ListQuickList2 => {
                    self.init_and_run::<ListQuickList2RecordParser>(buffer, child_input_offset)
                }
                RDBType::Set => self.init_and_run::<SetRecordParser>(buffer, child_input_offset),
                RDBType::SetIntSet => {
                    self.init_and_run::<SetIntSetRecordParser>(buffer, child_input_offset)
                }
                RDBType::SetListPack => {
                    self.init_and_run::<SetListPackRecordParser>(buffer, child_input_offset)
                }
                RDBType::ZSet => self.init_and_run::<ZSetRecordParser>(buffer, child_input_offset),
                RDBType::ZSet2 => {
                    self.init_and_run::<ZSet2RecordParser>(buffer, child_input_offset)
                }
                RDBType::ZSetListPack => {
                    self.init_and_run::<ZSetListPackRecordParser>(buffer, child_input_offset)
                }
                RDBType::Hash => self.init_and_run::<HashRecordParser>(buffer, child_input_offset),
                RDBType::ModulePreGA => bail!("not supported type: ModulePreGA"),
                RDBType::Module2 => {
                    self.init_and_run::<Module2RecordParser>(buffer, child_input_offset)
                }
                RDBType::HashZipMap => {
                    self.init_and_run::<HashZipMapRecordParser>(buffer, child_input_offset)
                }
                RDBType::HashZipList => {
                    self.init_and_run::<HashZipListRecordParser>(buffer, child_input_offset)
                }
                RDBType::HashListPack => {
                    self.init_and_run::<HashListPackRecordParser>(buffer, child_input_offset)
                }
                RDBType::HashListPackEx => {
                    self.init_and_run::<HashListPackExRecordParser>(buffer, child_input_offset)
                }
                RDBType::HashMetadata => {
                    self.init_and_run::<HashMetadataRecordParser>(buffer, child_input_offset)
                }
                RDBType::StreamListPacks => self.init_and_run::<StreamListPackRecordParser<
                    { StreamEncoding::ListPacks },
                >>(buffer, child_input_offset),
                RDBType::StreamListPacks2 => self.init_and_run::<StreamListPackRecordParser<
                    { StreamEncoding::ListPacks2 },
                >>(buffer, child_input_offset),
                RDBType::StreamListPacks3 => self.init_and_run::<StreamListPackRecordParser<
                    { StreamEncoding::ListPacks3 },
                >>(buffer, child_input_offset),
                RDBType::StreamListPacks4 => self.init_and_run::<StreamListPackRecordParser<
                    { StreamEncoding::ListPacks4 },
                >>(buffer, child_input_offset),
                RDBType::HashMetadataPreGA => bail!("unsupported type: HashMetadataPreGA"),
                RDBType::HashListPackExPreGA => bail!("unsupported type: HashListPackExPreGA"),
                RDBType::ZSetZipList => {
                    self.init_and_run::<ZSetZipListRecordParser>(buffer, child_input_offset)
                }
            };
        }

        // Enhanced error message with diagnostic information
        let buffer_position = buffer.tell();
        let remaining_bytes = input.len();
        let context_bytes = &input[..remaining_bytes.min(16)]; // Show up to 16 bytes of context
        let hex_context = context_bytes
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<Vec<_>>()
            .join(" ");

        let db_info = match self.current_db {
            Some(db) => format!(" (current DB: {db})"),
            None => " (no DB selected)".to_string(),
        };

        bail!(
            "unknown RDB flag: {:#04x} (decimal: {}) at buffer position {}, RDB version: {}, items parsed: {}{}, remaining bytes: {}, context: [{}]",
            flag,
            flag,
            buffer_position,
            self.version,
            self.items_parsed,
            db_info,
            remaining_bytes,
            hex_context
        );
    }
}
