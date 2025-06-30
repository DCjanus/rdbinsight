use std::marker::ConstParamTy;

use num_enum::{IntoPrimitive, TryFromPrimitive};
use serde::{Deserialize, Serialize};

use crate::{impl_serde_str_conversion, parser::core::raw::RDBStr};

/// A single logical item emitted by the high-level [`RDBFileParser`].
#[derive(Debug, Clone)]
pub enum Item {
    Aux {
        key: RDBStr,
        val: RDBStr,
    },
    ModuleAux {
        rdb_size: u64,
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
        /// Size of the record in bytes.
        rdb_size: u64,
        encoding: StringEncoding,
    },
    ListRecord {
        key: RDBStr,
        rdb_size: u64,
        encoding: ListEncoding,
        member_count: u64,
    },
    SetRecord {
        key: RDBStr,
        /// Size of the record in bytes.
        rdb_size: u64,
        encoding: SetEncoding,
        member_count: u64,
    },
    ZSetRecord {
        key: RDBStr,
        /// Size of the record in bytes.
        rdb_size: u64,
        encoding: ZSetEncoding,
        /// Number of elements (member / score pairs).
        member_count: u64,
    },
    /// ZSet2 (double score) record.
    ZSet2Record {
        key: RDBStr,
        /// Size of the record in bytes.
        rdb_size: u64,
        encoding: ZSetEncoding,
        /// Number of elements (member / score pairs).
        member_count: u64,
    },
    /// Stream record backed by listpacks.
    StreamRecord {
        key: RDBStr,
        /// Size of the record in bytes.
        rdb_size: u64,
        /// Encoding variant.
        encoding: StreamEncoding,
        /// Number of messages contained in the stream.
        message_count: u64,
    },
    HashRecord {
        key: RDBStr,
        rdb_size: u64,
        encoding: HashEncoding,
        pair_count: u64,
    },
    ModuleRecord {
        key: RDBStr,
        rdb_size: u64,
    },
    ExpiryMs {
        expire_at_ms: u64,
    },
    Idle {
        idle_seconds: u64,
    },
    Freq {
        freq: u8,
    },
    FunctionRecord {
        rdb_size: u64,
    },
    SlotInfo {
        slot_id: u64,
        slot_size: u64,
        expires_slot_size: u64,
    },
}

impl_serde_str_conversion!(StringEncoding);
impl_serde_str_conversion!(ListEncoding);
impl_serde_str_conversion!(SetEncoding);
impl_serde_str_conversion!(ZSetEncoding);
impl_serde_str_conversion!(HashEncoding);
impl_serde_str_conversion!(StreamEncoding);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StringEncoding {
    Raw,
    Int,
    LZF,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ListEncoding {
    List,
    ZipList,
    QuickList,
    QuickList2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SetEncoding {
    Raw,
    IntSet,
    ListPack,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ZSetEncoding {
    SkipList,
    ZipList,
    /// ListPack-based encoding introduced in Redis 7.0+
    ListPack,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HashEncoding {
    Raw,
    ZipMap,
    ZipList,
    ListPack,
    Metadata,
    ListPackEx,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ConstParamTy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamEncoding {
    ListPacks,
    ListPacks2,
    ListPacks3,
}

/// Opcode of RDB, ref: https://github.com/redis/redis/blob/2ba81b70957691a6a010e785225672e6657e53e8/src/rdb.h#L93
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
pub enum RDBOpcode {
    SlotInfo = 244,      // RDB_OPCODE_SLOT_INFO
    Function2 = 245,     // RDB_OPCODE_FUNCTION2
    FunctionPreGA = 246, // RDB_OPCODE_FUNCTION_PRE_GA
    ModuleAux = 247,     // RDB_OPCODE_MODULE_AUX
    Idle = 248,          // RDB_OPCODE_IDLE
    Freq = 249,          // RDB_OPCODE_FREQ
    Aux = 250,           // RDB_OPCODE_AUX
    ResizeDB = 251,      // RDB_OPCODE_RESIZEDB
    ExpireTimeMs = 252,  // RDB_OPCODE_EXPIRETIME_MS
    ExpireTime = 253,    // RDB_OPCODE_EXPIRETIME
    SelectDB = 254,      // RDB_OPCODE_SELECTDB
    Eof = 255,           // RDB_OPCODE_EOF
}

/// Type of RDB, ref: https://github.com/redis/redis/blob/2ba81b70957691a6a010e785225672e6657e53e8/src/rdb.h#L100
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
pub enum RDBType {
    String = 0,               // RDB_TYPE_STRING
    List = 1,                 // RDB_TYPE_LIST
    Set = 2,                  // RDB_TYPE_SET
    ZSet = 3,                 // RDB_TYPE_ZSET
    Hash = 4,                 // RDB_TYPE_HASH
    ZSet2 = 5,                // RDB_TYPE_ZSET_2
    ModulePreGA = 6,          // RDB_TYPE_MODULE_PRE_GA
    Module2 = 7,              // RDB_TYPE_MODULE_2
    HashZipMap = 9,           // RDB_TYPE_HASH_ZIPMAP
    ListZipList = 10,         // RDB_TYPE_LIST_ZIPLIST
    SetIntSet = 11,           // RDB_TYPE_SET_INTSET
    ZSetZipList = 12,         // RDB_TYPE_ZSET_ZIPLIST
    HashZipList = 13,         // RDB_TYPE_HASH_ZIPLIST
    ListQuickList = 14,       // RDB_TYPE_LIST_QUICKLIST
    StreamListPacks = 15,     // RDB_TYPE_STREAM_LISTPACKS
    HashListPack = 16,        // RDB_TYPE_HASH_LISTPACK
    ZSetListPack = 17,        // RDB_TYPE_ZSET_LISTPACK
    ListQuickList2 = 18,      // RDB_TYPE_LIST_QUICKLIST_2
    StreamListPacks2 = 19,    // RDB_TYPE_STREAM_LISTPACKS_2
    SetListPack = 20,         // RDB_TYPE_SET_LISTPACK
    StreamListPacks3 = 21,    // RDB_TYPE_STREAM_LISTPACKS_3
    HashMetadataPreGA = 22,   // RDB_TYPE_HASH_METADATA_PRE_GA
    HashListPackExPreGA = 23, // RDB_TYPE_HASH_LISTPACK_EX_PRE_GA
    HashMetadata = 24,        // RDB_TYPE_HASH_METADATA
    HashListPackEx = 25,      // RDB_TYPE_HASH_LISTPACK_EX
}

/// Module serialized values sub opcodes, ref: https://github.com/redis/redis/blob/2ba81b70957691a6a010e785225672e6657e53e8/src/rdb.h#L133
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
pub enum RDBModuleOpcode {
    Eof = 0,    // RDB_MODULE_OPCODE_EOF
    SInt = 1,   // RDB_MODULE_OPCODE_SINT
    UInt = 2,   // RDB_MODULE_OPCODE_UINT
    Float = 3,  // RDB_MODULE_OPCODE_FLOAT
    Double = 4, // RDB_MODULE_OPCODE_DOUBLE
    String = 5, // RDB_MODULE_OPCODE_STRING
}
