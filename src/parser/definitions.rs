use num_enum::{IntoPrimitive, TryFromPrimitive};

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
