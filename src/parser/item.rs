use crate::parser::rdb_parsers::RDBStr;

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
    HashRecord {
        key: RDBStr,
        rdb_size: u64,
        encoding: HashEncoding,
        field_count: u64,
    },
    ModuleRecord {
        key: RDBStr,
        rdb_size: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StringEncoding {
    Raw,
    Int,
    LZF,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListEncoding {
    List,
    ZipList,
    QuickList,
    QuickList2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetEncoding {
    Raw,
    IntSet,
    ListPack,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZSetEncoding {
    SkipList,
    ZipList,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashEncoding {
    Raw,
    ZipMap,
    ZipList,
    ListPack,
}
