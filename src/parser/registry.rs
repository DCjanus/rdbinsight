use crate::parser::model::{RDBOpcode, RDBType};

pub const OPCODE_REGISTRY: &[RDBOpcode] = &[
    RDBOpcode::Aux,
    RDBOpcode::SelectDB,
    RDBOpcode::ResizeDB,
    RDBOpcode::Eof,
    RDBOpcode::SlotInfo,
    RDBOpcode::Function2,
    RDBOpcode::FunctionPreGA,
    RDBOpcode::ModuleAux,
    RDBOpcode::Idle,
    RDBOpcode::Freq,
    RDBOpcode::ExpireTimeMs,
    RDBOpcode::ExpireTime,
];

pub const TYPE_REGISTRY: &[RDBType] = &[
    RDBType::String,
    RDBType::List,
    RDBType::Set,
    RDBType::ZSet,
    RDBType::Hash,
    RDBType::ZSet2,
    RDBType::ModulePreGA,
    RDBType::Module2,
    RDBType::HashZipMap,
    RDBType::ListZipList,
    RDBType::SetIntSet,
    RDBType::ZSetZipList,
    RDBType::HashZipList,
    RDBType::ListQuickList,
    RDBType::StreamListPacks,
    RDBType::HashListPack,
    RDBType::ZSetListPack,
    RDBType::ListQuickList2,
    RDBType::StreamListPacks2,
    RDBType::SetListPack,
    RDBType::StreamListPacks3,
    RDBType::HashMetadataPreGA,
    RDBType::HashListPackExPreGA,
    RDBType::HashMetadata,
    RDBType::HashListPackEx,
];

pub fn resolve_opcode(flag: u8) -> Option<RDBOpcode> {
    OPCODE_REGISTRY
        .iter()
        .copied()
        .find(|opcode| *opcode as u8 == flag)
}

pub fn resolve_type(flag: u8) -> Option<RDBType> {
    TYPE_REGISTRY
        .iter()
        .copied()
        .find(|type_id| *type_id as u8 == flag)
}
