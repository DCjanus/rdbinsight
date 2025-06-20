//! Parsers for RDB Module records (type id = 6 / 7).

use std::convert::TryFrom;

use anyhow::{Context, ensure};

use crate::{
    helper::AnyResult,
    parser::{
        core::{
            buffer::Buffer,
            combinators::read_exact,
            raw::{RDBStr, read_rdb_len, read_rdb_str},
        },
        model::{Item, RDBModuleOpcode},
        record::string::StringEncodingParser,
        state::traits::{InitializableParser, StateParser},
    },
};

pub struct Module2RecordParser {
    started: u64,
    key: RDBStr,
    entrust: Option<StringEncodingParser>,
}

impl InitializableParser for Module2RecordParser {
    fn init<'a>(buffer: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let (input, key) = read_rdb_str(input).context("read key")?;
        let (input, _module_id) = read_rdb_len(input).context("read module id")?;

        Ok((input, Self {
            started: buffer.tell(),
            key,
            entrust: None,
        }))
    }
}

impl Module2RecordParser {
    #[inline]
    fn read_module_opcode(input: &[u8]) -> AnyResult<(&[u8], RDBModuleOpcode)> {
        let (input, opcode) = read_rdb_len(input)?;
        let opcode = opcode
            .as_u64()
            .ok_or_else(|| anyhow::anyhow!("module opcode should be a number"))?;
        let opcode = RDBModuleOpcode::try_from(opcode as u8)
            .map_err(|_| anyhow::anyhow!("unknown module opcode: {}", opcode))?;
        Ok((input, opcode))
    }
}

impl StateParser for Module2RecordParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        loop {
            // Drain delegated parser if present (used for STRING opcodes).
            if let Some(entrust) = &mut self.entrust {
                entrust.call(buffer)?;
                self.entrust = None;
            }

            let input = buffer.as_ref();
            let (input, opcode) = Self::read_module_opcode(input)?;
            match opcode {
                RDBModuleOpcode::Eof => {
                    buffer.consume_to(input.as_ptr());
                    crate::parser_trace!("module2.raw");
                    return Ok(Item::ModuleRecord {
                        key: self.key.clone(),
                        rdb_size: buffer.tell() - self.started,
                    });
                }
                RDBModuleOpcode::SInt | RDBModuleOpcode::UInt => {
                    let (input, _) = read_rdb_len(input)?;
                    buffer.consume_to(input.as_ptr());
                }
                RDBModuleOpcode::Float => {
                    let (input, _) = read_exact(input, 4)?;
                    buffer.consume_to(input.as_ptr());
                }
                RDBModuleOpcode::Double => {
                    let (input, _) = read_exact(input, 8)?;
                    buffer.consume_to(input.as_ptr());
                }
                RDBModuleOpcode::String => {
                    let (input, entrust) = StringEncodingParser::init(buffer, input)?;
                    self.entrust = Some(entrust);
                    buffer.consume_to(input.as_ptr());
                }
            }
        }
    }
}

/// Parser for RDB_OPCODE_MODULE_AUX (opcode = 247)
pub struct ModuleAuxParser {
    started: u64,
    entrust: Option<StringEncodingParser>,
}

impl InitializableParser for ModuleAuxParser {
    fn init<'a>(buffer: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let (input, _module_id) = read_rdb_len(input)?;
        let (input, opcode) = Self::read_module_opcode(input)?;
        ensure!(opcode == RDBModuleOpcode::UInt);
        let (input, _when) = read_rdb_len(input)?;
        Ok((input, Self {
            started: buffer.tell(),
            entrust: None,
        }))
    }
}

impl ModuleAuxParser {
    fn read_module_opcode(input: &[u8]) -> AnyResult<(&[u8], RDBModuleOpcode)> {
        let (input, opcode) = read_rdb_len(input)?;
        let opcode = opcode
            .as_u64()
            .ok_or_else(|| anyhow::anyhow!("module aux opcode should be a number"))?;
        let opcode = RDBModuleOpcode::try_from(opcode as u8)
            .map_err(|_| anyhow::anyhow!("unknown module aux opcode: {}", opcode))?;
        Ok((input, opcode))
    }
}

impl StateParser for ModuleAuxParser {
    type Output = Item;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        loop {
            if let Some(entrust) = &mut self.entrust {
                entrust.call(buffer)?;
                self.entrust = None;
            }

            let input = buffer.as_ref();
            let (input, opcode) = Self::read_module_opcode(input)?;
            match opcode {
                RDBModuleOpcode::Eof => {
                    buffer.consume_to(input.as_ptr());
                    return Ok(Item::ModuleAux {
                        rdb_size: buffer.tell() - self.started,
                    });
                }
                RDBModuleOpcode::SInt => {
                    let (input, _) = read_rdb_len(input)?;
                    buffer.consume_to(input.as_ptr());
                }
                RDBModuleOpcode::UInt => {
                    let (input, _) = read_rdb_len(input)?;
                    buffer.consume_to(input.as_ptr());
                }
                RDBModuleOpcode::Float => {
                    let (input, _) = read_exact(input, 4)?;
                    buffer.consume_to(input.as_ptr());
                }
                RDBModuleOpcode::Double => {
                    let (input, _) = read_exact(input, 8)?;
                    buffer.consume_to(input.as_ptr());
                }
                RDBModuleOpcode::String => {
                    let (input, entrust) = StringEncodingParser::init(buffer, input)?;
                    self.entrust = Some(entrust);
                    buffer.consume_to(input.as_ptr());
                }
            };
        }
    }
}
