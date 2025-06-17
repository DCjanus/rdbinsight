use anyhow::{anyhow, bail};
use bytes::Bytes;

use crate::{
    helper::AnyResult,
    parser::core::combinators::{read_be_u16, read_be_u32, read_be_u64, read_exact, read_u8},
};

#[derive(Clone, Hash, Debug)]
pub enum RDBLen {
    Simple(u64),
    IntStr(u64),
    LZFStr,
}

impl RDBLen {
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            RDBLen::Simple(len) => Some(*len),
            RDBLen::IntStr(len) => Some(*len),
            _ => None,
        }
    }
}

pub fn read_rdb_len(input: &[u8]) -> AnyResult<(&[u8], RDBLen)> {
    let (input, first_byte) = read_u8(input)?;
    match first_byte {
        0b0000_0000..=0b0011_1111 => Ok((input, RDBLen::Simple(first_byte as u64))),
        0b0100_0000..=0b0111_1111 => {
            let (input, second_byte) = read_u8(input)?;
            let mut output = [0u8; 8];
            output[6] = first_byte & 0b0011_1111;
            output[7] = second_byte;
            Ok((input, RDBLen::Simple(u64::from_be_bytes(output))))
        }
        0b1000_0000 => {
            let (input, ret) = read_be_u32(input)?;
            Ok((input, RDBLen::Simple(ret as u64)))
        }
        0b1000_0001 => {
            let (input, ret) = read_be_u64(input)?;
            Ok((input, RDBLen::Simple(ret)))
        }
        0b1100_0000 => {
            let (input, ret) = read_u8(input)?;
            Ok((input, RDBLen::IntStr(ret as u64)))
        }
        0b1100_0001 => {
            let (input, ret) = read_be_u16(input)?;
            Ok((input, RDBLen::IntStr(ret as u64)))
        }
        0b1100_0010 => {
            let (input, ret) = read_be_u32(input)?;
            Ok((input, RDBLen::IntStr(ret as u64)))
        }
        0b1100_0011 => Ok((input, RDBLen::LZFStr)),
        _ => bail!("Invalid length leading byte: {:02x}", first_byte),
    }
}

#[derive(Clone, Hash, Debug, PartialEq, Eq)]
pub enum RDBStr {
    Str(Bytes), /* XXX: tricky way to avoid lifetime issue, might be slow than &[u8], but easy to use */
    Int(u64),
}

pub fn read_rdb_str(input: &[u8]) -> AnyResult<(&[u8], RDBStr)> {
    let (input, len) = read_rdb_len(input)?;
    match len {
        RDBLen::Simple(len) => {
            let (input, str) = read_exact(input, len as usize)?;
            Ok((input, RDBStr::Str(Bytes::copy_from_slice(str))))
        }
        RDBLen::IntStr(len) => Ok((input, RDBStr::Int(len))),
        RDBLen::LZFStr => {
            let (input, in_len) = read_rdb_len(input)?;
            let in_len = in_len.as_u64().ok_or_else(|| {
                anyhow!("Invalid input length for LZFStr, expected simple or integer length")
            })?;
            let (input, out_len) = read_rdb_len(input)?;
            let out_len = out_len.as_u64().ok_or_else(|| {
                anyhow!("Invalid output length for LZFStr, expected simple or integer length")
            })?;

            let (input, compressed) = read_exact(input, in_len as usize)?;
            let decompressed = lzf::decompress(compressed, out_len as usize)
                .map_err(|e| anyhow!("Failed to decompress LZFStr: {}", e))?;
            Ok((input, RDBStr::Str(Bytes::from(decompressed))))
        }
    }
}
