use std::fmt::Display;

use anyhow::{anyhow, bail};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

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

#[derive(Clone, Hash, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RDBStr {
    Str(Bytes), /* XXX: tricky way to avoid lifetime issue, might be slow than &[u8], but easy to use */
    Int(u64),
}

impl PartialOrd for RDBStr {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RDBStr {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (RDBStr::Str(a), RDBStr::Str(b)) => a.as_ref().cmp(b.as_ref()),
            (RDBStr::Str(a), RDBStr::Int(b)) => {
                let b_bytes = Bytes::from(b.to_string());
                a.cmp(&b_bytes)
            }
            (RDBStr::Int(a), RDBStr::Str(b)) => {
                let a_bytes = Bytes::from(a.to_string());
                a_bytes.cmp(b)
            }
            (RDBStr::Int(a), RDBStr::Int(b)) => {
                let a_bytes = Bytes::from(a.to_string());
                let b_bytes = Bytes::from(b.to_string());
                a_bytes.cmp(&b_bytes)
            }
        }
    }
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

impl Display for RDBStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RDBStr::Str(s) => write!(f, "{}", s.escape_ascii()),
            RDBStr::Int(i) => write!(f, "{}", i),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn str_vs_str_ordering() {
        let a = RDBStr::Str(Bytes::from("a"));
        let b = RDBStr::Str(Bytes::from("b"));
        let a2 = RDBStr::Str(Bytes::from("a"));
        assert!(a < b);
        assert_eq!(a.cmp(&a2), std::cmp::Ordering::Equal);
    }

    #[test]
    fn int_vs_int_lexicographic() {
        // "2" > "10" in lexicographic byte order
        let two = RDBStr::Int(2);
        let ten = RDBStr::Int(10);
        assert!(two > ten);
        assert_eq!(ten.cmp(&ten), std::cmp::Ordering::Equal);
    }

    #[test]
    fn str_vs_int_mixed() {
        // "2" (Str) vs 10 (Int -> "10")
        let s = RDBStr::Str(Bytes::from("2"));
        let i = RDBStr::Int(10);
        assert!(s > i);
    }

    #[test]
    fn empty_string_and_zero() {
        let empty = RDBStr::Str(Bytes::new());
        let zero_int = RDBStr::Int(0);
        // "" < "0"
        assert!(empty < zero_int);
    }

    #[test]
    fn binary_bytes_ordering() {
        let a = RDBStr::Str(Bytes::from(&b"\x00\x01\x02"[..]));
        let b = RDBStr::Str(Bytes::from(&b"\x00\x01\x03"[..]));
        assert!(a < b);
    }

    #[test]
    fn sorting_preserves_non_decreasing_order() {
        let mut v = vec![
            RDBStr::Str(Bytes::from("")),
            RDBStr::Int(0),
            RDBStr::Str(Bytes::from("0")),
            RDBStr::Int(1),
            RDBStr::Str(Bytes::from("1")),
            RDBStr::Str(Bytes::from("2")),
        ];
        v.sort();
        for i in 1..v.len() {
            assert!(v[i - 1] <= v[i], "order violated at index {}", i - 1);
        }
    }
}
