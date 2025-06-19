use anyhow::ensure;

use crate::{helper::AnyResult, parser::error::NeedMoreData};

pub fn read_tag<'a>(input: &'a [u8], tag: &'static [u8]) -> AnyResult<&'a [u8]> {
    let (input, found) = read_exact(input, tag.len())?;
    ensure!(found == tag, "expected tag mismatched");
    Ok(input)
}

pub fn read_exact(input: &[u8], len: usize) -> AnyResult<(&[u8], &[u8])> {
    if input.len() < len {
        return Err(NeedMoreData.into());
    }
    Ok((&input[len..], &input[..len]))
}

pub fn read_at_most_but_at_least_one(input: &[u8], len: usize) -> AnyResult<(&[u8], &[u8])> {
    not_empty(input)?;
    read_exact(input, len.min(input.len()))
}

pub fn read_u8(input: &[u8]) -> AnyResult<(&[u8], u8)> {
    let (input, found) = read_exact(input, 1)?;
    Ok((input, found[0]))
}

pub fn read_be_u16(input: &[u8]) -> AnyResult<(&[u8], u16)> {
    let (input, found) = read_exact(input, 2)?;
    let value = u16::from_be_bytes([found[0], found[1]]);
    Ok((input, value))
}

pub fn read_be_u32(input: &[u8]) -> AnyResult<(&[u8], u32)> {
    let (input, found) = read_exact(input, 4)?;
    let value = u32::from_be_bytes([found[0], found[1], found[2], found[3]]);
    Ok((input, value))
}

pub fn read_be_u64(input: &[u8]) -> AnyResult<(&[u8], u64)> {
    let (input, found) = read_exact(input, 8)?;
    let value = u64::from_be_bytes([
        found[0], found[1], found[2], found[3], found[4], found[5], found[6], found[7],
    ]);
    Ok((input, value))
}

pub fn read_le_u32(input: &[u8]) -> AnyResult<(&[u8], u32)> {
    let (input, found) = read_exact(input, 4)?;
    let value = u32::from_le_bytes([found[0], found[1], found[2], found[3]]);
    Ok((input, value))
}

pub fn read_le_u64(input: &[u8]) -> AnyResult<(&[u8], u64)> {
    let (input, found) = read_exact(input, 8)?;
    let value = u64::from_le_bytes([
        found[0], found[1], found[2], found[3], found[4], found[5], found[6], found[7],
    ]);
    Ok((input, value))
}

pub fn not_empty(input: &[u8]) -> AnyResult {
    if input.is_empty() {
        return Err(NeedMoreData.into());
    }
    Ok(())
}

pub fn peek_u8(input: &[u8]) -> AnyResult<(&[u8], u8)> {
    not_empty(input)?;
    Ok((input, input[0]))
}
