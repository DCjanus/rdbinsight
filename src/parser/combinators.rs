use anyhow::ensure;
use thiserror::Error;

use crate::helper::AnyResult;

pub fn read_tag<'a>(input: &'a [u8], tag: &'static [u8]) -> AnyResult<&'a [u8]> {
    let (input, found) = read_n(input, tag.len())?;
    ensure!(found == tag, "expected tag mismatched");
    Ok(input)
}

pub fn read_n(input: &[u8], len: usize) -> AnyResult<(&[u8], &[u8])> {
    if input.len() < len {
        return Err(NotFinished.into());
    }
    Ok((&input[len..], &input[..len]))
}

pub fn read_u8(input: &[u8]) -> AnyResult<(&[u8], u8)> {
    let (input, found) = read_n(input, 1)?;
    Ok((input, found[0]))
}

pub fn read_be_u16(input: &[u8]) -> AnyResult<(&[u8], u16)> {
    let (input, found) = read_n(input, 2)?;
    let value = u16::from_be_bytes([found[0], found[1]]);
    Ok((input, value))
}

pub fn read_be_u32(input: &[u8]) -> AnyResult<(&[u8], u32)> {
    let (input, found) = read_n(input, 4)?;
    let value = u32::from_be_bytes([found[0], found[1], found[2], found[3]]);
    Ok((input, value))
}

pub fn read_be_u64(input: &[u8]) -> AnyResult<(&[u8], u64)> {
    let (input, found) = read_n(input, 8)?;
    let value = u64::from_be_bytes([
        found[0], found[1], found[2], found[3], found[4], found[5], found[6], found[7],
    ]);
    Ok((input, value))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("Not finished")]
struct NotFinished;
