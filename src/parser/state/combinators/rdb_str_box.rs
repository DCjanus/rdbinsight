use anyhow::{Context, anyhow, bail, ensure};

use crate::{
    helper::AnyResult,
    parser::{
        core::{
            buffer::Buffer,
            raw::{RDBLen, read_rdb_len},
        },
        error::NeedMoreData,
        state::{
            lzf::LzfChunkDecoder,
            traits::{InitializableParser, StateParser},
        },
    },
};

/// A stateful parser that "unboxes" a Redis string to parse its inner content.
pub enum RDBStrBox<P> {
    Simple {
        expect_end: u64,
        entrust: P,
    },
    Lzf {
        remain_in: u64,
        remain_out: u64,
        out_buffer: Buffer,
        entrust: Option<P>,
        decoder: LzfChunkDecoder,
    },
}

impl<P> RDBStrBox<P>
where P: InitializableParser + StateParser
{
    pub fn is_lzf(&self) -> bool {
        matches!(self, Self::Lzf { .. })
    }

    fn call_simple(buffer: &mut Buffer, expect_end: u64, entrust: &mut P) -> AnyResult<P::Output> {
        let ret = entrust.call(buffer);
        let e = match ret {
            Ok(output) => {
                ensure!(
                    buffer.tell() == expect_end,
                    "RDBStrBox offset mismatch: expect: {}, actual: {}",
                    expect_end,
                    buffer.tell()
                );
                return Ok(output);
            }
            Err(e) => e,
        };

        if buffer.tell() >= expect_end && e.is::<NeedMoreData>() {
            bail!("all RDB string should be consumed, parser not finished: {e}");
        }

        Err(e)
    }

    fn call_lzf(
        in_buffer: &mut Buffer,
        out_buffer: &mut Buffer,
        decoder: &mut LzfChunkDecoder,
        remain_in: &mut u64,
        remain_out: &mut u64,
        entrust: &mut Option<P>,
    ) -> AnyResult<P::Output> {
        loop {
            match Self::call_lzf_inner(out_buffer, entrust) {
                Ok(output) => {
                    ensure!(
                        out_buffer.is_empty(),
                        "lzf decompress fail, output buffer not empty"
                    );
                    ensure!(*remain_in == 0, "lzf decompress fail, remain_in not 0");
                    ensure!(*remain_out == 0, "lzf decompress fail, remain_out not 0");
                    return Ok(output);
                }
                Err(e) if e.is::<NeedMoreData>() => {
                    Self::feed_lzf_inner(in_buffer, out_buffer, decoder, remain_in, remain_out)?;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
    }

    fn feed_lzf_inner(
        in_buffer: &mut Buffer,
        out_buffer: &mut Buffer,
        decoder: &mut LzfChunkDecoder,
        remain_in: &mut u64,
        remain_out: &mut u64,
    ) -> AnyResult {
        let before_in = in_buffer.tell();
        let before_out = out_buffer.len();

        decoder.feed(in_buffer, out_buffer)?;

        let in_size = in_buffer.tell() - before_in;
        let out_size = out_buffer.len() - before_out;

        *remain_in = remain_in.checked_sub(in_size).ok_or_else(|| {
            anyhow!(
                "lzf decompress fail, consumed too much data, this might caused by invalid rdb file"
            )
        })?;
        *remain_out = remain_out.checked_sub(out_size as u64).ok_or_else(|| {
            anyhow!(
                "lzf decompress fail, output buffer overflow, this might caused by invalid rdb file"
            )
        })?;

        Ok(())
    }

    fn call_lzf_inner(
        out_buffer: &mut Buffer,
        entrust: &mut Option<P>,
    ) -> AnyResult<<P as StateParser>::Output> {
        let input = out_buffer.as_slice();

        if entrust.is_none() {
            let (input, parser) = P::init(out_buffer, input)?;
            *entrust = Some(parser);
            out_buffer.consume_to(input.as_ptr());
        }

        let entrust = entrust.as_mut().expect("entrust should be initialized");
        entrust.call(out_buffer)
    }
}

impl<P> InitializableParser for RDBStrBox<P>
where P: InitializableParser
{
    fn init<'a>(buffer: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
        let (input, len) = read_rdb_len(input).context("read string length")?;
        match len {
            RDBLen::Simple(length) | RDBLen::IntStr(length) => {
                let expect_end = buffer.tell_to(input.as_ptr()) + length;
                let (input, entrust) = P::init(buffer, input)?;
                Ok((input, Self::Simple {
                    expect_end,
                    entrust,
                }))
            }
            RDBLen::LZFStr => {
                let (input, in_len) = read_rdb_len(input).context("read compressed in_len")?;
                let in_len = in_len
                    .as_u64()
                    .context("compressed in_len must be simple")?;
                let (input, out_len) = read_rdb_len(input).context("read compressed out_len")?;
                let out_len = out_len
                    .as_u64()
                    .context("compressed out_len must be simple")?;
                Ok((input, Self::Lzf {
                    remain_in: in_len,
                    remain_out: out_len,
                    out_buffer: Buffer::new(out_len as usize),
                    decoder: LzfChunkDecoder::default(),
                    entrust: None,
                }))
            }
        }
    }
}

impl<P> StateParser for RDBStrBox<P>
where P: InitializableParser + StateParser
{
    type Output = <P as StateParser>::Output;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        match self {
            Self::Simple {
                expect_end,
                entrust,
            } => RDBStrBox::call_simple(buffer, *expect_end, entrust),
            Self::Lzf {
                remain_in,
                remain_out,
                out_buffer,
                decoder,
                entrust,
            } => RDBStrBox::<P>::call_lzf(
                buffer, out_buffer, decoder, remain_in, remain_out, entrust,
            ),
        }
    }
}
