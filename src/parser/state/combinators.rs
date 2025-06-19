use anyhow::{Context, bail, ensure};

use crate::{
    helper::AnyResult,
    parser::{
        core::{
            buffer::Buffer,
            combinators::read_exact,
            raw::{RDBLen, read_rdb_len},
        },
        error::NeedMoreData,
        state::traits::{InitializableParser, StateParser},
    },
};

/// A stateful parser that "unboxes" a Redis string to parse its inner content.
///
/// In RDB files, it's common for complex data structures (like a listpack) to be
/// stored inside a standard RDB string value. This `RDBStrBox` acts as a container
/// or "box" parser. Its job is to handle the outer string encoding (including length
/// prefixes and potential LZF compression), and then delegate the parsing of the
/// "unboxed" content to a specialized inner parser.
///
/// ## Encoding and Unboxing
///
/// `RDBStrBox` is responsible for:
/// 1.  Reading the Redis string length-encoding prefix (`RDBLen`).
/// 2.  Determining if the string is stored plainly or is LZF-compressed.
///
/// It abstracts away the complexity of decompression and byte counting, presenting
/// a clean stream of bytes to the inner parser.
///
/// ## Delegation Model
///
/// This parser is generic over `P`, where `P` is the `InitializableParser`
/// responsible for parsing the content inside the string. `RDBStrBox` "entrusts"
/// the parsing job to an instance of `P`.
///
/// - For a **plain string**, it invokes `P` on the unread portion of the main buffer
///   and verifies that `P` consumes exactly the expected number of bytes.
/// - For an **LZF-compressed string**, it first decompresses the data into a
///   temporary buffer. Then, it invokes `P` to parse the entirety of this
///   new buffer.
///
/// ## Usage
///
/// Use this parser when the RDB format specifies a value as a "string-encoded"
/// field which contains another structure.
///
/// ```ignore
/// // Pseudocode showing how to parse a "boxed" listpack.
/// let (input, listpack_parser) = RDBStrBox::<ListPackParser>::init(buffer, input)?;
/// let listpack = listpack_parser.call(buffer)?;
/// ```
pub enum RDBStrBox<P> {
    /// State for parsing a plain, length-prefixed string.
    Simple {
        /// The absolute offset in the buffer where the string content is expected to end.
        expect_end: u64,
        /// The inner parser responsible for parsing the string's content.
        entrust: P,
    },
    /// State for parsing an LZF-compressed string.
    Lzf {
        /// The length of the compressed data in bytes.
        in_len: u64,
        /// The length of the original, uncompressed data in bytes.
        out_len: u64,
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

    fn call_lzf(buffer: &mut Buffer, in_len: u64, out_len: u64) -> AnyResult<P::Output> {
        let input = buffer.as_ref();
        let (input, compressed) =
            read_exact(input, in_len as usize).context("read compressed data")?;
        let decompressed = lzf::decompress(compressed, out_len as usize)
            .map_err(|e| anyhow::anyhow!("decompress quicklist node: {e}"))?;

        let mut decompressed_buffer = Buffer::new(decompressed.len());
        decompressed_buffer.extend(&decompressed)?;
        decompressed_buffer.set_finished();

        let output = full_parser::<P>(&mut decompressed_buffer)?;
        buffer.consume_to(input.as_ptr());
        Ok(output)
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
                Ok((input, Self::Lzf { in_len, out_len }))
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
            Self::Lzf { in_len, out_len } => RDBStrBox::<P>::call_lzf(buffer, *in_len, *out_len),
        }
    }
}

pub struct ReduceParser<P, R, F = fn(R, <P as StateParser>::Output) -> R>
where
    P: InitializableParser,
    F: FnMut(R, <P as StateParser>::Output) -> R,
{
    remain: u64,
    entrust: Option<P>,
    reduce: F,
    accum: Option<R>,
}

impl<P, R, F> ReduceParser<P, R, F>
where
    P: InitializableParser,
    F: FnMut(R, <P as StateParser>::Output) -> R,
{
    pub fn new(remain: u64, init: R, reduce: F) -> Self {
        Self {
            remain,
            entrust: None,
            reduce,
            accum: Some(init),
        }
    }
}

impl<P, R, F> StateParser for ReduceParser<P, R, F>
where
    P: InitializableParser,
    F: FnMut(R, <P as StateParser>::Output) -> R,
{
    type Output = R;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
        loop {
            if let Some(entrust) = self.entrust.as_mut() {
                let output = entrust.call(buffer)?;
                self.entrust = None;
                self.remain -= 1;

                // Apply reduce function.
                let acc = self
                    .accum
                    .take()
                    .expect("Accumulator must be available before completion");
                let new_acc = (self.reduce)(acc, output);
                self.accum = Some(new_acc);
            }

            if self.remain == 0 {
                // Parsing finished – return the accumulated result.
                return Ok(self
                    .accum
                    .take()
                    .expect("Accumulator should contain final value"));
            }

            let (input, entrust) = P::init(buffer, buffer.as_ref())?;
            buffer.consume_to(input.as_ptr());
            self.entrust = Some(entrust);
        }
    }
}

pub fn full_parser<P>(buffer: &mut Buffer) -> AnyResult<P::Output>
where P: InitializableParser + StateParser + Sized {
    let input = buffer.as_ref();
    let (input, mut entrust) = match P::init(buffer, input) {
        Ok((input, entrust)) => (input, entrust),
        Err(e) if e.is::<NeedMoreData>() => {
            bail!("full_parser meet NotFinished error: {e}")
        }
        Err(e) => return Err(e),
    };
    buffer.consume_to(input.as_ptr());

    match entrust.call(buffer) {
        Ok(output) => {
            ensure!(buffer.is_empty(), "buffer should be empty after parsing");
            Ok(output)
        }
        Err(e) if e.is::<NeedMoreData>() => {
            bail!("full_parser meet NotFinished error: {e}")
        }
        Err(e) => Err(e),
    }
}
