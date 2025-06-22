use crate::{
    helper::AnyResult,
    parser::{
        core::buffer::Buffer,
        state::traits::{InitializableParser, StateParser},
    },
};

/// Internal state machine wrapper used by `seq_parser!` macro.
#[derive(Debug)]
pub enum ParserPhase<P: StateParser> {
    Init,
    Call(P),
    Done(P::Output),
}

impl<P> Default for ParserPhase<P>
where P: StateParser
{
    fn default() -> Self {
        Self::Init
    }
}

impl<P> ParserPhase<P>
where P: StateParser + InitializableParser
{
    fn step(&mut self, buffer: &mut Buffer) -> AnyResult {
        loop {
            match self {
                Self::Init => {
                    let (input, p) = P::init(buffer, buffer.as_ref())?;
                    buffer.consume_to(input.as_ptr());
                    *self = Self::Call(p);
                }
                Self::Call(p) => {
                    let output = p.call(buffer)?;
                    *self = Self::Done(output);
                }
                Self::Done(_) => return Ok(()),
            }
        }
    }

    fn take(&mut self) -> P::Output {
        match std::mem::take(self) {
            Self::Done(output) => output,
            _ => unreachable!("parser should be done"),
        }
    }
}

// === Macro helpers =========================================================
/// Defines the generator macro for sequential parsers (`Seq{N}Parser`).
macro_rules! seq_parser {
    (
        $name:ident,
        $( $Pi:ident : $idx:tt ),+
    ) => {
        pub type $name< $( $Pi ),+ > = ( $( ParserPhase<$Pi>, )+ );

        impl< $( $Pi ),+ > InitializableParser for $name< $( $Pi ),+ >
        where
            $( $Pi : InitializableParser ),+
        {
            fn init<'a>(_: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
                Ok((input, ( $( ParserPhase::<$Pi>::default(), )+ )))
            }
        }

        impl< $( $Pi ),+ > StateParser for $name< $( $Pi ),+ >
        where
            $( $Pi : StateParser + InitializableParser ),+
        {
            type Output = ( $( $Pi::Output, )+ );

            #[allow(non_snake_case, clippy::too_many_arguments)]
            fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
                $( self.$idx.step(buffer)?; )+
                Ok(( $( self.$idx.take(), )+ ))
            }
        }
    };
}

seq_parser!(Seq2Parser, P1:0, P2:1);
seq_parser!(Seq3Parser, P1:0, P2:1, P3:2);
seq_parser!(Seq4Parser, P1:0, P2:1, P3:2, P4:3);
seq_parser!(Seq5Parser, P1:0, P2:1, P3:2, P4:3, P5:4);
seq_parser!(Seq6Parser, P1:0, P2:1, P3:2, P4:3, P5:4, P6:5);
seq_parser!(Seq7Parser, P1:0, P2:1, P3:2, P4:3, P5:4, P6:5, P7:6);
// ============================ Tests ========================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        helper::AnyResult,
        parser::{core::buffer::Buffer, error::NeedMoreData},
    };

    /// A trivial parser that consumes exactly one byte and returns it.
    #[derive(Default)]
    struct ByteParser;

    impl StateParser for ByteParser {
        type Output = u8;

        fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
            let input = buffer.as_ref();
            if input.is_empty() {
                return Err(NeedMoreData.into());
            }
            let byte = input[0];
            unsafe { buffer.consume_to(input.as_ptr().add(1)) };
            Ok(byte)
        }
    }

    impl InitializableParser for ByteParser {
        fn init<'a>(_buf: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
            Ok((input, Self::default()))
        }
    }

    /// A parser that consumes exactly two bytes and returns a little-endian `u16`.
    #[derive(Default)]
    struct U16LeParser;

    impl StateParser for U16LeParser {
        type Output = u16;

        fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output> {
            let input = buffer.as_ref();
            if input.len() < 2 {
                return Err(NeedMoreData.into());
            }
            let val = u16::from_le_bytes([input[0], input[1]]);
            unsafe { buffer.consume_to(input.as_ptr().add(2)) };
            Ok(val)
        }
    }

    impl InitializableParser for U16LeParser {
        fn init<'a>(_buf: &Buffer, input: &'a [u8]) -> AnyResult<(&'a [u8], Self)> {
            Ok((input, Self::default()))
        }
    }

    #[test]
    fn seq2_parser_success() -> AnyResult<()> {
        let mut buffer = Buffer::new(3);
        buffer.extend(&[0x12, 0x34, 0x56])?;

        let (input, mut parser) =
            <Seq2Parser<ByteParser, U16LeParser> as InitializableParser>::init(
                &buffer,
                buffer.as_ref(),
            )?;
        buffer.consume_to(input.as_ptr());

        let (b, n) = parser.call(&mut buffer)?;
        assert_eq!(b, 0x12);
        assert_eq!(n, 0x5634);
        assert!(buffer.is_empty());
        Ok(())
    }

    #[test]
    fn seq2_parser_need_more_data() -> AnyResult<()> {
        let mut buffer = Buffer::new(1);
        buffer.extend(&[0xAA])?;

        let (input, mut parser) =
            <Seq2Parser<ByteParser, U16LeParser> as InitializableParser>::init(
                &buffer,
                buffer.as_ref(),
            )?;
        buffer.consume_to(input.as_ptr());

        let err = parser
            .call(&mut buffer)
            .expect_err("parser should require more data");
        assert!(err.is::<NeedMoreData>());
        Ok(())
    }
}
