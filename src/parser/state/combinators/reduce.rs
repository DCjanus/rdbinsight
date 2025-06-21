use crate::{
    helper::AnyResult,
    parser::{
        core::buffer::Buffer,
        state::traits::{InitializableParser, StateParser},
    },
};

/// A generic **reduce**-style combinator: it invokes an inner parser `P` exactly
/// `remain` times, each time folding the produced value into an accumulator via
/// the user-supplied closure `reduce`.
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

                let acc = self
                    .accum
                    .take()
                    .expect("Accumulator must be available before completion");
                let new_acc = (self.reduce)(acc, output);
                self.accum = Some(new_acc);
            }

            if self.remain == 0 {
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
