//! Incremental, state-machine based parser trait.

use super::buffer::Buffer;
use crate::helper::AnyResult;

pub trait StateParser {
    type Output;

    fn call(&mut self, buffer: &mut Buffer) -> AnyResult<Self::Output>;
}
