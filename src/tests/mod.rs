//! Internal test infrastructure lives here and will gradually replace the helpers under `tests/`.
//! The migration will take time, so both stacks will coexist for a while as we move cases over.

#![allow(dead_code)]
pub mod redis;
pub mod cases;