#![feature(adt_const_params)]
#![feature(trait_alias)]
#![allow(incomplete_features)]

#[cfg(not(target_pointer_width = "64"))]
compile_error!(
    "rdbinsight currently supports only 64-bit targets. \
Many parser paths cast lengths with `as usize`, and 32-bit targets may truncate values."
);

pub mod config;
pub mod helper;
pub mod metric;
pub mod output;
pub mod parser;
pub mod record;
pub mod report;
pub mod source;
