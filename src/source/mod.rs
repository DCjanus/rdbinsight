use std::{future::Future, pin::Pin};

use tokio::io::AsyncRead;

use crate::helper::AnyResult;

pub mod file;
pub mod standalone;

pub trait RdbSourceConfig {
    fn get_rdb_stream(&self) -> impl Future<Output = AnyResult<Pin<Box<dyn AsyncRead + Send>>>>;
}
