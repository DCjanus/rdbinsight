use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("Not finished")]
pub struct NeedMoreData;
