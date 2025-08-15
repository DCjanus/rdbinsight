use time::OffsetDateTime;

use crate::record::Record;

#[derive(Debug, Clone)]
pub struct Chunk {
    pub cluster: String,
    pub batch_ts: OffsetDateTime,
    pub instance: String,
    pub records: Vec<Record>,
}
