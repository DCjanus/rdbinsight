use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::Stream;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncRead;
use typed_builder::TypedBuilder;

use crate::{
    helper::{AnyResult, codis_slot, redis_slot},
    parser::{
        NeedMoreData,
        core::{buffer::Buffer, raw::RDBStr},
        model::{
            HashEncoding, Item, ListEncoding, SetEncoding, StreamEncoding, StringEncoding,
            ZSetEncoding,
        },
        rdb_file::RDBFileParser,
    },
    source::SourceType,
};

/// Redis data type variants
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RecordType {
    String,
    List,
    Set,
    ZSet,
    Hash,
    Stream,
    Module,
}

impl RecordType {
    pub fn type_name(&self) -> &'static str {
        match self {
            RecordType::String => "string",
            RecordType::List => "list",
            RecordType::Set => "set",
            RecordType::ZSet => "zset",
            RecordType::Hash => "hash",
            RecordType::Stream => "stream",
            RecordType::Module => "module",
        }
    }
}

/// Encoding information for different Redis data types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordEncoding {
    String(StringEncoding),
    List(ListEncoding),
    Set(SetEncoding),
    ZSet(ZSetEncoding),
    Hash(HashEncoding),
    Stream(StreamEncoding),
    Module, // Module doesn't have specific encoding variants
}

/// A complete Redis key record with all necessary metadata
#[derive(Debug, Clone, Serialize, Deserialize, TypedBuilder)]
pub struct Record {
    /// Database number (0-15 in standard Redis)
    pub db: u64,
    /// Key name
    pub key: RDBStr,
    /// Redis data type
    pub r#type: RecordType,
    /// Encoding used for this key
    pub encoding: RecordEncoding,
    /// Expiration time in milliseconds since Unix epoch
    #[builder(default)]
    pub expire_at_ms: Option<u64>,
    /// Idle time in seconds (LRU info)
    #[builder(default)]
    pub idle_seconds: Option<u64>,
    /// Frequency counter (LFU info)
    #[builder(default)]
    pub freq: Option<u8>,
    /// Number of elements/members in the data structure
    /// - For String: always 1
    /// - For List/Set/ZSet: number of elements
    /// - For Hash: number of field-value pairs
    /// - For Stream: number of messages
    /// - For Module: None (unknown)
    #[builder(default)]
    pub member_count: Option<u64>,
    /// Size of the record in bytes as stored in RDB
    pub rdb_size: u64,
    /// Codis Slot ID (0-1023), only for Codis clusters
    #[builder(default)]
    pub codis_slot: Option<u16>,
    /// Redis Cluster Slot ID (0-16383), only for Redis Cluster
    #[builder(default)]
    pub redis_slot: Option<u16>,
}

impl Record {
    /// Check if the key has an expiration time set
    pub fn is_expired(&self, current_time_ms: u64) -> bool {
        self.expire_at_ms
            .map(|expire_time| current_time_ms > expire_time)
            .unwrap_or(false)
    }

    /// Get the type as a string for serialization
    pub fn type_name(&self) -> &'static str {
        self.r#type.type_name()
    }

    /// Get the encoding as a string for serialization
    pub fn encoding_name(&self) -> String {
        match self.encoding {
            RecordEncoding::String(enc) => enc.to_string(),
            RecordEncoding::List(enc) => enc.to_string(),
            RecordEncoding::Set(enc) => enc.to_string(),
            RecordEncoding::ZSet(enc) => enc.to_string(),
            RecordEncoding::Hash(enc) => enc.to_string(),
            RecordEncoding::Stream(enc) => enc.to_string(),
            RecordEncoding::Module => "module".to_string(),
        }
    }
}

/// A stream that reads RDB data and yields Records
pub struct RecordStream {
    parser: RDBFileParser,
    buffer: Buffer,
    reader: Pin<Box<dyn AsyncRead + Send>>,
    source_type: SourceType,
    current_db: u64,
    pending_expiry: Option<u64>,
    pending_idle: Option<u64>,
    pending_freq: Option<u8>,
    finished: bool,
}

impl RecordStream {
    /// Create a new RecordStream with an async reader and source type
    pub fn new(reader: Pin<Box<dyn AsyncRead + Send>>, source_type: SourceType) -> Self {
        Self {
            parser: RDBFileParser::default(),
            buffer: Buffer::new(16 * 1024 * 1024), // 16MB buffer
            reader,
            source_type,
            current_db: 0,
            pending_expiry: None,
            pending_idle: None,
            pending_freq: None,
            finished: false,
        }
    }

    /// Get the next record from the stream
    async fn next_record(&mut self) -> AnyResult<Option<Record>> {
        loop {
            match self.try_parse_record()? {
                Some(record) => return Ok(Some(record)),
                None => {
                    // Need more data or end of stream
                    if self.finished {
                        return Ok(None);
                    }

                    // Try to read more data
                    if !self.read_more_data().await? {
                        // EOF reached
                        self.finished = true;
                        self.buffer.set_finished();

                        // Try one more time to parse any remaining data
                        return self.try_parse_record();
                    }
                }
            }
        }
    }

    /// Try to parse a record from the current buffer
    fn try_parse_record(&mut self) -> AnyResult<Option<Record>> {
        loop {
            match self.parser.poll_next(&mut self.buffer) {
                Ok(Some(item)) => {
                    match self.process_item(item) {
                        Some(record) => return Ok(Some(record)),
                        None => continue, // Skip non-record items and continue processing
                    }
                }
                Ok(None) => return Ok(None), // End of stream
                Err(e) if e.is::<NeedMoreData>() => {
                    return Ok(None); // Need more data
                }
                Err(e) => {
                    return Err(e); // Real error
                }
            }
        }
    }

    /// Read more data from the async reader into the buffer
    async fn read_more_data(&mut self) -> AnyResult<bool> {
        use tokio::io::AsyncReadExt;

        let mut read_buf = vec![0u8; 64 * 1024]; // 64KB read buffer
        match self.reader.read(&mut read_buf).await {
            Ok(0) => Ok(false), // EOF
            Ok(n) => {
                self.buffer.extend(&read_buf[..n])?;
                Ok(true)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Process an Item and potentially return a Record
    fn process_item(&mut self, item: Item) -> Option<Record> {
        match item {
            Item::SelectDB { db } => {
                self.current_db = db;
                None
            }
            Item::ExpiryMs { expire_at_ms } => {
                self.pending_expiry = Some(expire_at_ms);
                None
            }
            Item::Idle { idle_seconds } => {
                self.pending_idle = Some(idle_seconds);
                None
            }
            Item::Freq { freq } => {
                self.pending_freq = Some(freq);
                None
            }
            Item::StringRecord {
                key,
                encoding,
                rdb_size,
            } => {
                let (codis_slot, redis_slot) = self.calculate_slot(&key);
                Some(
                    Record::builder()
                        .db(self.current_db)
                        .key(key)
                        .r#type(RecordType::String)
                        .encoding(RecordEncoding::String(encoding))
                        .rdb_size(rdb_size)
                        .member_count(Some(1)) // Strings always have 1 element
                        .expire_at_ms(self.pending_expiry.take())
                        .idle_seconds(self.pending_idle.take())
                        .freq(self.pending_freq.take())
                        .codis_slot(codis_slot)
                        .redis_slot(redis_slot)
                        .build(),
                )
            }
            Item::ListRecord {
                key,
                encoding,
                member_count,
                rdb_size,
            } => {
                let (codis_slot, redis_slot) = self.calculate_slot(&key);
                Some(
                    Record::builder()
                        .db(self.current_db)
                        .key(key)
                        .r#type(RecordType::List)
                        .encoding(RecordEncoding::List(encoding))
                        .rdb_size(rdb_size)
                        .member_count(Some(member_count))
                        .expire_at_ms(self.pending_expiry.take())
                        .idle_seconds(self.pending_idle.take())
                        .freq(self.pending_freq.take())
                        .codis_slot(codis_slot)
                        .redis_slot(redis_slot)
                        .build(),
                )
            }
            Item::SetRecord {
                key,
                encoding,
                member_count,
                rdb_size,
            } => {
                let (codis_slot, redis_slot) = self.calculate_slot(&key);
                Some(
                    Record::builder()
                        .db(self.current_db)
                        .key(key)
                        .r#type(RecordType::Set)
                        .encoding(RecordEncoding::Set(encoding))
                        .rdb_size(rdb_size)
                        .member_count(Some(member_count))
                        .expire_at_ms(self.pending_expiry.take())
                        .idle_seconds(self.pending_idle.take())
                        .freq(self.pending_freq.take())
                        .codis_slot(codis_slot)
                        .redis_slot(redis_slot)
                        .build(),
                )
            }
            Item::ZSetRecord {
                key,
                encoding,
                member_count,
                rdb_size,
            }
            | Item::ZSet2Record {
                key,
                encoding,
                member_count,
                rdb_size,
            } => {
                let (codis_slot, redis_slot) = self.calculate_slot(&key);
                Some(
                    Record::builder()
                        .db(self.current_db)
                        .key(key)
                        .r#type(RecordType::ZSet)
                        .encoding(RecordEncoding::ZSet(encoding))
                        .rdb_size(rdb_size)
                        .member_count(Some(member_count))
                        .expire_at_ms(self.pending_expiry.take())
                        .idle_seconds(self.pending_idle.take())
                        .freq(self.pending_freq.take())
                        .codis_slot(codis_slot)
                        .redis_slot(redis_slot)
                        .build(),
                )
            }
            Item::HashRecord {
                key,
                encoding,
                pair_count,
                rdb_size,
            } => {
                let (codis_slot, redis_slot) = self.calculate_slot(&key);
                Some(
                    Record::builder()
                        .db(self.current_db)
                        .key(key)
                        .r#type(RecordType::Hash)
                        .encoding(RecordEncoding::Hash(encoding))
                        .rdb_size(rdb_size)
                        .member_count(Some(pair_count))
                        .expire_at_ms(self.pending_expiry.take())
                        .idle_seconds(self.pending_idle.take())
                        .freq(self.pending_freq.take())
                        .codis_slot(codis_slot)
                        .redis_slot(redis_slot)
                        .build(),
                )
            }
            Item::StreamRecord {
                key,
                encoding,
                message_count,
                rdb_size,
            } => {
                let (codis_slot, redis_slot) = self.calculate_slot(&key);
                Some(
                    Record::builder()
                        .db(self.current_db)
                        .key(key)
                        .r#type(RecordType::Stream)
                        .encoding(RecordEncoding::Stream(encoding))
                        .rdb_size(rdb_size)
                        .member_count(Some(message_count))
                        .expire_at_ms(self.pending_expiry.take())
                        .idle_seconds(self.pending_idle.take())
                        .freq(self.pending_freq.take())
                        .codis_slot(codis_slot)
                        .redis_slot(redis_slot)
                        .build(),
                )
            }
            Item::ModuleRecord { key, rdb_size } => {
                let (codis_slot, redis_slot) = self.calculate_slot(&key);
                Some(
                    Record::builder()
                        .db(self.current_db)
                        .key(key)
                        .r#type(RecordType::Module)
                        .encoding(RecordEncoding::Module)
                        .rdb_size(rdb_size)
                        .expire_at_ms(self.pending_expiry.take())
                        .idle_seconds(self.pending_idle.take())
                        .freq(self.pending_freq.take())
                        .codis_slot(codis_slot)
                        .redis_slot(redis_slot)
                        .build(),
                )
            }
            // Skip auxiliary items, resize info, function records, etc.
            // These are not actual Redis keys
            Item::Aux { .. }
            | Item::ModuleAux { .. }
            | Item::ResizeDB { .. }
            | Item::SlotInfo { .. }
            | Item::FunctionRecord { .. } => None,
        }
    }

    /// Calculate slot IDs based on source type
    fn calculate_slot(&self, key: &RDBStr) -> (Option<u16>, Option<u16>) {
        match self.source_type {
            SourceType::Codis => {
                let slot = codis_slot(key.to_bytes().as_ref());
                (Some(slot), None)
            }
            SourceType::Cluster => {
                let slot = match key {
                    RDBStr::Str(bytes) => redis_slot(bytes.as_ref()),
                    RDBStr::Int(int_val) => {
                        // Convert integer key to string representation for slot calculation
                        let key_str = int_val.to_string();
                        redis_slot(key_str.as_bytes())
                    }
                };
                (None, Some(slot))
            }
            SourceType::Standalone | SourceType::File => (None, None),
        }
    }
}

impl Stream for RecordStream {
    type Item = AnyResult<Record>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Convert async method to poll-based
        let future = self.next_record();
        tokio::pin!(future);

        match future.poll(cx) {
            Poll::Ready(Ok(Some(record))) => Poll::Ready(Some(Ok(record))),
            Poll::Ready(Ok(None)) => Poll::Ready(None),
            Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[tokio::test]
    async fn test_record_stream_basic() {
        // Create an empty cursor as a mock reader
        let cursor = Cursor::new(Vec::<u8>::new());
        let reader = Box::pin(cursor);
        let mut stream = RecordStream::new(reader, SourceType::File);

        // Since we have no data, this should return None
        let result = stream.next_record().await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_record_creation() {
        use bytes::Bytes;

        let record = Record::builder()
            .db(0)
            .key(RDBStr::Str(Bytes::from("test_key")))
            .r#type(RecordType::String)
            .encoding(RecordEncoding::String(StringEncoding::Raw))
            .rdb_size(100)
            .expire_at_ms(Some(1234567890))
            .member_count(Some(1))
            .build();

        assert_eq!(record.db, 0);
        match record.key {
            RDBStr::Str(bytes) => assert_eq!(bytes.as_ref(), b"test_key"),
            RDBStr::Int(_) => panic!("Expected string key, got integer"),
        }
        assert_eq!(record.r#type, RecordType::String);
        assert_eq!(record.expire_at_ms, Some(1234567890));
        assert_eq!(record.member_count, Some(1));
        assert_eq!(record.rdb_size, 100);
    }

    #[test]
    fn test_record_helper_methods() {
        let record = Record::builder()
            .db(0)
            .key(RDBStr::Int(42))
            .r#type(RecordType::Hash)
            .encoding(RecordEncoding::Hash(HashEncoding::Raw))
            .rdb_size(200)
            .build();

        assert_eq!(record.type_name(), "hash");
        assert_eq!(record.encoding_name(), "raw");
        assert!(!record.is_expired(1000)); // No expiry set

        let expired_record = Record::builder()
            .db(record.db)
            .key(record.key.clone())
            .r#type(record.r#type)
            .encoding(record.encoding)
            .rdb_size(record.rdb_size)
            .expire_at_ms(Some(500))
            .build();
        assert!(expired_record.is_expired(1000)); // Should be expired
        assert!(!expired_record.is_expired(400)); // Should not be expired
    }
}
