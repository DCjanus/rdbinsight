use std::sync::Arc;

#[cfg(test)]
use arrow::array::{Int32Array, Int64Array};
use arrow::{
    array::{
        BinaryArray, StringArray, TimestampMillisecondArray, TimestampNanosecondArray, UInt8Array,
        UInt16Array, UInt64Array,
    },
    record_batch::RecordBatch,
};

use super::schema::create_redis_record_schema;
use crate::parser::core::raw::RDBStr;

/// Convert Records with explicit context to an Arrow RecordBatch
pub fn records_to_columns(
    cluster: &str,
    batch_ts: time::OffsetDateTime,
    instance: &str,
    records: &[crate::record::Record],
) -> arrow::error::Result<RecordBatch> {
    let schema = Arc::new(create_redis_record_schema());
    let num_records = records.len();

    let mut cluster_data = Vec::with_capacity(num_records);
    let mut batch_data = Vec::with_capacity(num_records);
    let mut instance_data = Vec::with_capacity(num_records);
    let mut db_data = Vec::with_capacity(num_records);
    let mut key_data: Vec<Vec<u8>> = Vec::with_capacity(num_records);
    let mut type_data = Vec::with_capacity(num_records);
    let mut member_count_data = Vec::with_capacity(num_records);
    let mut rdb_size_data = Vec::with_capacity(num_records);
    let mut encoding_data = Vec::with_capacity(num_records);
    let mut expire_at_data = Vec::with_capacity(num_records);
    let mut idle_seconds_data = Vec::with_capacity(num_records);
    let mut freq_data = Vec::with_capacity(num_records);
    let mut codis_slot_data = Vec::with_capacity(num_records);
    let mut redis_slot_data = Vec::with_capacity(num_records);

    let batch_nanos = batch_ts.unix_timestamp_nanos() as i64;

    for record in records {
        cluster_data.push(cluster.to_string());
        batch_data.push(batch_nanos);
        instance_data.push(instance.to_string());
        db_data.push(record.db);
        let key_bytes = match &record.key {
            RDBStr::Str(bytes) => bytes.to_vec(),
            RDBStr::Int(int_val) => int_val.to_string().into_bytes(),
        };
        key_data.push(key_bytes);
        type_data.push(record.type_name().to_string());
        member_count_data.push(record.member_count.unwrap_or(0));
        rdb_size_data.push(record.rdb_size);
        encoding_data.push(record.encoding_name());
        expire_at_data.push(record.expire_at_ms.map(|ms| ms as i64));
        idle_seconds_data.push(record.idle_seconds);
        freq_data.push(record.freq);
        codis_slot_data.push(record.codis_slot);
        redis_slot_data.push(record.redis_slot);
    }

    let cluster_array = Arc::new(StringArray::from(cluster_data));
    let batch_array = Arc::new(TimestampNanosecondArray::from(batch_data).with_timezone("UTC"));
    let instance_array = Arc::new(StringArray::from(instance_data));
    let db_array = Arc::new(UInt64Array::from(db_data));

    let key_refs: Vec<&[u8]> = key_data.iter().map(|v| v.as_slice()).collect();
    let key_array = Arc::new(BinaryArray::from(key_refs));

    let type_array = Arc::new(StringArray::from(type_data));
    let member_count_array = Arc::new(UInt64Array::from(member_count_data));
    let rdb_size_array = Arc::new(UInt64Array::from(rdb_size_data));
    let encoding_array = Arc::new(StringArray::from(encoding_data));
    let expire_at_array =
        Arc::new(TimestampMillisecondArray::from(expire_at_data).with_timezone("UTC"));
    let idle_seconds_array = Arc::new(UInt64Array::from(idle_seconds_data));
    let freq_array = Arc::new(UInt8Array::from(freq_data));
    let codis_slot_array = Arc::new(UInt16Array::from(codis_slot_data));
    let redis_slot_array = Arc::new(UInt16Array::from(redis_slot_data));

    RecordBatch::try_new(schema, vec![
        cluster_array,
        batch_array,
        instance_array,
        db_array,
        key_array,
        type_array,
        member_count_array,
        rdb_size_array,
        encoding_array,
        expire_at_array,
        idle_seconds_array,
        freq_array,
        codis_slot_array,
        redis_slot_array,
    ])
}

#[cfg(test)]
mod tests {
    use arrow::array::Array;
    use bytes::Bytes;
    use time::OffsetDateTime;

    use super::*;
    use crate::{
        parser::model::StringEncoding,
        record::{Record, RecordEncoding, RecordType},
    };

    fn test_cluster() -> &'static str {
        "test-cluster"
    }

    fn test_batch_ts() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp_nanos(1691999696123456789).unwrap()
    }

    fn create_test_record() -> Record {
        Record::builder()
            .db(0)
            .key(RDBStr::Str(Bytes::from("test_key")))
            .r#type(RecordType::String)
            .encoding(RecordEncoding::String(StringEncoding::Raw))
            .rdb_size(100)
            .member_count(Some(1))
            .expire_at_ms(Some(1691999999000))
            .idle_seconds(Some(300))
            .freq(Some(5))
            .codis_slot(Some(256))
            .redis_slot(Some(8192))
            .build()
    }

    #[test]
    fn test_records_to_record_batch_basic() {
        let record = create_test_record();
        let records = vec![record];
        let result =
            records_to_columns(test_cluster(), test_batch_ts(), "127.0.0.1:6379", &records);
        assert!(result.is_ok());

        let record_batch = result.unwrap();
        assert_eq!(record_batch.num_rows(), 1);
        assert_eq!(record_batch.num_columns(), 14);
    }

    #[test]
    fn test_key_as_binary() {
        let record = create_test_record();
        let records = vec![record];
        let record_batch =
            records_to_columns(test_cluster(), test_batch_ts(), "127.0.0.1:6379", &records)
                .unwrap();

        // Get key column (index 4)
        let key_array = record_batch
            .column(4)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();

        assert_eq!(key_array.len(), 1);
        assert_eq!(key_array.value(0), b"test_key");
    }

    #[test]
    fn test_batch_timestamp_nanoseconds() {
        let record = create_test_record();
        let records = vec![record];
        let record_batch =
            records_to_columns(test_cluster(), test_batch_ts(), "127.0.0.1:6379", &records)
                .unwrap();

        // Get batch column (index 1)
        let batch_array = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();

        assert_eq!(batch_array.len(), 1);
        assert_eq!(batch_array.value(0), 1691999696123456789);
    }

    #[test]
    fn test_expire_at_milliseconds() {
        let record = create_test_record();
        let records = vec![record];
        let record_batch =
            records_to_columns(test_cluster(), test_batch_ts(), "127.0.0.1:6379", &records)
                .unwrap();

        // Get expire_at column (index 9)
        let expire_at_array = record_batch
            .column(9)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();

        assert_eq!(expire_at_array.len(), 1);
        assert_eq!(expire_at_array.value(0), 1691999999000);
    }

    #[test]
    fn test_nullable_fields_with_none_values() {
        let record = Record::builder()
            .db(0)
            .key(RDBStr::Str(Bytes::from("test_key")))
            .r#type(RecordType::String)
            .encoding(RecordEncoding::String(StringEncoding::Raw))
            .rdb_size(100)
            .member_count(Some(1))
            // All nullable fields set to None
            .expire_at_ms(None)
            .idle_seconds(None)
            .freq(None)
            .codis_slot(None)
            .redis_slot(None)
            .build();
        let records = vec![record];

        let record_batch =
            records_to_columns(test_cluster(), test_batch_ts(), "127.0.0.1:6379", &records)
                .unwrap();

        // Test expire_at is null
        let expire_at_array = record_batch
            .column(9)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert!(expire_at_array.is_null(0));

        // Test idle_seconds is null
        let idle_seconds_array = record_batch
            .column(10)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(idle_seconds_array.is_null(0));

        // Test freq is null
        let freq_array = record_batch
            .column(11)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(freq_array.is_null(0));

        // Test codis_slot is null
        let codis_slot_array = record_batch
            .column(12)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(codis_slot_array.is_null(0));

        // Test redis_slot is null
        let redis_slot_array = record_batch
            .column(13)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(redis_slot_array.is_null(0));
    }

    #[test]
    fn test_integer_key_conversion() {
        let record = Record::builder()
            .db(0)
            .key(RDBStr::Int(42))
            .r#type(RecordType::String)
            .encoding(RecordEncoding::String(StringEncoding::Raw))
            .rdb_size(100)
            .member_count(Some(1))
            .build();
        let records = vec![record];

        let record_batch =
            records_to_columns(test_cluster(), test_batch_ts(), "127.0.0.1:6379", &records)
                .unwrap();

        // Get key column (index 4)
        let key_array = record_batch
            .column(4)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();

        assert_eq!(key_array.len(), 1);
        assert_eq!(key_array.value(0), b"42");
    }

    #[test]
    fn test_multiple_records() {
        let record1 = create_test_record();
        let mut record2 = create_test_record();
        record2.db = 1;
        record2.key = RDBStr::Str(Bytes::from("another_key"));

        let records = vec![record1, record2];

        let record_batch =
            records_to_columns(test_cluster(), test_batch_ts(), "127.0.0.1:6379", &records)
                .unwrap();
        assert_eq!(record_batch.num_rows(), 2);

        // Check db values
        let db_array = record_batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(db_array.value(0), 0);
        assert_eq!(db_array.value(1), 1);

        // Check key values
        let key_array = record_batch
            .column(4)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(key_array.value(0), b"test_key");
        assert_eq!(key_array.value(1), b"another_key");
    }
}
