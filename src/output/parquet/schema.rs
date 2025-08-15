use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

/// Create the Arrow schema for Redis records in Parquet format
pub fn create_redis_record_schema() -> Schema {
    Schema::new(vec![
        Field::new("cluster", DataType::Utf8, false),
        Field::new(
            "batch",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("instance", DataType::Utf8, false),
        Field::new("db", DataType::Int64, false),
        Field::new("key", DataType::Binary, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("member_count", DataType::Int64, false),
        Field::new("rdb_size", DataType::Int64, false),
        Field::new("encoding", DataType::Utf8, false),
        Field::new(
            "expire_at",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true, // nullable
        ),
        Field::new("idle_seconds", DataType::Int64, true), // nullable
        Field::new("freq", DataType::Int32, true),         // nullable
        Field::new("codis_slot", DataType::Int32, true),   // nullable
        Field::new("redis_slot", DataType::Int32, true),   // nullable
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_field_names() {
        let schema = create_redis_record_schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

        let expected_names = vec![
            "cluster",
            "batch",
            "instance",
            "db",
            "key",
            "type",
            "member_count",
            "rdb_size",
            "encoding",
            "expire_at",
            "idle_seconds",
            "freq",
            "codis_slot",
            "redis_slot",
        ];

        assert_eq!(field_names, expected_names);
    }

    #[test]
    fn test_schema_field_types() {
        let schema = create_redis_record_schema();
        let fields = schema.fields();

        // Test required string fields
        assert_eq!(fields[0].data_type(), &DataType::Utf8); // cluster
        assert!(!fields[0].is_nullable());

        // Test batch timestamp (nanosecond, UTC)
        assert_eq!(
            fields[1].data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );
        assert!(!fields[1].is_nullable());

        // Test instance field
        assert_eq!(fields[2].data_type(), &DataType::Utf8); // instance
        assert!(!fields[2].is_nullable());

        // Test integer fields
        assert_eq!(fields[3].data_type(), &DataType::Int64); // db
        assert!(!fields[3].is_nullable());

        // Test binary key field
        assert_eq!(fields[4].data_type(), &DataType::Binary); // key
        assert!(!fields[4].is_nullable());

        // Test type field
        assert_eq!(fields[5].data_type(), &DataType::Utf8); // type
        assert!(!fields[5].is_nullable());

        // Test required int64 fields
        assert_eq!(fields[6].data_type(), &DataType::Int64); // member_count
        assert!(!fields[6].is_nullable());

        assert_eq!(fields[7].data_type(), &DataType::Int64); // rdb_size
        assert!(!fields[7].is_nullable());

        // Test encoding field
        assert_eq!(fields[8].data_type(), &DataType::Utf8); // encoding
        assert!(!fields[8].is_nullable());

        // Test expire_at timestamp (millisecond, UTC, nullable)
        assert_eq!(
            fields[9].data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        );
        assert!(fields[9].is_nullable());

        // Test nullable int64 field
        assert_eq!(fields[10].data_type(), &DataType::Int64); // idle_seconds
        assert!(fields[10].is_nullable());

        // Test nullable int32 field
        assert_eq!(fields[11].data_type(), &DataType::Int32); // freq
        assert!(fields[11].is_nullable());

        // Test slot fields (nullable int32)
        assert_eq!(fields[12].data_type(), &DataType::Int32); // codis_slot
        assert!(fields[12].is_nullable());

        assert_eq!(fields[13].data_type(), &DataType::Int32); // redis_slot
        assert!(fields[13].is_nullable());
    }

    #[test]
    fn test_schema_field_count() {
        let schema = create_redis_record_schema();
        assert_eq!(schema.fields().len(), 14);
    }
}
