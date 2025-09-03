use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use parquet::format::SortingColumn;

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
        Field::new("db", DataType::UInt64, false),
        Field::new("key", DataType::Binary, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("member_count", DataType::UInt64, false),
        Field::new("rdb_size", DataType::UInt64, false),
        Field::new("encoding", DataType::Utf8, false),
        Field::new(
            "expire_at",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true, // nullable
        ),
        Field::new("idle_seconds", DataType::UInt64, true), // nullable
        Field::new("freq", DataType::UInt8, true),          // nullable
        Field::new("codis_slot", DataType::UInt16, true),   // nullable
        Field::new("redis_slot", DataType::UInt16, true),   // nullable
    ])
}

/// Create sorting_columns metadata for (db ASC, key ASC) by field names.
pub fn create_db_key_sorting_columns(
    schema: &Schema,
) -> anyhow::Result<Vec<parquet::format::SortingColumn>> {
    use anyhow::anyhow;
    let cluster_idx = find_field_index(schema, "cluster")
        .ok_or_else(|| anyhow!("Field 'cluster' not found in Redis record schema"))?;
    let batch_idx = find_field_index(schema, "batch")
        .ok_or_else(|| anyhow!("Field 'batch' not found in Redis record schema"))?;
    let instance_idx = find_field_index(schema, "instance")
        .ok_or_else(|| anyhow!("Field 'instance' not found in Redis record schema"))?;
    let db_idx = find_field_index(schema, "db")
        .ok_or_else(|| anyhow!("Field 'db' not found in Redis record schema"))?;
    let key_idx = find_field_index(schema, "key")
        .ok_or_else(|| anyhow!("Field 'key' not found in Redis record schema"))?;
    Ok(vec![
        SortingColumn::new(cluster_idx as i32, false, false),
        SortingColumn::new(batch_idx as i32, false, false),
        SortingColumn::new(instance_idx as i32, false, false),
        SortingColumn::new(db_idx as i32, false, false),
        SortingColumn::new(key_idx as i32, false, false),
    ])
}

/// Find the index of a field by name in the given Arrow Schema.
/// Returns None if no such field exists.
pub(crate) fn find_field_index(schema: &Schema, name: &str) -> Option<usize> {
    schema.fields().iter().position(|f| f.name() == name)
}
