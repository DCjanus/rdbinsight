use std::collections::HashSet;

use anyhow::{Result, anyhow};
use futures_util::StreamExt;
use rdbinsight::{
    parser::core::raw::RDBStr,
    record::{Record, RecordEncoding, RecordStream, RecordType},
    source::{RdbSourceConfig, standalone::Config as StandaloneConfig},
};
use redis::AsyncCommands;

mod common;

use common::setup::{RedisConfig, RedisVariant};

/// Test data structure to hold expected record information
#[derive(Debug, Clone)]
struct ExpectedRecord {
    key: String,
    record_type: RecordType,
    member_count: Option<u64>,
}

impl ExpectedRecord {
    fn new(key: &str, record_type: RecordType, member_count: Option<u64>) -> Self {
        Self {
            key: key.to_string(),
            record_type,
            member_count,
        }
    }
}

#[tokio::test]
async fn test_record_stream_integration() -> Result<()> {
    // Create Redis Stack instance to test module records (includes RedisBloom)
    let redis_instance = RedisConfig::default()
        .with_version(RedisVariant::StackLatest)
        .with_diskless(true)
        .build()
        .await?;

    // Get connection to populate test data
    let client = redis::Client::open(redis_instance.connection_string.as_str())?;
    let mut conn = client.get_multiplexed_tokio_connection().await?;

    // Populate test data with different Redis data types
    let expected_records = populate_test_data(&mut conn).await?;

    // Force a save to ensure data is persisted
    let _: () = redis::cmd("SAVE").query_async(&mut conn).await?;

    // Wait a bit for Redis to complete the save
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Get the Redis address for standalone connection
    let host = redis_instance.container.get_host().await?;
    let port = redis_instance.container.get_host_port_ipv4(6379).await?;
    let address = format!("{}:{}", host, port);

    // Create standalone config to read RDB
    let standalone_config = StandaloneConfig {
        address,
        username: None,
        password: None,
    };

    // Get RDB stream
    let stream = standalone_config.get_rdb_stream().await?;

    // Create record stream that manages buffer internally
    let mut record_stream = RecordStream::new(stream);
    let mut records = Vec::new();

    // Read all records from the stream
    loop {
        match record_stream.next().await {
            Some(Ok(record)) => {
                records.push(record);
            }
            Some(Err(e)) => {
                return Err(e);
            }
            None => {
                // End of stream
                break;
            }
        }
    }

    // Verify the results
    verify_records(&records, &expected_records)?;

    Ok(())
}

async fn populate_test_data(
    conn: &mut redis::aio::MultiplexedConnection,
) -> Result<Vec<ExpectedRecord>> {
    let mut expected = Vec::new();

    // String data - without TTL
    let _: () = conn.set("string_key", "hello_world").await?;
    expected.push(ExpectedRecord::new(
        "string_key",
        RecordType::String,
        Some(1),
    ));

    // String data - with TTL
    let _: () = conn
        .set_ex("string_with_ttl", "expiring_value", 3600)
        .await?;
    expected.push(ExpectedRecord::new(
        "string_with_ttl",
        RecordType::String,
        Some(1),
    ));

    // List data - without TTL
    let _: () = conn
        .lpush("list_key", vec!["item1", "item2", "item3"])
        .await?;
    expected.push(ExpectedRecord::new("list_key", RecordType::List, Some(3)));

    // List data - with TTL
    let _: () = conn
        .lpush("list_with_ttl", vec!["exp_item1", "exp_item2", "exp_item3"])
        .await?;
    let _: () = conn.expire("list_with_ttl", 7200).await?; // 2 hours
    expected.push(ExpectedRecord::new(
        "list_with_ttl",
        RecordType::List,
        Some(3),
    ));

    // Set data - without TTL
    let _: () = conn
        .sadd("set_key", vec!["member1", "member2", "member3", "member4"])
        .await?;
    expected.push(ExpectedRecord::new("set_key", RecordType::Set, Some(4)));

    // Set data - with TTL
    let _: () = conn
        .sadd("set_with_ttl", vec![
            "exp_member1",
            "exp_member2",
            "exp_member3",
        ])
        .await?;
    let _: () = conn.expire("set_with_ttl", 5400).await?; // 1.5 hours
    expected.push(ExpectedRecord::new(
        "set_with_ttl",
        RecordType::Set,
        Some(3),
    ));

    // Hash data - without TTL
    let _: () = conn
        .hset_multiple("hash_key", &[
            ("field1", "value1"),
            ("field2", "value2"),
            ("field3", "value3"),
        ])
        .await?;
    expected.push(ExpectedRecord::new("hash_key", RecordType::Hash, Some(3)));

    // Hash data - with TTL
    let _: () = conn
        .hset_multiple("hash_with_ttl", &[
            ("exp_field1", "exp_value1"),
            ("exp_field2", "exp_value2"),
        ])
        .await?;
    let _: () = conn.expire("hash_with_ttl", 10800).await?; // 3 hours
    expected.push(ExpectedRecord::new(
        "hash_with_ttl",
        RecordType::Hash,
        Some(2),
    ));

    // ZSet data - without TTL
    let _: () = conn
        .zadd_multiple("zset_key", &[
            (1.0, "member1"),
            (2.0, "member2"),
            (3.0, "member3"),
            (4.0, "member4"),
            (5.0, "member5"),
        ])
        .await?;
    expected.push(ExpectedRecord::new("zset_key", RecordType::ZSet, Some(5)));

    // ZSet data - with TTL
    let _: () = conn
        .zadd_multiple("zset_with_ttl", &[
            (1.5, "exp_member1"),
            (2.5, "exp_member2"),
            (3.5, "exp_member3"),
        ])
        .await?;
    let _: () = conn.expire("zset_with_ttl", 14400).await?; // 4 hours
    expected.push(ExpectedRecord::new(
        "zset_with_ttl",
        RecordType::ZSet,
        Some(3),
    ));

    // Stream data - without TTL
    let _: () = redis::cmd("XADD")
        .arg("stream_key")
        .arg("*")
        .arg("field1")
        .arg("value1")
        .query_async(conn)
        .await?;
    let _: () = redis::cmd("XADD")
        .arg("stream_key")
        .arg("*")
        .arg("field2")
        .arg("value2")
        .query_async(conn)
        .await?;
    expected.push(ExpectedRecord::new(
        "stream_key",
        RecordType::Stream,
        Some(2),
    ));

    // Stream data - with TTL
    let _: () = redis::cmd("XADD")
        .arg("stream_with_ttl")
        .arg("*")
        .arg("exp_field1")
        .arg("exp_value1")
        .query_async(conn)
        .await?;
    let _: () = redis::cmd("XADD")
        .arg("stream_with_ttl")
        .arg("*")
        .arg("exp_field2")
        .arg("exp_value2")
        .query_async(conn)
        .await?;
    let _: () = redis::cmd("XADD")
        .arg("stream_with_ttl")
        .arg("*")
        .arg("exp_field3")
        .arg("exp_value3")
        .query_async(conn)
        .await?;
    let _: () = conn.expire("stream_with_ttl", 18000).await?; // 5 hours
    expected.push(ExpectedRecord::new(
        "stream_with_ttl",
        RecordType::Stream,
        Some(3),
    ));

    // Large string to test different encodings - without TTL
    let large_string = "x".repeat(1000);
    let _: () = conn.set("large_string", &large_string).await?;
    expected.push(ExpectedRecord::new(
        "large_string",
        RecordType::String,
        Some(1),
    ));

    // Large string with TTL
    let large_string_ttl = "y".repeat(1000);
    let _: () = conn.set("large_string_with_ttl", &large_string_ttl).await?;
    let _: () = conn.expire("large_string_with_ttl", 21600).await?; // 6 hours
    expected.push(ExpectedRecord::new(
        "large_string_with_ttl",
        RecordType::String,
        Some(1),
    ));

    // Integer-encoded string - without TTL
    let _: () = conn.set("integer_string", 12345).await?;
    expected.push(ExpectedRecord::new(
        "integer_string",
        RecordType::String,
        Some(1),
    ));

    // Integer-encoded string - with TTL
    let _: () = conn.set("integer_string_with_ttl", 67890).await?;
    let _: () = conn.expire("integer_string_with_ttl", 25200).await?; // 7 hours
    expected.push(ExpectedRecord::new(
        "integer_string_with_ttl",
        RecordType::String,
        Some(1),
    ));

    // Large list to potentially trigger different encodings - without TTL
    let large_list: Vec<String> = (0..100).map(|i| format!("item_{}", i)).collect();
    let _: () = conn.lpush("large_list", &large_list).await?;
    expected.push(ExpectedRecord::new(
        "large_list",
        RecordType::List,
        Some(100),
    ));

    // Large list with TTL
    let large_list_ttl: Vec<String> = (0..80).map(|i| format!("exp_item_{}", i)).collect();
    let _: () = conn.lpush("large_list_with_ttl", &large_list_ttl).await?;
    let _: () = conn.expire("large_list_with_ttl", 28800).await?; // 8 hours
    expected.push(ExpectedRecord::new(
        "large_list_with_ttl",
        RecordType::List,
        Some(80),
    ));

    // Module data - Bloom Filter (without TTL)
    let _: () = redis::cmd("BF.RESERVE")
        .arg("bloom_filter_key")
        .arg(0.01) // error rate
        .arg(1000) // capacity
        .query_async(conn)
        .await?;
    let _: () = redis::cmd("BF.ADD")
        .arg("bloom_filter_key")
        .arg("item1")
        .query_async(conn)
        .await?;
    expected.push(ExpectedRecord::new(
        "bloom_filter_key",
        RecordType::Module,
        None, // Module records don't have member count
    ));

    // Module data - Bloom Filter (with TTL)
    let _: () = redis::cmd("BF.RESERVE")
        .arg("bloom_filter_with_ttl")
        .arg(0.01) // error rate
        .arg(500) // capacity
        .query_async(conn)
        .await?;
    let _: () = redis::cmd("BF.ADD")
        .arg("bloom_filter_with_ttl")
        .arg("exp_item1")
        .query_async(conn)
        .await?;
    let _: () = conn.expire("bloom_filter_with_ttl", 32400).await?; // 9 hours
    expected.push(ExpectedRecord::new(
        "bloom_filter_with_ttl",
        RecordType::Module,
        None, // Module records don't have member count
    ));

    Ok(expected)
}

fn verify_records(actual_records: &[Record], expected_records: &[ExpectedRecord]) -> Result<()> {
    println!("Total records found: {}", actual_records.len());
    println!("Expected records: {}", expected_records.len());

    // Create a map of expected records by key
    let expected_map: std::collections::HashMap<String, &ExpectedRecord> = expected_records
        .iter()
        .map(|r| (r.key.clone(), r))
        .collect();

    // Track found keys
    let mut found_keys = HashSet::new();

    // Verify each actual record
    for record in actual_records {
        let key_str = match &record.key {
            RDBStr::Str(bytes) => String::from_utf8(bytes.to_vec())
                .map_err(|e| anyhow!("Invalid UTF-8 in record key: {}", e))?,
            RDBStr::Int(n) => n.to_string(),
        };

        if let Some(expected) = expected_map.get(&key_str) {
            found_keys.insert(key_str.clone());

            // Verify record type
            assert_eq!(
                record.r#type, expected.record_type,
                "Type mismatch for key '{}': expected {:?}, got {:?}",
                key_str, expected.record_type, record.r#type
            );

            // Verify member count if specified
            if let Some(expected_count) = expected.member_count {
                assert_eq!(
                    record.member_count,
                    Some(expected_count),
                    "Member count mismatch for key '{}': expected {:?}, got {:?}",
                    key_str,
                    Some(expected_count),
                    record.member_count
                );
            } else if expected.record_type == RecordType::Module {
                // Module records should not have member count
                assert_eq!(
                    record.member_count, None,
                    "Module record should not have member count for key '{}'",
                    key_str
                );
            }

            // Verify DB is 0 (default)
            assert_eq!(
                record.db, 0,
                "DB mismatch for key '{}': expected 0, got {}",
                key_str, record.db
            );

            // Verify RDB size is reasonable (> 0)
            assert!(
                record.rdb_size > 0,
                "RDB size should be > 0 for key '{}', got {}",
                key_str,
                record.rdb_size
            );

            // Verify encoding for Module records
            if expected.record_type == RecordType::Module {
                assert_eq!(
                    record.encoding,
                    RecordEncoding::Module,
                    "Module record should have Module encoding for key '{}'",
                    key_str
                );
            }

            println!(
                "✓ Key '{}': type={:?}, members={:?}, size={}, encoding={}",
                key_str,
                record.r#type,
                record.member_count,
                record.rdb_size,
                record.encoding_name()
            );
        } else {
            println!("? Unexpected key found: '{}'", key_str);
        }
    }

    // Check that all expected keys were found
    for expected in expected_records {
        if !found_keys.contains(&expected.key) {
            panic!("Expected key '{}' not found in records", expected.key);
        }
    }

    // Verify we found the expected number of records
    assert_eq!(
        found_keys.len(),
        expected_records.len(),
        "Found {} expected keys out of {} total records",
        found_keys.len(),
        actual_records.len()
    );

    println!(
        "✓ All {} expected records verified successfully!",
        expected_records.len()
    );

    Ok(())
}

#[tokio::test]
async fn test_record_stream_with_expiry() -> Result<()> {
    // Create Redis instance
    let redis_instance = RedisConfig::default()
        .with_version(RedisVariant::Redis8_0)
        .build()
        .await?;

    let client = redis::Client::open(redis_instance.connection_string.as_str())?;
    let mut conn = client.get_multiplexed_tokio_connection().await?;

    // Create keys with different TTL values
    let _: () = conn.set_ex("key_with_ttl_1", "value1", 3600).await?; // 1 hour
    let _: () = conn.set_ex("key_with_ttl_2", "value2", 86400).await?; // 1 day

    // Get RDB stream
    let host = redis_instance.container.get_host().await?;
    let port = redis_instance.container.get_host_port_ipv4(6379).await?;
    let address = format!("{}:{}", host, port);
    let standalone_config = StandaloneConfig {
        address,
        username: None,
        password: None,
    };

    let stream = standalone_config.get_rdb_stream().await?;
    let mut record_stream = RecordStream::new(stream);
    let mut records_with_expiry = Vec::new();

    // Collect all records
    loop {
        match record_stream.next().await {
            Some(Ok(record)) => {
                if record.expire_at_ms.is_some() {
                    records_with_expiry.push(record);
                }
            }
            Some(Err(e)) => return Err(e),
            None => break,
        }
    }

    // Verify we found records with expiry
    assert_eq!(
        records_with_expiry.len(),
        2,
        "Expected 2 records with expiry, found {}",
        records_with_expiry.len()
    );

    for record in &records_with_expiry {
        assert!(
            record.expire_at_ms.is_some(),
            "Record should have expiry time"
        );

        let key_str = match &record.key {
            RDBStr::Str(bytes) => String::from_utf8(bytes.to_vec())
                .map_err(|e| anyhow!("Invalid UTF-8 in record key: {}", e))?,
            RDBStr::Int(n) => n.to_string(),
        };

        assert!(
            key_str.starts_with("key_with_ttl_"),
            "Key should start with 'key_with_ttl_', got '{}'",
            key_str
        );

        println!("✓ Key '{}' has expiry: {:?}", key_str, record.expire_at_ms);
    }

    Ok(())
}

#[tokio::test]
async fn test_record_stream_empty_database() -> Result<()> {
    // Create Redis instance without any data
    let redis_instance = RedisConfig::default()
        .with_version(RedisVariant::Redis8_0)
        .build()
        .await?;

    // Get RDB stream without adding any data
    let host = redis_instance.container.get_host().await?;
    let port = redis_instance.container.get_host_port_ipv4(6379).await?;
    let address = format!("{}:{}", host, port);
    let standalone_config = StandaloneConfig {
        address,
        username: None,
        password: None,
    };

    let stream = standalone_config.get_rdb_stream().await?;
    let mut record_stream = RecordStream::new(stream);
    let mut record_count = 0;

    // Try to read records from empty database
    loop {
        match record_stream.next().await {
            Some(Ok(_record)) => {
                record_count += 1;
            }
            Some(Err(e)) => return Err(e),
            None => break,
        }
    }

    // Empty database should have no records
    assert_eq!(
        record_count, 0,
        "Empty database should have no records, found {}",
        record_count
    );

    println!(
        "✓ Empty database test passed: {} records found",
        record_count
    );

    Ok(())
}
