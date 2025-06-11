use std::task::Poll;

use anyhow::Context;
use rdbinsight::{
    helper::AnyResult,
    parser::{Item, Parser, rdb_parsers::RDBStr},
};
use redis::Commands;
use tokio;
use tracing::info;

mod common;

#[tokio::test]
async fn parser_unified_test() -> AnyResult<()> {
    let redis_instance = common::RedisInstance::new("8.0")
        .await
        .context("create redis instance")?;

    let rdb_path = redis_instance
        .generate_rdb("parser_unified_test", |conn| {
            let _: () = conn
                .set("string_key", "string_value")
                .expect("set string_key");
            let _: () = conn.set("int_key", 123).expect("set int_key");
            // TODO: add more data types
            Ok(())
        })
        .await
        .context("generate rdb file")?;

    let rdb_data = tokio::fs::read(&rdb_path).await.context("read rdb file")?;
    let mut parser = Parser::default();
    parser.feed(&rdb_data)?;

    let mut items = Vec::new();
    loop {
        match parser.parse_next()? {
            Poll::Ready(Some(item)) => {
                items.push(item);
            }
            Poll::Ready(None) => {
                break;
            }
            Poll::Pending => {
                info!("waiting for more data");
            }
        }
    }

    // TODO: Extract this validation logic into a common function for reuse across tests
    // TODO: Add tests for large key scenarios to verify memory usage patterns
    let non_aux_items: Vec<_> = items
        .iter()
        .filter(|item| !matches!(item, Item::Aux { .. }))
        .collect();
    assert_eq!(
        non_aux_items.len(),
        4,
        "Expected 4 non-aux items, got {}",
        non_aux_items.len()
    );

    let mut select_db_count = 0;
    let mut resize_db_count = 0;
    let mut string_record_count = 0;
    let mut found_string_key = false;
    let mut found_int_key = false;

    for item in &non_aux_items {
        match item {
            Item::SelectDB { db } => {
                select_db_count += 1;
                assert_eq!(*db, 0, "Expected database 0, got {}", db);
            }
            Item::ResizeDB {
                table_size,
                ttl_table_size: _,
            } => {
                resize_db_count += 1;
                assert!(*table_size > 0, "Table size should be greater than 0");
            }
            Item::StringRecord {
                key,
                rdb_size,
                mem_size,
            } => {
                string_record_count += 1;

                match key {
                    RDBStr::Str(bytes) => {
                        let key_str =
                            std::str::from_utf8(bytes).context("key should be valid UTF-8")?;
                        match key_str {
                            "string_key" => {
                                found_string_key = true;
                            }
                            "int_key" => {
                                found_int_key = true;
                            }
                            _ => {
                                panic!("Unexpected key: {}", key_str);
                            }
                        }
                    }
                    RDBStr::Int(_) => {
                        panic!("Key should not be an integer: {:?}", key);
                    }
                }

                assert!(*rdb_size > 0, "RDB size should be greater than 0");
                assert!(*mem_size > 0, "Memory size should be greater than 0");
            }
            Item::Aux { .. } => {
                unreachable!("Aux items should have been filtered out");
            }
        }
    }

    assert_eq!(
        select_db_count, 1,
        "Expected exactly 1 SelectDB item, got {}",
        select_db_count
    );
    assert_eq!(
        resize_db_count, 1,
        "Expected exactly 1 ResizeDB item, got {}",
        resize_db_count
    );
    assert_eq!(
        string_record_count, 2,
        "Expected exactly 2 StringRecord items, got {}",
        string_record_count
    );

    assert!(found_string_key, "Did not find string_key record");
    assert!(found_int_key, "Did not find int_key record");

    Ok(())
}

#[tokio::test]
async fn empty_rdb_test() -> AnyResult<()> {
    let redis_instance = common::RedisInstance::new("8.0")
        .await
        .context("create redis instance")?;

    let rdb_path = redis_instance
        .generate_rdb("empty_rdb_test", |_| Ok(()))
        .await
        .context("generate rdb file")?;

    let rdb_data = tokio::fs::read(&rdb_path).await.context("read rdb file")?;
    let mut parser = Parser::default();
    parser.feed(&rdb_data)?;

    let mut items = Vec::new();
    loop {
        match parser.parse_next()? {
            Poll::Ready(Some(item)) => {
                items.push(item);
            }
            Poll::Ready(None) => {
                break;
            }
            Poll::Pending => {
                info!("waiting for more data");
            }
        }
    }
    for item in items {
        assert!(matches!(item, Item::Aux { .. }));
    }

    Ok(())
}
