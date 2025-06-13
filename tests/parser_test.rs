use anyhow::Context;
use bytes::Bytes;
use futures_util::FutureExt;
use rdbinsight::{
    helper::AnyResult,
    parser::{Item, rdb_parsers::RDBStr},
};
use tokio;

mod common;

use rdbinsight::parser::{ListEncoding, StringEncoding};

#[tokio::test]
async fn empty_rdb_test() -> AnyResult<()> {
    let redis_instance = common::RedisInstance::new("8.0")
        .await
        .context("create redis instance")?;

    let rdb_path = redis_instance
        .generate_rdb("empty_rdb_test", |_| async { Ok(()) }.boxed())
        .await
        .context("generate rdb file")?;

    let rdb_data = tokio::fs::read(&rdb_path).await.context("read rdb file")?;
    let items = common::parser_utils::collect_items(&rdb_data)?;
    assert_eq!(items.len(), 5);
    assert!(items.iter().all(|item| matches!(item, Item::Aux { .. })));

    Ok(())
}

#[tokio::test]
async fn string_raw_encoding_test() -> AnyResult<()> {
    let redis = common::RedisInstance::new("8.0").await?;

    let rdb_path = redis
        .generate_rdb("string_raw_encoding_test", |conn| {
            async move {
                redis::cmd("SET")
                    .arg("raw_key")
                    .arg("raw_value")
                    .query_async::<()>(conn)
                    .await?;
                Ok(())
            }
            .boxed()
        })
        .await?;

    let bytes = tokio::fs::read(&rdb_path).await?;
    let items = common::parser_utils::collect_items(&bytes)?;
    let items = common::parser_utils::filter_records(items);
    assert_eq!(items.len(), 1);
    let item = items[0].clone();
    let Item::StringRecord { key, encoding, .. } = item else {
        panic!("expected StringRecord");
    };
    assert_eq!(key, RDBStr::Str(Bytes::from("raw_key")));
    assert_eq!(encoding, StringEncoding::Raw);

    Ok(())
}

#[tokio::test]
async fn string_int_encoding_test() -> AnyResult<()> {
    let redis = common::RedisInstance::new("8.0").await?;

    let rdb_path = redis
        .generate_rdb("string_int_encoding_test", |conn| {
            async move {
                redis::cmd("SET")
                    .arg("int_key")
                    .arg(123_i64)
                    .query_async::<()>(conn)
                    .await?;
                Ok(())
            }
            .boxed()
        })
        .await?;

    let bytes = tokio::fs::read(&rdb_path).await?;
    let items = common::parser_utils::collect_items(&bytes)?;
    let items = common::parser_utils::filter_records(items);
    assert_eq!(items.len(), 1);
    let item = items[0].clone();
    let Item::StringRecord { key, encoding, .. } = item else {
        panic!("expected StringRecord");
    };
    assert_eq!(key, RDBStr::Str(Bytes::from("int_key")));
    assert_eq!(encoding, StringEncoding::Int);

    Ok(())
}

#[tokio::test]
async fn string_lzf_encoding_test() -> AnyResult<()> {
    let redis = common::RedisInstance::new("8.0").await?;

    let long_text = "a".repeat(10_000);

    let rdb_path = redis
        .generate_rdb("string_lzf_encoding_test", |conn| {
            async move {
                redis::cmd("CONFIG")
                    .arg("SET")
                    .arg("rdb-save-incremental-fsync")
                    .arg("no")
                    .query_async::<()>(conn)
                    .await?;
                redis::cmd("SET")
                    .arg("lzf_key")
                    .arg(&long_text)
                    .query_async::<()>(conn)
                    .await?;
                Ok(())
            }
            .boxed()
        })
        .await?;

    let bytes = tokio::fs::read(&rdb_path).await?;
    let items = common::parser_utils::collect_items(&bytes)?;
    let items = common::parser_utils::filter_records(items);
    assert_eq!(items.len(), 1);
    let item = items[0].clone();
    let Item::StringRecord { key, encoding, .. } = item else {
        panic!("expected StringRecord");
    };
    assert_eq!(key, RDBStr::Str(Bytes::from("lzf_key")));
    assert_eq!(encoding, StringEncoding::LZF);
    Ok(())
}

// (additional list encoding tests can be added here)

#[tokio::test]
async fn list_quicklist_encoding_test() -> AnyResult<()> {
    let redis = common::RedisInstance::new("6.0").await?;

    let rdb_path = redis
        .generate_rdb("list_quicklist_encoding_test", |conn| {
            async move { common::seed_list(conn, "ql_key", 2000).await }.boxed()
        })
        .await?;

    let bytes = tokio::fs::read(&rdb_path).await?;
    let items = common::parser_utils::collect_items(&bytes)?;
    let items = common::parser_utils::filter_records(items);
    assert_eq!(items.len(), 1);
    let item = items[0].clone();
    let Item::ListRecord {
        key,
        encoding,
        members,
        ..
    } = item
    else {
        panic!("expected ListRecord");
    };
    assert_eq!(key, RDBStr::Str(Bytes::from("ql_key")));
    assert_eq!(members, 2000);
    assert_eq!(encoding, ListEncoding::QuickList);

    Ok(())
}

#[tokio::test]
async fn list_quicklist2_encoding_test() -> AnyResult<()> {
    let redis = common::RedisInstance::new("8.0").await?;

    let rdb_path = redis
        .generate_rdb("list_quicklist2_encoding_test", |conn| {
            async move { common::seed_list(conn, "ql2_key", 2000).await }.boxed()
        })
        .await?;

    let bytes = tokio::fs::read(&rdb_path).await?;
    let items = common::parser_utils::collect_items(&bytes)?;
    let items = common::parser_utils::filter_records(items);
    assert_eq!(items.len(), 1);
    let item = items[0].clone();
    let Item::ListRecord {
        key,
        encoding,
        members,
        ..
    } = item
    else {
        panic!("expected ListRecord");
    };
    assert_eq!(key, RDBStr::Str(Bytes::from("ql2_key")));
    assert_eq!(members, 2000);
    assert_eq!(encoding, ListEncoding::QuickList2);
    Ok(())
}

#[tokio::test]
async fn list_raw_encoding_test() -> AnyResult<()> {
    // Redis 2.8 still produces the legacy LIST encoding (type id = 1)
    let redis = common::RedisInstance::new("2.8").await?;

    let rdb_path = redis
        .generate_rdb("list_raw_encoding_test", |conn| {
            async move { common::seed_list(conn, "list_key", 2000).await }.boxed()
        })
        .await?;

    // Parse the RDB and collect records.
    let bytes = tokio::fs::read(&rdb_path).await?;
    let items = common::parser_utils::collect_items(&bytes)?;
    let items = common::parser_utils::filter_records(items);

    // Expect exactly one ListRecord.
    assert_eq!(items.len(), 1);
    let item = items[0].clone();
    let Item::ListRecord {
        key,
        encoding,
        members,
        ..
    } = item
    else {
        panic!("expected ListRecord");
    };

    assert_eq!(key, RDBStr::Str(Bytes::from("list_key")));
    assert_eq!(members as usize, 2000);
    assert_eq!(encoding, ListEncoding::List);

    Ok(())
}

#[tokio::test]
async fn list_ziplist_encoding_test() -> AnyResult<()> {
    // Redis 2.8 encodes small lists as ziplist (RDB type id = 10)
    let redis = common::RedisInstance::new("2.8").await?;

    // Seed a small list (10 elements) which will stay within ziplist thresholds
    let rdb_path = redis
        .generate_rdb("list_ziplist_encoding_test", |conn| {
            async move { common::seed_list(conn, "zl_key", 300).await }.boxed()
        })
        .await?;

    // Read and parse the generated RDB file
    let bytes = tokio::fs::read(&rdb_path).await?;
    let items = common::parser_utils::collect_items(&bytes)?;
    let items = common::parser_utils::filter_records(items);

    // Expect exactly one ListRecord
    assert_eq!(items.len(), 1);
    let item = items[0].clone();

    let Item::ListRecord {
        key,
        encoding,
        members,
        ..
    } = item
    else {
        panic!("expected ListRecord");
    };

    assert_eq!(key, RDBStr::Str(Bytes::from("zl_key")));
    assert_eq!(members as usize, 300);
    assert_eq!(encoding, ListEncoding::ZipList);

    Ok(())
}

#[tokio::test]
async fn list_ziplist_scan_path_test() -> AnyResult<()> {
    // Redis 2.8 + tweaked ziplist thresholds to trigger zllen == 0xFFFF
    let redis = common::RedisInstance::new("2.8").await?;

    const ELEMENT_COUNT: usize = 66_000; // > 65_534 so Redis writes 0xFFFF

    let rdb_path = redis
        .generate_rdb("list_ziplist_scan_path_test", |conn| {
            async move {
                // Allow very large ziplist nodes (entries < 70_000 & val length <= 64)
                redis::cmd("CONFIG")
                    .arg("SET")
                    .arg("list-max-ziplist-entries")
                    .arg("70000")
                    .query_async::<()>(&mut *conn)
                    .await?;
                // Keep entry size small (we push short integers as strings)
                redis::cmd("CONFIG")
                    .arg("SET")
                    .arg("list-max-ziplist-value")
                    .arg("64")
                    .query_async::<()>(&mut *conn)
                    .await?;

                // Disable RDB compression to ensure predictable encoding
                redis::cmd("CONFIG")
                    .arg("SET")
                    .arg("rdbcompression")
                    .arg("no")
                    .query_async::<()>(&mut *conn)
                    .await?;

                common::seed_list(conn, "zl_ff_key", ELEMENT_COUNT).await?;
                Ok(())
            }
            .boxed()
        })
        .await?;

    // Parse RDB and collect items
    let bytes = tokio::fs::read(&rdb_path).await?;
    let items = common::parser_utils::collect_items(&bytes)?;
    let items = common::parser_utils::filter_records(items);

    assert_eq!(items.len(), 1, "expect exactly one ListRecord");
    let Item::ListRecord {
        key,
        encoding,
        members,
        ..
    } = &items[0]
    else {
        panic!("expected ListRecord");
    };

    assert_eq!(*key, RDBStr::Str(Bytes::from("zl_ff_key")));
    assert_eq!(*members as usize, ELEMENT_COUNT);
    assert_eq!(*encoding, ListEncoding::ZipList);

    Ok(())
}

#[tokio::test]
async fn list_quicklist_lzf_compressed_test() -> AnyResult<()> {
    // Redis 6.0 writes QuickList nodes and, with default rdbcompression=yes, attempts LZF
    // compression for each node whose serialized size > 20 bytes. A list with thousands of
    // small elements comfortably exceeds that threshold, guaranteeing LZFStr encoding.
    const ELEMENTS: usize = 4000;

    let redis = common::RedisInstance::new("6.0").await?;

    let rdb_path = redis
        .generate_rdb("list_quicklist_lzf_compressed_test", |conn| {
            async move {
                // Make sure RDB compression is enabled (it is by default, but we set explicitly)
                redis::cmd("CONFIG")
                    .arg("SET")
                    .arg("rdbcompression")
                    .arg("yes")
                    .query_async::<()>(&mut *conn)
                    .await?;
                redis::cmd("CONFIG")
                    .arg("SET")
                    .arg("rdb-save-incremental-fsync")
                    .arg("no")
                    .query_async::<()>(&mut *conn)
                    .await?;

                // Populate the list with incremental integers as strings.
                common::seed_list(conn, "ql_lzf_key", ELEMENTS).await?;

                Ok(())
            }
            .boxed()
        })
        .await?;

    // Parse the RDB and verify parser output.
    let bytes = tokio::fs::read(&rdb_path).await?;
    let items = common::parser_utils::collect_items(&bytes)?;
    let items = common::parser_utils::filter_records(items);

    assert_eq!(items.len(), 1, "should produce exactly one ListRecord");

    let Item::ListRecord {
        key,
        encoding,
        members,
        ..
    } = &items[0]
    else {
        panic!("expected ListRecord");
    };

    assert_eq!(*key, RDBStr::Str(Bytes::from("ql_lzf_key")));
    assert_eq!(*members as usize, ELEMENTS);
    assert_eq!(*encoding, ListEncoding::QuickList);

    Ok(())
}
