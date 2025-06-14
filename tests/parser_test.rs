use anyhow::{Context, ensure};
use bytes::Bytes;
use futures_util::FutureExt;
use rdbinsight::{
    helper::AnyResult,
    parser::{
        Buffer, Item, ListEncoding, RDBFileParser, SetEncoding, StringEncoding,
        combinators::NotFinished, rdb_parsers::RDBStr,
    },
};

mod common;

use common::RedisInstance;

#[tokio::test]
async fn empty_rdb_test() -> AnyResult<()> {
    let redis_instance = RedisInstance::new("8.0")
        .await
        .context("create redis instance")?;

    let rdb_path = redis_instance
        .generate_rdb("empty_rdb_test", |_| async { Ok(()) }.boxed())
        .await
        .context("generate rdb file")?;

    let rdb_data = tokio::fs::read(&rdb_path).await.context("read rdb file")?;
    let items = collect_items(&rdb_data)?;
    assert_eq!(items.len(), 5);
    assert!(items.iter().all(|item| matches!(item, Item::Aux { .. })));

    Ok(())
}

#[tokio::test]
async fn string_raw_encoding_test() -> AnyResult<()> {
    let redis = RedisInstance::new("8.0").await?;

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
    let items = collect_items(&bytes)?;
    let items = filter_records(items);
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
    let redis = RedisInstance::new("8.0").await?;

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
    let items = collect_items(&bytes)?;
    let items = filter_records(items);
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
    let redis = RedisInstance::new("8.0").await?;

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
    let items = collect_items(&bytes)?;
    let items = filter_records(items);
    assert_eq!(items.len(), 1);
    let item = items[0].clone();
    let Item::StringRecord { key, encoding, .. } = item else {
        panic!("expected StringRecord");
    };
    assert_eq!(key, RDBStr::Str(Bytes::from("lzf_key")));
    assert_eq!(encoding, StringEncoding::LZF);

    Ok(())
}

#[tokio::test]
async fn list_raw_encoding_test() -> AnyResult<()> {
    // Redis 2.8 still produces the legacy LIST encoding (type id = 1)
    let redis = RedisInstance::new("2.8").await?;

    let rdb_path = redis
        .generate_rdb("list_raw_encoding_test", |conn| {
            async move { common::seed_list(conn, "list_key", 2000).await }.boxed()
        })
        .await?;

    // Parse the RDB and collect records.
    let bytes = tokio::fs::read(&rdb_path).await?;
    let items = collect_items(&bytes)?;
    let items = filter_records(items);

    // Expect exactly one ListRecord.
    assert_eq!(items.len(), 1);
    let item = items[0].clone();
    let Item::ListRecord {
        key,
        encoding,
        member_count,
        ..
    } = item
    else {
        panic!("expected ListRecord");
    };

    assert_eq!(key, RDBStr::Str(Bytes::from("list_key")));
    assert_eq!(member_count as usize, 2000);
    assert_eq!(encoding, ListEncoding::List);

    Ok(())
}

#[tokio::test]
async fn list_ziplist_encoding_test() -> AnyResult<()> {
    // Redis 2.8 encodes small lists as ziplist (RDB type id = 10)
    let redis = RedisInstance::new("2.8").await?;

    // Seed a small list (300 elements) which will stay within ziplist thresholds
    let rdb_path = redis
        .generate_rdb("list_ziplist_encoding_test", |conn| {
            async move { common::seed_list(conn, "zl_key", 300).await }.boxed()
        })
        .await?;

    // Read and parse the generated RDB file
    let bytes = tokio::fs::read(&rdb_path).await?;
    let items = collect_items(&bytes)?;
    let items = filter_records(items);

    // Expect exactly one ListRecord
    assert_eq!(items.len(), 1);
    let item = items[0].clone();

    let Item::ListRecord {
        key,
        encoding,
        member_count,
        ..
    } = item
    else {
        panic!("expected ListRecord");
    };

    assert_eq!(key, RDBStr::Str(Bytes::from("zl_key")));
    assert_eq!(member_count as usize, 300);
    assert_eq!(encoding, ListEncoding::ZipList);

    Ok(())
}

#[tokio::test]
async fn list_ziplist_scan_path_test() -> AnyResult<()> {
    // Redis 2.8 + tweaked ziplist thresholds to trigger zllen == 0xFFFF
    let redis = RedisInstance::new("2.8").await?;

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
    let items = collect_items(&bytes)?;
    let items = filter_records(items);

    assert_eq!(items.len(), 1, "expect exactly one ListRecord");
    let Item::ListRecord {
        key,
        encoding,
        member_count,
        ..
    } = &items[0]
    else {
        panic!("expected ListRecord");
    };

    assert_eq!(*key, RDBStr::Str(Bytes::from("zl_ff_key")));
    assert_eq!(*member_count as usize, ELEMENT_COUNT);
    assert_eq!(*encoding, ListEncoding::ZipList);

    Ok(())
}

#[tokio::test]
async fn list_quicklist_encoding_test() -> AnyResult<()> {
    let redis = RedisInstance::new("6.0").await?;

    let rdb_path = redis
        .generate_rdb("list_quicklist_encoding_test", |conn| {
            async move { common::seed_list(conn, "ql_key", 2000).await }.boxed()
        })
        .await?;

    let bytes = tokio::fs::read(&rdb_path).await?;
    let items = collect_items(&bytes)?;
    let items = filter_records(items);
    assert_eq!(items.len(), 1);
    let item = items[0].clone();
    let Item::ListRecord {
        key,
        encoding,
        member_count,
        ..
    } = item
    else {
        panic!("expected ListRecord");
    };
    assert_eq!(key, RDBStr::Str(Bytes::from("ql_key")));
    assert_eq!(member_count, 2000);
    assert_eq!(encoding, ListEncoding::QuickList);

    Ok(())
}

#[tokio::test]
async fn list_quicklist_lzf_compressed_test() -> AnyResult<()> {
    // Redis 6.0 writes QuickList nodes and, with default rdbcompression=yes, attempts LZF
    // compression for each node whose serialized size > 20 bytes.
    const ELEMENTS: usize = 4000;

    let redis = RedisInstance::new("6.0").await?;

    let rdb_path = redis
        .generate_rdb("list_quicklist_lzf_compressed_test", |conn| {
            async move {
                // Make sure RDB compression is enabled (it is by default, but set explicitly)
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
                common::seed_list(conn, "ql_lzf_key", ELEMENTS).await?;
                Ok(())
            }
            .boxed()
        })
        .await?;

    let bytes = tokio::fs::read(&rdb_path).await?;
    let items = collect_items(&bytes)?;
    let items = filter_records(items);

    assert_eq!(items.len(), 1, "should produce exactly one ListRecord");

    let Item::ListRecord {
        key,
        encoding,
        member_count,
        ..
    } = &items[0]
    else {
        panic!("expected ListRecord");
    };

    assert_eq!(*key, RDBStr::Str(Bytes::from("ql_lzf_key")));
    assert_eq!(*member_count as usize, ELEMENTS);
    assert_eq!(*encoding, ListEncoding::QuickList);

    Ok(())
}

#[tokio::test]
async fn set_raw_encoding_test() -> AnyResult<()> {
    // Redis 2.8 encodes large integer sets as raw hash table (type id = 2)
    const MEMBER_COUNT: usize = 3_000;

    let redis = RedisInstance::new("2.8").await?;

    let rdb_path = redis
        .generate_rdb("set_raw_encoding_test", |conn| {
            async move { common::seed_set(conn, "set_raw_key", MEMBER_COUNT).await }.boxed()
        })
        .await?;

    let bytes = tokio::fs::read(&rdb_path).await?;
    let items = collect_items(&bytes)?;
    let items = filter_records(items);

    assert_eq!(items.len(), 1, "expected exactly one SetRecord");
    let item = items[0].clone();
    let Item::SetRecord {
        key,
        encoding,
        member_count,
        ..
    } = item
    else {
        panic!("expected SetRecord");
    };

    assert_eq!(key, RDBStr::Str(Bytes::from("set_raw_key")));
    assert_eq!(member_count as usize, MEMBER_COUNT);
    assert_eq!(encoding, SetEncoding::Raw);

    Ok(())
}

#[tokio::test]
async fn set_intset_encoding_test() -> AnyResult<()> {
    // Redis 2.8 encodes small integer-only sets as intset (type id = 11)
    const MEMBER_COUNT: usize = 50;

    let redis = RedisInstance::new("2.8").await?;

    let rdb_path = redis
        .generate_rdb("set_intset_encoding_test", |conn| {
            async move { common::seed_set(conn, "intset_key", MEMBER_COUNT).await }.boxed()
        })
        .await?;

    let bytes = tokio::fs::read(&rdb_path).await?;
    let items = collect_items(&bytes)?;
    let items = filter_records(items);

    assert_eq!(items.len(), 1);
    let item = items[0].clone();
    let Item::SetRecord {
        key,
        encoding,
        member_count,
        ..
    } = item
    else {
        panic!("expected SetRecord");
    };

    assert_eq!(key, RDBStr::Str(Bytes::from("intset_key")));
    assert_eq!(member_count as usize, MEMBER_COUNT);
    assert_eq!(encoding, SetEncoding::IntSet);

    Ok(())
}

#[tokio::test]
async fn set_listpack_encoding_test() -> AnyResult<()> {
    // Redis 7.2 encodes small non-integer sets using listpack (type id = 13)
    const MEMBER_COUNT: usize = 30;

    let redis = RedisInstance::new("7.2").await?;

    let rdb_path = redis
        .generate_rdb("set_listpack_encoding_test", |conn| {
            async move {
                // Use non–integer members to avoid intset encoding
                let mut pipe = redis::pipe();
                for idx in 0..MEMBER_COUNT {
                    pipe.sadd("lp_key", format!("m{}", idx)).ignore();
                }
                pipe.query_async::<()>(&mut *conn).await?;
                // Disable RDB compression to guarantee raw listpack data for easier parsing
                redis::cmd("CONFIG")
                    .arg("SET")
                    .arg("rdbcompression")
                    .arg("no")
                    .query_async::<()>(&mut *conn)
                    .await?;
                redis::cmd("CONFIG")
                    .arg("SET")
                    .arg("rdb-save-incremental-fsync")
                    .arg("no")
                    .query_async::<()>(&mut *conn)
                    .await?;
                Ok(())
            }
            .boxed()
        })
        .await?;

    let bytes = tokio::fs::read(&rdb_path).await?;
    let items = collect_items(&bytes)?;
    let items = filter_records(items);

    assert_eq!(items.len(), 1, "should produce exactly one SetRecord");
    let Item::SetRecord {
        key,
        encoding,
        member_count,
        ..
    } = &items[0]
    else {
        panic!("expected SetRecord");
    };

    assert_eq!(*key, RDBStr::Str(Bytes::from("lp_key")));
    assert_eq!(*member_count as usize, MEMBER_COUNT);
    assert_eq!(*encoding, SetEncoding::ListPack);

    Ok(())
}

#[tokio::test]
async fn set_listpack_scan_path_test() -> AnyResult<()> {
    // Redis 7.2 + tweaked listpack thresholds to trigger lp_length == 0xFFFF
    const MEMBER_COUNT: usize = 66_000; // > 65_534 so Redis writes 0xFFFF

    let redis = RedisInstance::new("7.2").await?;

    let rdb_path = redis
        .generate_rdb("set_listpack_scan_path_test", |conn| {
            async move {
                // Allow very large listpack (entries < 70_000 & val length <= 64)
                redis::cmd("CONFIG")
                    .arg("SET")
                    .arg("set-max-listpack-entries")
                    .arg("70000")
                    .query_async::<()>(&mut *conn)
                    .await?;

                // Disable RDB compression to ensure predictable encoding
                redis::cmd("CONFIG")
                    .arg("SET")
                    .arg("rdbcompression")
                    .arg("no")
                    .query_async::<()>(&mut *conn)
                    .await?;
                // Disable incremental fsync to avoid extra header records
                redis::cmd("CONFIG")
                    .arg("SET")
                    .arg("rdb-save-incremental-fsync")
                    .arg("no")
                    .query_async::<()>(&mut *conn)
                    .await?;

                // TODO(perf): this test is slow—the majority of the runtime
                // is spent queuing 66 000 SADD commands.

                // Seed a large non-integer set so Redis keeps listpack encoding but writes lp_len = 0xFFFF
                let mut pipe = redis::pipe();
                for idx in 0..MEMBER_COUNT {
                    pipe.sadd("lp_ff_key", format!("m{}", idx)).ignore();
                }
                pipe.query_async::<()>(&mut *conn).await?;
                Ok(())
            }
            .boxed()
        })
        .await?;

    // Parse the generated RDB file and collect items
    let bytes = tokio::fs::read(&rdb_path).await?;
    let items = collect_items(&bytes)?;
    let items = filter_records(items);

    // Expect exactly one SetRecord.
    assert_eq!(items.len(), 1, "expect exactly one SetRecord");

    let Item::SetRecord {
        key,
        encoding,
        member_count,
        ..
    } = &items[0]
    else {
        panic!("expected SetRecord");
    };

    assert_eq!(*key, RDBStr::Str(Bytes::from("lp_ff_key")));
    assert_eq!(*member_count as usize, MEMBER_COUNT);
    assert_eq!(*encoding, SetEncoding::ListPack);

    Ok(())
}

fn collect_items(bytes: &[u8]) -> AnyResult<Vec<Item>> {
    let mut parser = RDBFileParser::default();
    let mut buffer = Buffer::new(1024 * 1024 * 64);
    buffer.extend(bytes)?;
    let mut items = Vec::new();
    let mut pending = 0;
    loop {
        match parser.poll_next(&mut buffer) {
            Ok(Some(item)) => {
                pending = 0;
                items.push(item);
            }
            Ok(None) => break,
            Err(e) if e.is::<NotFinished>() => {
                pending += 1;
                ensure!(pending < 1000000, "pending items: {}", pending);
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
    Ok(items)
}

fn filter_records(items: Vec<Item>) -> Vec<Item> {
    items
        .into_iter()
        .filter(|item| !matches!(item, Item::Aux { .. }))
        .filter(|item| !matches!(item, Item::SelectDB { .. }))
        .filter(|item| !matches!(item, Item::ResizeDB { .. }))
        .collect()
}
