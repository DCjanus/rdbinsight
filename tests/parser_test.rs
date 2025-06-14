//! Note: Tests that rely on `common::trace::capture()` use `#[tokio::test(flavor = "current_thread")]`
//! to ensure the tracing subscriber remains active within the single thread that executes the parser.
//! This avoids missing events when tasks hop between worker threads in Tokio's multi-thread runtime.

use std::path::Path;

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

use common::{RedisInstance, config_set_many, trace};

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

    let items = read_filtered_items(&rdb_path).await?;
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

    let items = read_filtered_items(&rdb_path).await?;
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

    let items = read_filtered_items(&rdb_path).await?;
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

    let items = read_filtered_items(&rdb_path).await?;

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

    let items = read_filtered_items(&rdb_path).await?;

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
                config_set_many(conn, &[
                    ("list-max-ziplist-entries", "70000"),
                    ("list-max-ziplist-value", "64"),
                ])
                .await?;

                common::seed_list(conn, "zl_ff_key", ELEMENT_COUNT).await?;
                Ok(())
            }
            .boxed()
        })
        .await?;

    let items = read_filtered_items(&rdb_path).await?;

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

    let items = read_filtered_items(&rdb_path).await?;
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

#[tokio::test(flavor = "current_thread")]
async fn list_quicklist_lzf_compressed_test() -> AnyResult<()> {
    // Redis 6.0 writes QuickList nodes and, with default rdbcompression=yes, attempts LZF
    // compression for each node whose serialized size > 20 bytes.
    const ELEMENTS: usize = 4000;

    let redis = RedisInstance::new("6.0").await?;
    let guard = trace::capture();

    let rdb_path = redis
        .generate_rdb("list_quicklist_lzf_compressed_test", |conn| {
            async move {
                common::seed_list(conn, "ql_lzf_key", ELEMENTS).await?;
                Ok(())
            }
            .boxed()
        })
        .await?;

    let items = read_filtered_items(&rdb_path).await?;

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
    assert!(
        guard.hit("quicklist.ziplist.lzf"),
        "expected quicklist.ziplist.lzf trace event; captured: {:?}",
        guard.collected()
    );

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

    let items = read_filtered_items(&rdb_path).await?;

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

#[tokio::test(flavor = "current_thread")]
async fn set_intset_encoding_test() -> AnyResult<()> {
    // Redis 2.8 encodes small integer-only sets as intset (type id = 11)
    const MEMBER_COUNT: usize = 50;

    let redis = RedisInstance::new("2.8").await?;
    let guard = trace::capture();

    let rdb_path = redis
        .generate_rdb("set_intset_encoding_test", |conn| {
            async move { common::seed_set(conn, "intset_key", MEMBER_COUNT).await }.boxed()
        })
        .await?;

    let items = read_filtered_items(&rdb_path).await?;

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
    assert!(
        guard.hit("intset.raw"),
        "expected intset.raw trace event; captured: {:?}",
        guard.collected()
    );

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
                let mut pipe = redis::pipe();
                for idx in 0..MEMBER_COUNT {
                    pipe.sadd("lp_key", format!("m{}", idx)).ignore();
                }
                pipe.query_async::<()>(conn).await?;
                Ok(())
            }
            .boxed()
        })
        .await?;

    let items = read_filtered_items(&rdb_path).await?;

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
                config_set_many(conn, &[
                    ("set-max-listpack-entries", "70000"),
                    ("rdbcompression", "no"),
                ])
                .await?;

                // TODO(perf): this test is slow—the majority of the runtime
                // is spent queuing 66 000 SADD commands.

                // Seed a large non-integer set so Redis keeps listpack encoding but writes lp_len = 0xFFFF
                let mut pipe = redis::pipe();
                for idx in 0..MEMBER_COUNT {
                    pipe.sadd("lp_ff_key", format!("m{}", idx)).ignore();
                }
                pipe.query_async::<()>(conn).await?;
                Ok(())
            }
            .boxed()
        })
        .await?;

    let items = read_filtered_items(&rdb_path).await?;

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

#[tokio::test]
async fn list_quicklist2_encoding_test() -> AnyResult<()> {
    // Redis 7.0 encodes lists using the newer QuickList2 format (RDB type id = 17)
    const MEMBER_COUNT: usize = 2000;

    let redis = RedisInstance::new("7.0").await?;

    let rdb_path = redis
        .generate_rdb("list_quicklist2_encoding_test", |conn| {
            async move { common::seed_list(conn, "ql2_key", MEMBER_COUNT).await }.boxed()
        })
        .await?;

    let items = read_filtered_items(&rdb_path).await?;

    // Expect exactly one ListRecord.
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

    assert_eq!(*key, RDBStr::Str(Bytes::from("ql2_key")));
    assert_eq!(*member_count as usize, MEMBER_COUNT);
    assert_eq!(*encoding, ListEncoding::QuickList2);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn list_quicklist2_plain_node_test() -> AnyResult<()> {
    // Plain node is produced when an element exceeds list-max-listpack-size (8 KiB by default).
    let redis = RedisInstance::new("8.0").await?;
    let guard = trace::capture();

    const LARGE_TEXT_SIZE: usize = 10_000; // > 8 KiB
    let large_text = "X".repeat(LARGE_TEXT_SIZE);

    let rdb_path = redis
        .generate_rdb("list_quicklist2_plain_node_test", |conn| {
            async move {
                // Ensure deterministic quicklist behaviour.
                config_set_many(conn, &[
                    ("list-max-listpack-size", "-2"),
                    ("list-compress-depth", "0"),
                    ("rdbcompression", "no"),
                ])
                .await?;

                redis::cmd("RPUSH")
                    .arg("ql2_plain_key")
                    .arg(&large_text)
                    .query_async::<()>(conn)
                    .await?;
                Ok(())
            }
            .boxed()
        })
        .await?;

    let items = read_filtered_items(&rdb_path).await?;

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

    assert_eq!(*key, RDBStr::Str(Bytes::from("ql2_plain_key")));
    assert_eq!(*member_count as usize, 1);
    assert_eq!(*encoding, ListEncoding::QuickList2);
    assert!(
        guard.hit("quicklist2.plain"),
        "expected quicklist2.plain trace event; captured: {:?}",
        guard.collected()
    );

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn list_quicklist2_listpack_raw_node_test() -> AnyResult<()> {
    // Disable RDB compression to force raw listpack serialization for each node.
    const ELEMENTS: usize = 2000;

    let redis = RedisInstance::new("7.0").await?;
    let guard = trace::capture();

    let rdb_path = redis
        .generate_rdb("list_quicklist2_listpack_raw_node_test", |conn| {
            async move {
                // Ensure deterministic quicklist behaviour.
                config_set_many(conn, &[
                    ("rdbcompression", "no"),
                    ("list-max-listpack-size", "-2"),
                    ("list-compress-depth", "0"),
                ])
                .await?;

                common::seed_list(conn, "ql2_lp_raw_key", ELEMENTS).await?;
                Ok(())
            }
            .boxed()
        })
        .await?;

    let items = read_filtered_items(&rdb_path).await?;

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

    assert_eq!(*key, RDBStr::Str(Bytes::from("ql2_lp_raw_key")));
    assert_eq!(*member_count as usize, ELEMENTS);
    assert_eq!(*encoding, ListEncoding::QuickList2);
    assert!(
        guard.hit("quicklist2.packed.raw"),
        "expected quicklist2.packed.raw trace event; captured: {:?}",
        guard.collected()
    );

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn list_quicklist2_listpack_lzf_node_test() -> AnyResult<()> {
    // With rdbcompression=yes (default) and many elements, Redis will LZF-compress each listpack node.
    const ELEMENTS: usize = 8000;

    let redis = RedisInstance::new("8.0").await?;
    let guard = trace::capture();

    let rdb_path = redis
        .generate_rdb("list_quicklist2_listpack_lzf_node_test", |conn| {
            async move {
                // Ensure deterministic quicklist behaviour (compression enabled by default)
                config_set_many(conn, &[
                    ("list-max-listpack-size", "-3"),
                    ("list-compress-depth", "1"),
                ])
                .await?;

                common::seed_list(conn, "ql2_lp_lzf_key", ELEMENTS).await?;
                Ok(())
            }
            .boxed()
        })
        .await?;

    let items = read_filtered_items(&rdb_path).await?;

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

    assert_eq!(*key, RDBStr::Str(Bytes::from("ql2_lp_lzf_key")));
    assert_eq!(*member_count as usize, ELEMENTS);
    assert_eq!(*encoding, ListEncoding::QuickList2);
    assert!(
        guard.hit("quicklist2.packed.lzf"),
        "expected quicklist2.packed.lzf trace event; captured: {:?}",
        guard.collected()
    );

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn list_quicklist_ziplist_raw_node_test() -> AnyResult<()> {
    // Disable RDB compression to guarantee raw ziplist nodes inside QuickList
    const ELEMENTS: usize = 4000;

    let redis = RedisInstance::new("6.0").await?;
    let guard = trace::capture();

    let rdb_path = redis
        .generate_rdb("list_quicklist_ziplist_raw_node_test", |conn| {
            async move {
                config_set_many(conn, &[("rdbcompression", "no")]).await?;
                // keep default list-max-ziplist-size so each node will be listpack threshold; same as LZF test
                common::seed_list(conn, "ql_raw_key", ELEMENTS).await?;
                Ok(())
            }
            .boxed()
        })
        .await?;

    let items = read_filtered_items(&rdb_path).await?;

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
    assert_eq!(*key, RDBStr::Str(Bytes::from("ql_raw_key")));
    assert_eq!(*member_count as usize, ELEMENTS);
    assert_eq!(*encoding, ListEncoding::QuickList);
    assert!(
        guard.hit("quicklist.ziplist.raw"),
        "expected quicklist.ziplist.raw trace event; captured: {:?}",
        guard.collected()
    );

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

async fn read_filtered_items<P>(path: P) -> AnyResult<Vec<Item>>
where P: AsRef<Path> {
    let bytes = tokio::fs::read(path.as_ref()).await?;
    let items = collect_items(&bytes)?;
    Ok(items
        .into_iter()
        .filter(|item| !matches!(item, Item::Aux { .. }))
        .filter(|item| !matches!(item, Item::SelectDB { .. }))
        .filter(|item| !matches!(item, Item::ResizeDB { .. }))
        .collect())
}
