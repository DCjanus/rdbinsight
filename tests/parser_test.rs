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
        Buffer, HashEncoding, Item, ListEncoding, RDBFileParser, SetEncoding, StringEncoding,
        ZSetEncoding, combinators::NotFinished, rdb_parsers::RDBStr,
    },
};

mod common;

use common::{RedisInstance, config_set_many, seed_hash, seed_zset, trace};

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
    const MEMBER_COUNT: usize = 30;

    let redis = RedisInstance::new("8.0").await?;

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
    const MEMBER_COUNT: usize = 66_000; // > 65_534 so Redis writes 0xFFFF

    let redis = RedisInstance::new("8.0").await?;

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
    const MEMBER_COUNT: usize = 2000;

    let redis = RedisInstance::new("8.0").await?;

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

    let redis = RedisInstance::new("8.0").await?;
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

#[tokio::test]
async fn set_listpack_large_string_variants_test() -> AnyResult<()> {
    let redis = RedisInstance::new("8.0").await?;
    const MEMBER_COUNT: usize = 66_000; // > 65 534 triggers 0xFFFF

    // > 4095 bytes → 32-bit header
    let big_str = "X".repeat(5_000);
    // 100-byte template → 12-bit header
    let small_template = "Y".repeat(100);

    let rdb_path = redis
        .generate_rdb("set_listpack_large_string_variants_test", |conn| {
            async move {
                // Keep listpack encoding and disable compression.
                config_set_many(conn, &[
                    ("set-max-listpack-entries", "70000"),
                    ("set-max-listpack-value", "6000"),
                    ("rdbcompression", "no"),
                ])
                .await?;

                let mut pipe = redis::pipe();
                pipe.sadd("lp_var_key", &big_str).ignore();

                for idx in 0..MEMBER_COUNT {
                    let val = format!("{small_template}{idx:0>5}");
                    pipe.sadd("lp_var_key", val).ignore();
                }

                pipe.query_async::<()>(&mut *conn).await?;
                Ok(())
            }
            .boxed()
        })
        .await?;

    let items = read_filtered_items(&rdb_path).await?;

    assert_eq!(items.len(), 1);
    let Item::SetRecord {
        key,
        encoding,
        member_count,
        ..
    } = &items[0]
    else {
        panic!("expected SetRecord");
    };

    assert_eq!(*key, RDBStr::Str(Bytes::from("lp_var_key")));
    assert_eq!(*member_count as usize, MEMBER_COUNT + 1);
    assert_eq!(*encoding, SetEncoding::ListPack);

    Ok(())
}

#[tokio::test]
async fn set_listpack_integer_variants_test() -> AnyResult<()> {
    const FILLER_COUNT: usize = 66_000; // > 65_534 so Redis writes lp_len == 0xFFFF and the parser takes the slow scan path.
    const SPECIAL_INTS: [&str; 6] = [
        "63",                  // 7-bit unsigned int (flag 0x00..0x7F)
        "4095",                // 13-bit signed int (flag 0xC0..0xDF)
        "32767",               // 16-bit signed int (flag 0xF1)
        "8388607",             // 24-bit signed int (flag 0xF2)
        "2147483647",          // 32-bit signed int (flag 0xF3)
        "9223372036854775807", // 64-bit signed int (flag 0xF4)
    ];

    let redis = RedisInstance::new("8.0").await?;

    let rdb_path = redis
        .generate_rdb("set_listpack_integer_variants_test", |conn| {
            async move {
                // Ensure listpack encoding and disable RDB compression to keep raw listpack payloads.
                config_set_many(conn, &[
                    ("set-max-listpack-entries", "70000"),
                    ("set-max-listpack-value", "64"),
                    ("rdbcompression", "no"),
                ])
                .await?;

                // Insert integer members that map to each listpack integer flag.
                let mut pipe = redis::pipe();
                for val in SPECIAL_INTS.iter() {
                    pipe.sadd("lp_int_key", *val).ignore();
                }

                // Add many non-integer members so that member count is unknown (0xFFFF) to hit the slow counting path.
                for idx in 0..FILLER_COUNT {
                    pipe.sadd("lp_int_key", format!("m{}", idx)).ignore();
                }
                pipe.query_async::<()>(conn).await?;
                Ok(())
            }
            .boxed()
        })
        .await?;

    let items = read_filtered_items(&rdb_path).await?;

    // Expect exactly one SetRecord
    assert_eq!(items.len(), 1);
    let Item::SetRecord {
        key,
        encoding,
        member_count,
        ..
    } = &items[0]
    else {
        panic!("expected SetRecord");
    };

    let expected_count = FILLER_COUNT + SPECIAL_INTS.len();
    assert_eq!(*key, RDBStr::Str(Bytes::from("lp_int_key")));
    assert_eq!(*member_count as usize, expected_count);
    assert_eq!(*encoding, SetEncoding::ListPack);

    Ok(())
}

// -------------------------- ZSet Tests -----------------------------

#[tokio::test(flavor = "current_thread")]
async fn zset_skiplist_encoding_test() -> AnyResult<()> {
    // Insert >128 members so Redis 2.8 stores ZSET as raw skiplist (type id 3).
    const MEMBER_COUNT: usize = 300;

    let redis = RedisInstance::new("2.8").await?;
    let guard = trace::capture();

    let rdb_path = redis
        .generate_rdb("zset_skiplist_encoding_test", |conn| {
            async move { seed_zset(conn, "zsl_key", MEMBER_COUNT).await }.boxed()
        })
        .await?;

    let items = read_filtered_items(&rdb_path).await?;

    assert_eq!(items.len(), 1, "expected exactly one ZSetRecord");
    let Item::ZSetRecord {
        key,
        encoding,
        member_count,
        ..
    } = &items[0]
    else {
        panic!("expected ZSetRecord");
    };

    assert_eq!(*key, RDBStr::Str(Bytes::from("zsl_key")));
    assert_eq!(*member_count as usize, MEMBER_COUNT);
    assert_eq!(*encoding, ZSetEncoding::SkipList);
    assert!(
        guard.hit("zset.skiplist"),
        "expected zset.skiplist trace event; captured: {:?}",
        guard.collected()
    );

    Ok(())
}

#[tokio::test]
async fn zset_ziplist_encoding_test() -> AnyResult<()> {
    // Insert <128 members so Redis 2.8 stores ZSET as ziplist (type id 12).
    const MEMBER_COUNT: usize = 50;

    let redis = RedisInstance::new("2.8").await?;

    let rdb_path = redis
        .generate_rdb("zset_ziplist_encoding_test", |conn| {
            async move { seed_zset(conn, "zsl_zl_key", MEMBER_COUNT).await }.boxed()
        })
        .await?;

    let items = read_filtered_items(&rdb_path).await?;

    assert_eq!(items.len(), 1, "expected exactly one ZSetRecord");
    let Item::ZSetRecord {
        key,
        encoding,
        member_count,
        ..
    } = &items[0]
    else {
        panic!("expected ZSetRecord");
    };

    assert_eq!(*key, RDBStr::Str(Bytes::from("zsl_zl_key")));
    assert_eq!(*member_count as usize, MEMBER_COUNT);
    assert_eq!(*encoding, ZSetEncoding::ZipList);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn zset2_skiplist_encoding_test() -> AnyResult<()> {
    // Insert >128 members so modern Redis stores ZSET as ZSet2 skiplist (type id = 5).
    const MEMBER_COUNT: usize = 300;

    let redis = RedisInstance::new("8.0").await?;
    let guard = trace::capture();

    let rdb_path = redis
        .generate_rdb("zset2_skiplist_encoding_test", |conn| {
            async move { seed_zset(conn, "zs2_key", MEMBER_COUNT).await }.boxed()
        })
        .await?;

    let items = read_filtered_items(&rdb_path).await?;

    assert_eq!(items.len(), 1, "expected exactly one ZSet2Record");
    let Item::ZSet2Record {
        key,
        encoding,
        member_count,
        ..
    } = &items[0]
    else {
        panic!("expected ZSet2Record");
    };

    assert_eq!(*key, RDBStr::Str(Bytes::from("zs2_key")));
    assert_eq!(*member_count as usize, MEMBER_COUNT);
    assert_eq!(*encoding, ZSetEncoding::SkipList);
    assert!(
        guard.hit("zset2.skiplist"),
        "expected zset2.skiplist trace event; captured: {:?}",
        guard.collected()
    );

    Ok(())
}

#[tokio::test]
async fn hash_raw_encoding_test() -> AnyResult<()> {
    // Redis 2.8 writes large hashes using raw hash table (type id = 4)
    const FIELD_COUNT: usize = 600; // > hash-max-ziplist-entries (512)

    let redis = RedisInstance::new("2.8").await?;

    let rdb_path = redis
        .generate_rdb("hash_raw_encoding_test", |conn| {
            async move { seed_hash(conn, "hash_raw_key", FIELD_COUNT).await }.boxed()
        })
        .await?;

    let items = read_filtered_items(&rdb_path).await?;

    assert_eq!(items.len(), 1, "expected exactly one HashRecord");
    let Item::HashRecord {
        key,
        encoding,
        field_count,
        ..
    } = &items[0]
    else {
        panic!("expected HashRecord");
    };

    assert_eq!(*key, RDBStr::Str(Bytes::from("hash_raw_key")));
    assert_eq!(*field_count as usize, FIELD_COUNT);
    assert_eq!(*encoding, HashEncoding::Raw);

    Ok(())
}

#[tokio::test]
async fn hash_ziplist_encoding_test() -> AnyResult<()> {
    // Redis 2.8 stores small hashes (≤512 fields) as ziplist (type id = 13)
    const FIELD_COUNT: usize = 50;

    let redis = RedisInstance::new("2.8").await?;

    let rdb_path = redis
        .generate_rdb("hash_ziplist_encoding_test", |conn| {
            async move { seed_hash(conn, "hm_zl_key", FIELD_COUNT).await }.boxed()
        })
        .await?;

    let items = read_filtered_items(&rdb_path).await?;

    assert_eq!(items.len(), 1, "expected exactly one HashRecord");
    let Item::HashRecord {
        key,
        encoding,
        field_count,
        ..
    } = &items[0]
    else {
        panic!("expected HashRecord");
    };

    assert_eq!(*key, RDBStr::Str(Bytes::from("hm_zl_key")));
    assert_eq!(*field_count as usize, FIELD_COUNT);
    assert_eq!(*encoding, HashEncoding::ZipList);

    Ok(())
}

#[tokio::test]
async fn hash_listpack_encoding_test() -> AnyResult<()> {
    const FIELD_COUNT: usize = 40;

    let redis = RedisInstance::new("8.0").await?;

    let rdb_path = redis
        .generate_rdb("hash_listpack_encoding_test", |conn| {
            async move { seed_hash(conn, "hm_lp_key", FIELD_COUNT).await }.boxed()
        })
        .await?;

    let items = read_filtered_items(&rdb_path).await?;

    assert_eq!(items.len(), 1, "expected exactly one HashRecord");
    let Item::HashRecord {
        key,
        encoding,
        field_count,
        ..
    } = &items[0]
    else {
        panic!("expected HashRecord");
    };

    assert_eq!(*key, RDBStr::Str(Bytes::from("hm_lp_key")));
    assert_eq!(*field_count as usize, FIELD_COUNT);
    assert_eq!(*encoding, HashEncoding::ListPack);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn hash_zipmap_fixture_raw_test() -> AnyResult<()> {
    use std::path::Path;

    let guard = trace::capture();
    let path = Path::new("tests/fixtures/zipmap_that_doesnt_compress.rdb");

    let items = read_filtered_items(path).await?;

    // Expect exactly one HashRecord encoded as ZipMap.
    assert_eq!(items.len(), 1, "expected exactly one HashRecord");
    let Item::HashRecord { encoding, .. } = &items[0] else {
        panic!("expected HashRecord");
    };

    assert_eq!(*encoding, HashEncoding::ZipMap);
    assert!(
        guard.hit("hash.zipmap.raw"),
        "expected hash.zipmap.raw trace event; captured: {:?}",
        guard.collected()
    );

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn hash_zipmap_fixture_lzf_test() -> AnyResult<()> {
    use std::path::Path;

    let guard = trace::capture();
    let path = Path::new("tests/fixtures/zipmap_that_compresses_easily.rdb");

    let items = read_filtered_items(path).await?;

    assert_eq!(items.len(), 1, "expected exactly one HashRecord");
    let Item::HashRecord { encoding, .. } = &items[0] else {
        panic!("expected HashRecord");
    };

    assert_eq!(*encoding, HashEncoding::ZipMap);
    assert!(
        guard.hit("hash.zipmap.raw"),
        "expected hash.zipmap.raw trace event; captured: {:?}",
        guard.collected()
    );

    Ok(())
}

#[tokio::test]
async fn module2_encoding_test() -> AnyResult<()> {
    // Use redis-stack image that ships with official modules; encoding will be Module2 (type id = 7).
    let redis = RedisInstance::new_stack("latest").await?;

    let rdb_path = redis
        .generate_rdb("module2_encoding_test", |conn| {
            async move {
                // Ensure a module key exists. RedisBloom is bundled in redis-stack.
                redis::cmd("BF.RESERVE")
                    .arg("bf_key")
                    .arg(0.01)
                    .arg(100)
                    .query_async::<()>(conn)
                    .await?;
                Ok(())
            }
            .boxed()
        })
        .await?;

    let guard = trace::capture();
    let items = read_filtered_items(&rdb_path).await?;

    let modules: Vec<_> = items
        .iter()
        .filter(|it| matches!(it, Item::ModuleRecord { .. }))
        .collect();
    assert!(!modules.is_empty(), "expected at least one ModuleRecord");
    assert!(guard.hit("module2.raw"));

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
        .filter(|item| !matches!(item, Item::ModuleAux { .. }))
        .collect())
}
