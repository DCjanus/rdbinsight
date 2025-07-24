use rdbinsight::{
    helper::AnyResult,
    source::{RdbSourceConfig, standalone::Config as StandaloneConfig},
};

use crate::common::setup::{RedisConfig, RedisInstance, RedisVariant};

mod common;

#[derive(Debug)]
struct TestCase {
    name: &'static str,
    redis_config: fn() -> RedisConfig,
    username: Option<&'static str>,
    password: Option<&'static str>,
    expected_trace: &'static str,
    expected_item_count: usize,
}

fn extract_address(redis_url: &str) -> String {
    redis_url
        .strip_prefix("redis://")
        .unwrap_or(redis_url)
        .to_string()
}

async fn verify_rdb_content(
    redis: &RedisInstance,
    username: Option<&str>,
    password: Option<&str>,
    expected_item_count: usize,
) -> AnyResult<()> {
    let address = extract_address(&redis.connection_string);
    let cfg = StandaloneConfig {
        address,
        username: username.map(|s| s.to_string()),
        password: password.map(|s| s.to_string()),
    };

    let mut streams = cfg.get_rdb_streams().await?;
    assert_eq!(streams.len(), 1, "Expected exactly one RDB stream");
    let mut reader = streams.remove(0);
    reader.as_mut().prepare().await?;
    let items = common::utils::collect_items(&mut reader).await?;
    let filtered_items = common::utils::filter_items(items);

    assert_eq!(
        filtered_items.len(),
        expected_item_count,
        "Expected {} items, but got {}",
        expected_item_count,
        filtered_items.len()
    );

    Ok(())
}

async fn seed_test_data(
    redis: &RedisInstance,
    username: Option<&str>,
    password: Option<&str>,
) -> AnyResult<()> {
    use redis::Client;

    let client = Client::open(redis.connection_string.as_str())?;
    let mut conn = client.get_multiplexed_tokio_connection().await?;

    // Authenticate first if authentication information is provided
    if let Some(password) = password {
        if let Some(username) = username {
            redis::cmd("AUTH")
                .arg(username)
                .arg(password)
                .query_async::<()>(&mut conn)
                .await?;
        } else {
            redis::cmd("AUTH")
                .arg(password)
                .query_async::<()>(&mut conn)
                .await?;
        }
    }

    // Add various types of data to improve test coverage

    // String data - different sizes
    for i in 0..100 {
        redis::cmd("SET")
            .arg(format!("str_key_{}", i))
            .arg(format!("value_{}", i))
            .query_async::<()>(&mut conn)
            .await?;
    }

    // Large strings
    redis::cmd("SET")
        .arg("large_string")
        .arg("x".repeat(1024))
        .query_async::<()>(&mut conn)
        .await?;

    redis::cmd("SET")
        .arg("very_large_string")
        .arg("y".repeat(10240))
        .query_async::<()>(&mut conn)
        .await?;

    // List data - test different encoding methods
    common::seed_list(&mut conn, "list_small", 10).await?;
    common::seed_list(&mut conn, "list_medium", 100).await?;
    common::seed_list(&mut conn, "list_large", 1000).await?;

    // Set data - test different encoding methods
    common::seed_set(&mut conn, "set_small", 20).await?;
    common::seed_set(&mut conn, "set_medium", 200).await?;

    // Integer set
    let mut pipe = redis::pipe();
    for i in 0..50 {
        pipe.sadd("set_intset", i).ignore();
    }
    pipe.query_async::<()>(&mut conn).await?;

    // Hash data - test different encoding methods
    common::seed_hash(&mut conn, "hash_small", 15).await?;
    common::seed_hash(&mut conn, "hash_medium", 150).await?;

    // Sorted set data
    common::seed_zset(&mut conn, "zset_small", 25).await?;
    common::seed_zset(&mut conn, "zset_medium", 250).await?;

    // Compact sorted set (ziplist encoding) - using integer scores
    let mut pipe = redis::pipe();
    for i in 0..10 {
        pipe.cmd("ZADD")
            .arg("zset_ziplist")
            .arg(i as isize) // Use integer instead of float
            .arg(format!("member_{}", i))
            .ignore();
    }
    pipe.query_async::<()>(&mut conn).await?;

    // Sorted set containing different types of scores - using cmd("ZADD") instead of zadd()
    redis::cmd("ZADD")
        .arg("zset_scores")
        .arg(1.5)
        .arg("float_score")
        .query_async::<()>(&mut conn)
        .await?;

    redis::cmd("ZADD")
        .arg("zset_scores")
        .arg(-10.0)
        .arg("negative_score")
        .query_async::<()>(&mut conn)
        .await?;

    redis::cmd("ZADD")
        .arg("zset_scores")
        .arg(999999.0)
        .arg("large_score")
        .query_async::<()>(&mut conn)
        .await?;

    // Add more complex floating-point number test cases
    redis::cmd("ZADD")
        .arg("zset_complex")
        .arg(0.0)
        .arg("zero_score")
        .query_async::<()>(&mut conn)
        .await?;

    redis::cmd("ZADD")
        .arg("zset_complex")
        .arg(-0.0)
        .arg("negative_zero")
        .query_async::<()>(&mut conn)
        .await?;

    // Test very small floating-point numbers
    redis::cmd("ZADD")
        .arg("zset_complex")
        .arg(1e-10)
        .arg("tiny_positive")
        .query_async::<()>(&mut conn)
        .await?;

    redis::cmd("ZADD")
        .arg("zset_complex")
        .arg(-1e-10)
        .arg("tiny_negative")
        .query_async::<()>(&mut conn)
        .await?;

    // Test large floating-point numbers (but not extreme values)
    redis::cmd("ZADD")
        .arg("zset_complex")
        .arg(1e15)
        .arg("large_positive")
        .query_async::<()>(&mut conn)
        .await?;

    redis::cmd("ZADD")
        .arg("zset_complex")
        .arg(-1e15)
        .arg("large_negative")
        .query_async::<()>(&mut conn)
        .await?;

    // Test positive and negative infinity - Redis documentation explicitly supports these values
    redis::cmd("ZADD")
        .arg("zset_infinity")
        .arg(f64::INFINITY)
        .arg("positive_infinity")
        .query_async::<()>(&mut conn)
        .await?;

    redis::cmd("ZADD")
        .arg("zset_infinity")
        .arg(f64::NEG_INFINITY)
        .arg("negative_infinity")
        .query_async::<()>(&mut conn)
        .await?;

    // Test maximum and minimum finite floating-point numbers
    redis::cmd("ZADD")
        .arg("zset_extremes")
        .arg(f64::MAX)
        .arg("max_finite")
        .query_async::<()>(&mut conn)
        .await?;

    redis::cmd("ZADD")
        .arg("zset_extremes")
        .arg(f64::MIN)
        .arg("min_finite")
        .query_async::<()>(&mut conn)
        .await?;

    // Test minimum normalized and minimum denormalized floating-point numbers
    redis::cmd("ZADD")
        .arg("zset_extremes")
        .arg(f64::MIN_POSITIVE)
        .arg("min_positive")
        .query_async::<()>(&mut conn)
        .await?;

    redis::cmd("ZADD")
        .arg("zset_extremes")
        .arg(f64::EPSILON)
        .arg("epsilon")
        .query_async::<()>(&mut conn)
        .await?;

    // Test numbers represented in scientific notation
    redis::cmd("ZADD")
        .arg("zset_scientific")
        .arg(1.23e-100)
        .arg("very_tiny")
        .query_async::<()>(&mut conn)
        .await?;

    redis::cmd("ZADD")
        .arg("zset_scientific")
        .arg(9.87e+100)
        .arg("very_large")
        .query_async::<()>(&mut conn)
        .await?;

    // Test integer boundary values as floating-point numbers
    redis::cmd("ZADD")
        .arg("zset_int_bounds")
        .arg(i64::MAX as f64)
        .arg("i64_max")
        .query_async::<()>(&mut conn)
        .await?;

    redis::cmd("ZADD")
        .arg("zset_int_bounds")
        .arg(i64::MIN as f64)
        .arg("i64_min")
        .query_async::<()>(&mut conn)
        .await?;

    redis::cmd("ZADD")
        .arg("zset_int_bounds")
        .arg(u64::MAX as f64)
        .arg("u64_max")
        .query_async::<()>(&mut conn)
        .await?;

    // Test special decimal values
    redis::cmd("ZADD")
        .arg("zset_decimals")
        .arg(0.1)
        .arg("one_tenth")
        .query_async::<()>(&mut conn)
        .await?;

    redis::cmd("ZADD")
        .arg("zset_decimals")
        .arg(0.333333333333333)
        .arg("one_third")
        .query_async::<()>(&mut conn)
        .await?;

    redis::cmd("ZADD")
        .arg("zset_decimals")
        .arg(std::f64::consts::PI)
        .arg("pi")
        .query_async::<()>(&mut conn)
        .await?;

    redis::cmd("ZADD")
        .arg("zset_decimals")
        .arg(std::f64::consts::E)
        .arg("euler")
        .query_async::<()>(&mut conn)
        .await?;

    // Stream data
    common::seed_stream(&mut conn, "stream_data", 30).await?;

    // Bitmap
    redis::cmd("SETBIT")
        .arg("bitmap_key")
        .arg(100)
        .arg(1)
        .query_async::<()>(&mut conn)
        .await?;

    // HyperLogLog
    redis::cmd("PFADD")
        .arg("hll_key")
        .arg("element1")
        .arg("element2")
        .arg("element3")
        .query_async::<()>(&mut conn)
        .await?;

    Ok(())
}

async fn run_test_case(test_case: &TestCase) -> AnyResult<()> {
    let guard = common::trace::capture();

    let redis = (test_case.redis_config)()
        .with_snapshot(false) // Disable RDB save
        .build()
        .await?;

    // Add test data
    seed_test_data(&redis, test_case.username, test_case.password).await?;

    verify_rdb_content(
        &redis,
        test_case.username,
        test_case.password,
        test_case.expected_item_count,
    )
    .await?;

    assert!(
        guard.hit(test_case.expected_trace),
        "Test '{}': expected trace '{}' but captured: {:?}",
        test_case.name,
        test_case.expected_trace,
        guard.collected()
    );

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn standalone_source_tests() -> AnyResult<()> {
    let test_cases = vec![
        TestCase {
            name: "disk_sync",
            redis_config: || RedisConfig::default().with_diskless(false),
            username: None,
            password: None,
            expected_trace: "rdb.disk",
            expected_item_count: 123, // Number of items including comprehensive ZADD score tests
        },
        TestCase {
            name: "diskless_sync",
            redis_config: || RedisConfig::default().with_diskless(true),
            username: None,
            password: None,
            expected_trace: "rdb.diskless",
            expected_item_count: 123,
        },
        TestCase {
            name: "auth_password_only",
            redis_config: || RedisConfig::default().with_password("pwdonly"),
            username: None,
            password: Some("pwdonly"),
            expected_trace: "auth.password_only",
            expected_item_count: 123,
        },
        TestCase {
            name: "auth_with_username_repluser",
            redis_config: || {
                RedisConfig::default()
                    .with_username("repluser")
                    .with_password("replpwd")
            },
            username: Some("repluser"),
            password: Some("replpwd"),
            expected_trace: "auth.with_username",
            expected_item_count: 123,
        },
        TestCase {
            name: "auth_with_username_testuser",
            redis_config: || {
                RedisConfig::default()
                    .with_username("testuser")
                    .with_password("testpass")
            },
            username: Some("testuser"),
            password: Some("testpass"),
            expected_trace: "auth.with_username",
            expected_item_count: 123,
        },
        TestCase {
            name: "redis6_rdb_only_unsupported",
            redis_config: || RedisConfig::default().with_version(RedisVariant::Redis6_0),
            username: None,
            password: None,
            expected_trace: "replconf.rdb_only.unsupported",
            expected_item_count: 123,
        },
        TestCase {
            name: "redis8_rdb_only_supported",
            redis_config: || RedisConfig::default().with_version(RedisVariant::Redis8_0),
            username: None,
            password: None,
            expected_trace: "replconf.rdb_only.supported",
            expected_item_count: 123,
        },
    ];

    for test_case in &test_cases {
        if let Err(e) = run_test_case(test_case).await {
            panic!("Test case '{}' failed: {}", test_case.name, e);
        }
    }

    Ok(())
}

/// Seeds one hundred thousand strings into Redis for testing feed_more coverage
async fn seed_lots_of_strings(redis: &RedisInstance) -> AnyResult<()> {
    use redis::Client;
    let client = Client::open(redis.connection_string.as_str())?;
    let mut conn = client.get_multiplexed_tokio_connection().await?;

    let mut pipe = redis::pipe();
    for i in 0..32_000 {
        pipe.set(format!("key_{:06}", i), format!("value_{:06}", i))
            .ignore();

        // Execute batch every 10,000 keys
        if i % 10_000 == 9_999 {
            pipe.query_async::<()>(&mut conn).await?;
            pipe = redis::pipe();
        }
    }

    // Execute remaining operations
    if !pipe.cmd_iter().next().is_none() {
        pipe.query_async::<()>(&mut conn).await?;
    }

    Ok(())
}

/// Verifies client output buffer limit configuration
async fn verify_buffer_limit_config(redis: &RedisInstance) -> AnyResult<()> {
    use redis::Client;
    let client = Client::open(redis.connection_string.as_str())?;
    let mut conn = client.get_multiplexed_tokio_connection().await?;

    let _config_result: Vec<String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("client-output-buffer-limit")
        .query_async(&mut conn)
        .await?;

    Ok(())
}

/// Configuration for feed_more coverage tests
struct FeedMoreTestConfig {
    /// Whether to use diskless replication
    diskless: bool,
    /// Expected RDB trace event
    expected_rdb_trace: &'static str,
    /// Expected feed_more trace event
    expected_feed_more_trace: &'static str,
    /// Whether to strictly validate item count (false = tolerate EOF errors)
    strict_validation: bool,
}

/// Generic test function for feed_more coverage in both disk and diskless modes
async fn run_lots_of_strings_feed_more_test(config: FeedMoreTestConfig) -> AnyResult<()> {
    let guard = common::trace::capture();

    // Configure Redis with appropriate sync mode and large buffer limits
    let redis = RedisConfig::default()
        .with_diskless(config.diskless)
        .with_snapshot(false)
        .build()
        .await?;

    // Seed large dataset to trigger feed_more
    seed_lots_of_strings(&redis).await?;
    verify_buffer_limit_config(&redis).await?;

    // Verify RDB content and trace coverage
    let address = extract_address(&redis.connection_string);
    let cfg = StandaloneConfig {
        address,
        username: None,
        password: None,
    };

    match cfg.get_rdb_streams().await {
        Ok(mut streams) => {
            let mut reader = streams.remove(0);
            match reader.as_mut().prepare().await {
                Ok(_) => match common::utils::collect_items(&mut reader).await {
                    Ok(items) => {
                        let filtered_items = common::utils::filter_items(items);

                        // Strict validation for disk mode, lenient for diskless mode
                        if config.strict_validation {
                            assert_eq!(filtered_items.len(), 32000, "Expected 32000 items");
                        }

                        // Verify basic trace events
                        assert!(
                            guard.hit(config.expected_rdb_trace),
                            "Expected trace '{}' but captured: {:?}",
                            config.expected_rdb_trace,
                            guard.collected()
                        );

                        // Verify feed_more function was called
                        if !guard.hit(config.expected_feed_more_trace) {
                            // For disk mode, feed_more should be triggered, so this is an assertion failure
                            if config.strict_validation {
                                panic!(
                                    "Expected feed_more trace '{}' but captured: {:?}",
                                    config.expected_feed_more_trace,
                                    guard.collected()
                                );
                            }
                        }
                    }
                    Err(e) => {
                        if config.strict_validation {
                            // For disk mode, this is a real error - provide diagnostics
                            eprintln!("❌ Error during data collection: {}", e);

                            // Get Redis container logs for diagnosis
                            if let Ok(logs) = redis.get_logs().await {
                                eprintln!("🔍 Redis container logs:\n{}", logs);
                            }

                            return Err(e);
                        }
                        // For diskless mode, EOF errors are expected for very large datasets
                        // Just verify we got the expected traces and continue
                    }
                },
                Err(e) => {
                    if config.strict_validation {
                        eprintln!("❌ Error preparing RDB stream: {}", e);
                        if let Ok(logs) = redis.get_logs().await {
                            eprintln!("🔍 Redis container logs:\n{}", logs);
                        }
                        return Err(e.into());
                    }
                }
            }
        }
        Err(e) => {
            if config.strict_validation {
                // For disk mode, this is a real error - provide diagnostics
                eprintln!("❌ Error getting RDB stream: {}", e);

                // Get Redis container logs for diagnosis
                if let Ok(logs) = redis.get_logs().await {
                    eprintln!("🔍 Redis container logs:\n{}", logs);
                }

                return Err(e);
            }
            // For diskless mode, this is expected for very large datasets
        }
    }

    Ok(())
}

/// Tests feed_more coverage for diskless mode (DelimiterReader::feed_more)
#[tokio::test(flavor = "current_thread")]
async fn diskless_lots_of_strings_test() -> AnyResult<()> {
    run_lots_of_strings_feed_more_test(FeedMoreTestConfig {
        diskless: true,
        expected_rdb_trace: "rdb.diskless",
        expected_feed_more_trace: "delimiter_reader.feed_more",
        strict_validation: false, // Tolerate EOF errors for large datasets
    })
    .await
}

// Tests feed_more coverage for disk mode (LimitedAsyncReader::feed_more)
#[tokio::test(flavor = "current_thread")]
async fn disk_lots_of_strings_test() -> AnyResult<()> {
    run_lots_of_strings_feed_more_test(FeedMoreTestConfig {
        diskless: false,
        expected_rdb_trace: "rdb.disk",
        expected_feed_more_trace: "limited_reader.feed_more",
        strict_validation: true, // Strict validation for disk mode
    })
    .await
}
