use std::path::PathBuf;

use anyhow::Result;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rdbinsight::{
    config::ParquetCompression,
    output::{ChunkWriter, Output, parquet::ParquetOutput},
    source::{RdbSourceConfig, SourceType, standalone::Config as StandaloneConfig},
};
use tempfile::TempDir;
use time::OffsetDateTime;
mod common;

/// Test the Parquet output functionality end-to_end using a live Redis via testcontainers
#[tokio::test]
async fn test_parquet_output_end_to_end() -> Result<()> {
    // Create a temporary directory for output
    let temp_dir = TempDir::new()?;
    let output_dir = temp_dir.path().to_path_buf();

    // Start a Redis instance using testcontainers (no snapshot to speed up and match other tests)
    let redis = crate::common::setup::RedisConfig::default()
        .with_snapshot(false)
        .build()
        .await?;

    // Seed minimal data into Redis
    {
        use redis::Client;
        let client = Client::open(redis.connection_string.as_str())?;
        let mut conn = client.get_multiplexed_tokio_connection().await?;
        let mut pipe = redis::pipe();
        for i in 0..100u32 {
            pipe.set(format!("str_key_{}", i), format!("value_{}", i))
                .ignore();
        }
        pipe.query_async::<()>(&mut conn).await?;
    }

    // Prepare Standalone source to fetch RDB over replication
    fn extract_address(redis_url: &str) -> String {
        redis_url
            .strip_prefix("redis://")
            .unwrap_or(redis_url)
            .to_string()
    }

    let address = extract_address(&redis.connection_string);
    let cfg = StandaloneConfig::new(address.clone(), String::new(), None);

    // Set up batch and output
    let cluster_name = "test-cluster";
    let instance = address.as_str();
    let batch_ts = OffsetDateTime::now_utc();

    let parquet_output = ParquetOutput::new(
        output_dir.clone(),
        ParquetCompression::None,
        100_000,
        ParquetCompression::Lz4,
        cluster_name.to_string(),
        batch_ts,
    );

    // Prepare batch and writer
    parquet_output.prepare_batch().await?;
    let mut writer = parquet_output.create_writer(instance).await?;

    // Get RDB stream and process records
    let mut streams = cfg.get_rdb_streams().await?;
    assert_eq!(streams.len(), 1, "Should have exactly one stream");
    let mut stream = streams.remove(0);
    stream.as_mut().prepare().await?;

    let mut total_records = 0usize;

    use futures_util::StreamExt;
    use rdbinsight::record::RecordStream;

    let mut record_stream = RecordStream::new(stream, SourceType::Standalone);
    while let Some(record_result) = record_stream.next().await {
        let record = record_result?;
        total_records += 1;
        writer.write_record(record).await?;
    }

    writer.finalize_instance().await?;
    Box::new(parquet_output).finalize_batch().await?;

    // Verify the output files exist
    let batch_dir_name = rdbinsight::output::parquet::path::format_batch_dir(batch_ts);
    let final_batch_dir = output_dir
        .join(rdbinsight::output::parquet::path::cluster_dir_name(
            cluster_name,
        ))
        .join(rdbinsight::output::parquet::path::final_batch_dir_name(
            &batch_dir_name,
        ));
    assert!(
        final_batch_dir.exists(),
        "Final batch directory should exist"
    );

    let sanitized_instance = instance.replace(':', "-");
    let instance_file = final_batch_dir.join(format!("{sanitized_instance}.parquet"));
    assert!(instance_file.exists(), "Instance parquet file should exist");

    // Verify the parquet file contains the expected number of records
    let file = std::fs::File::open(&instance_file)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut total_rows = 0usize;
    for batch_result in reader {
        let batch = batch_result?;
        total_rows += batch.num_rows();
    }

    assert!(total_rows > 0, "Should have at least one record");
    assert_eq!(total_rows, total_records, "Record count should match");

    println!(
        "Test passed: processed {total_records} records to {}",
        instance_file.display()
    );

    Ok(())
}

/// Test that Parquet output works with different compression algorithms
#[tokio::test]
async fn test_parquet_compression_algorithms() -> Result<()> {
    let compressions = [
        ParquetCompression::None,
        ParquetCompression::Snappy,
        ParquetCompression::Zstd,
    ];

    for compression in compressions {
        // Create a temporary directory for this compression test
        let temp_dir = TempDir::new()?;
        let output_dir = temp_dir.path().to_path_buf();

        // Use a small test RDB file
        let _rdb_file_path = PathBuf::from("tests/dumps/string_raw_encoding_test_redis_8.0.rdb");

        let cluster_name = "test-cluster";
        let instance = "127.0.0.1:6379";
        let batch_ts = OffsetDateTime::now_utc();

        // Initialize Parquet output with specific compression
        let parquet_output = ParquetOutput::new(
            output_dir.clone(),
            compression,
            100_000,
            ParquetCompression::Lz4,
            cluster_name.to_string(),
            batch_ts,
        );
        parquet_output.prepare_batch().await?;
        let mut writer = parquet_output.create_writer(instance).await?;

        // Create a simple test record
        use bytes::Bytes;
        use rdbinsight::{
            parser::{core::raw::RDBStr, model::StringEncoding},
            record::{Record, RecordEncoding, RecordType},
        };

        let test_record = Record::builder()
            .db(0)
            .key(RDBStr::Str(Bytes::from("test_key")))
            .r#type(RecordType::String)
            .encoding(RecordEncoding::String(StringEncoding::Raw))
            .rdb_size(100)
            .build();

        // Write a single record
        writer.write_record(test_record).await?;

        // Finalize
        writer.finalize_instance().await?;
        Box::new(parquet_output).finalize_batch().await?;

        // Verify file exists
        let batch_dir_name = rdbinsight::output::parquet::path::format_batch_dir(batch_ts);
        let instance_file = output_dir
            .join(rdbinsight::output::parquet::path::cluster_dir_name(
                cluster_name,
            ))
            .join(rdbinsight::output::parquet::path::final_batch_dir_name(
                &batch_dir_name,
            ))
            .join("127.0.0.1-6379.parquet");

        assert!(
            instance_file.exists(),
            "Instance file should exist for compression: {compression:?}"
        );

        println!("Compression test passed for: {compression:?}");
    }

    Ok(())
}

/// Test multiple instances writing to the same batch
#[tokio::test]
async fn test_multiple_instances_parquet() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let output_dir = temp_dir.path().to_path_buf();

    let cluster_name = "test-cluster";
    let batch_ts = OffsetDateTime::now_utc();

    let parquet_output = ParquetOutput::new(
        output_dir.clone(),
        ParquetCompression::None,
        100_000,
        ParquetCompression::Lz4,
        cluster_name.to_string(),
        batch_ts,
    );

    parquet_output.prepare_batch().await?;

    // Create test records for different instances
    use bytes::Bytes;
    use rdbinsight::{
        parser::{core::raw::RDBStr, model::StringEncoding},
        record::{Record, RecordEncoding, RecordType},
    };

    let instances = ["127.0.0.1:6379", "127.0.0.1:6380", "127.0.0.1:6381"];

    for (i, instance) in instances.iter().enumerate() {
        let test_record = Record::builder()
            .db(i as u64)
            .key(RDBStr::Str(Bytes::from(format!("key_{}", i))))
            .r#type(RecordType::String)
            .encoding(RecordEncoding::String(StringEncoding::Raw))
            .rdb_size(100)
            .build();

        let mut writer = parquet_output.create_writer(instance).await?;
        writer.write_record(test_record).await?;
        writer.finalize_instance().await?;
    }

    Box::new(parquet_output).finalize_batch().await?;

    // Verify all instance files exist
    let batch_dir_name = rdbinsight::output::parquet::path::format_batch_dir(batch_ts);
    let final_batch_dir = output_dir
        .join(rdbinsight::output::parquet::path::cluster_dir_name(
            cluster_name,
        ))
        .join(rdbinsight::output::parquet::path::final_batch_dir_name(
            &batch_dir_name,
        ));

    for instance in instances {
        let sanitized_instance = instance.replace(':', "-");
        let instance_file = final_batch_dir.join(format!("{sanitized_instance}.parquet"));
        assert!(
            instance_file.exists(),
            "Instance file should exist for: {instance}"
        );
    }

    println!(
        "Multiple instances test passed for {} instances",
        instances.len()
    );

    Ok(())
}
