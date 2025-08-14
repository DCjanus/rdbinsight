use std::path::PathBuf;

use anyhow::Result;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rdbinsight::{
    config::{DumpConfig, OutputConfig, ParquetCompression, ParquetConfig, SourceConfig},
    output::{clickhouse::BatchInfo, parquet::ParquetOutput},
    source::RdbSourceConfig,
};
use tempfile::TempDir;
use time::OffsetDateTime;

mod common;

/// Test the Parquet output functionality end-to-end using a small RDB file
#[tokio::test]
async fn test_parquet_output_end_to_end() -> Result<()> {
    // Create a temporary directory for output
    let temp_dir = TempDir::new()?;
    let output_dir = temp_dir.path().to_path_buf();

    // Use a small test RDB file
    let rdb_file_path = PathBuf::from("tests/dumps/string_raw_encoding_test_redis_8.0.rdb");
    assert!(rdb_file_path.exists(), "Test RDB file should exist");

    // Set up configuration
    let cluster_name = "test-cluster";
    let instance = "127.0.0.1:6379";
    let batch_timestamp = OffsetDateTime::now_utc();

    let source_config = SourceConfig::RDBFile {
        cluster_name: cluster_name.to_string(),
        path: rdb_file_path.to_string_lossy().to_string(),
        instance: instance.to_string(),
    };

    let parquet_config = ParquetConfig::new(output_dir.clone(), ParquetCompression::None)?;
    let output_config = OutputConfig::Parquet(parquet_config);

    let config = DumpConfig {
        source: source_config,
        output: output_config,
        concurrency: 1,
    };

    // Validate the configuration
    config.validate()?;

    // Create batch info
    let batch_info = BatchInfo {
        cluster: cluster_name.to_string(),
        batch: batch_timestamp,
    };

    // Get RDB streams from source
    let streams = config.source.get_rdb_streams().await?;
    assert_eq!(streams.len(), 1, "Should have exactly one stream");

    // Initialize Parquet output
    let mut parquet_output = match &config.output {
        OutputConfig::Parquet(parquet_config) => {
            ParquetOutput::new(
                parquet_config.dir.clone(),
                parquet_config.compression,
                cluster_name,
                &batch_info,
            )
            .await?
        }
        _ => unreachable!("Should be Parquet config"),
    };

    // Process the stream
    let mut stream = streams.into_iter().next().unwrap();
    stream.as_mut().prepare().await?;

    // Collect records manually to avoid complex stream processing
    let mut total_records = 0;
    let mut record_buffer = Vec::new();
    const BATCH_SIZE: usize = 100;

    use futures_util::StreamExt;
    use rdbinsight::record::RecordStream;

    let mut record_stream = RecordStream::new(stream, rdbinsight::source::SourceType::File);

    while let Some(record_result) = record_stream.next().await {
        let record = record_result?;
        total_records += 1;
        record_buffer.push(record);

        // Write in batches
        if record_buffer.len() >= BATCH_SIZE {
            parquet_output
                .write(&record_buffer, &batch_info, instance)
                .await?;
            record_buffer.clear();
        }
    }

    // Write remaining records
    if !record_buffer.is_empty() {
        parquet_output
            .write(&record_buffer, &batch_info, instance)
            .await?;
    }

    // Finalize the instance
    parquet_output.finalize_instance(instance).await?;

    // Finalize the batch
    parquet_output.finalize_batch().await?;

    // Verify the output files exist
    let batch_dir_name = rdbinsight::output::parquet::path::format_batch_dir(batch_info.batch);
    let final_batch_dir = output_dir.join(cluster_name).join(batch_dir_name);

    assert!(
        final_batch_dir.exists(),
        "Final batch directory should exist"
    );

    let instance_file = final_batch_dir.join("127.0.0.1-6379.parquet");
    assert!(instance_file.exists(), "Instance parquet file should exist");

    // Verify the parquet file contains the expected number of records
    let file = std::fs::File::open(&instance_file)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut total_rows = 0;
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
        let batch_timestamp = OffsetDateTime::now_utc();

        let batch_info = BatchInfo {
            cluster: cluster_name.to_string(),
            batch: batch_timestamp,
        };

        // Initialize Parquet output with specific compression
        let mut parquet_output =
            ParquetOutput::new(output_dir.clone(), compression, cluster_name, &batch_info).await?;

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
        parquet_output
            .write(&[test_record], &batch_info, instance)
            .await?;

        // Finalize
        parquet_output.finalize_instance(instance).await?;
        parquet_output.finalize_batch().await?;

        // Verify file exists
        let batch_dir_name = rdbinsight::output::parquet::path::format_batch_dir(batch_info.batch);
        let final_batch_dir = output_dir.join(cluster_name).join(batch_dir_name);
        let instance_file = final_batch_dir.join("127.0.0.1-6379.parquet");

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
    let batch_timestamp = OffsetDateTime::now_utc();

    let batch_info = BatchInfo {
        cluster: cluster_name.to_string(),
        batch: batch_timestamp,
    };

    let mut parquet_output = ParquetOutput::new(
        output_dir.clone(),
        ParquetCompression::None,
        cluster_name,
        &batch_info,
    )
    .await?;

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
            .key(RDBStr::Str(Bytes::from(format!("key_{i}"))))
            .r#type(RecordType::String)
            .encoding(RecordEncoding::String(StringEncoding::Raw))
            .rdb_size(100)
            .build();

        parquet_output
            .write(&[test_record], &batch_info, instance)
            .await?;

        parquet_output.finalize_instance(instance).await?;
    }

    parquet_output.finalize_batch().await?;

    // Verify all instance files exist
    let batch_dir_name = rdbinsight::output::parquet::path::format_batch_dir(batch_info.batch);
    let final_batch_dir = output_dir.join(cluster_name).join(batch_dir_name);

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
