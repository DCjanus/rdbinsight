use std::{path::PathBuf, time::Instant};

use anyhow::{Context, Result};
use backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
use clap::{Parser, Subcommand};
use futures_util::StreamExt;
use rdbinsight::{
    config::{Config, OutputConfig, SourceConfig},
    output::clickhouse::{BatchInfo, ClickHouseOutput},
    record::{Record, RecordStream},
    source::{RdbSourceConfig, file::Config as FileConfig, standalone::Config as StandaloneConfig},
};
use time::OffsetDateTime;
use tracing::{debug, error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,

    /// Enable verbose logging
    #[clap(short, long, global = true)]
    verbose: bool,
}

#[derive(Subcommand)]
enum Command {
    /// Dump Redis data to ClickHouse
    Dump(DumpArgs),
    /// Miscellaneous utilities
    #[command(subcommand)]
    Misc(MiscCommand),
}

#[derive(Parser)]
struct DumpArgs {
    /// Path to configuration file
    config: PathBuf,
}

#[derive(Subcommand)]
enum MiscCommand {
    /// Print recommended ClickHouse schema DDL statements
    #[command(name = "print-clickhouse-schema")]
    PrintClickhouseSchema,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let level = if cli.verbose {
        tracing::level_filters::LevelFilter::DEBUG
    } else {
        tracing::level_filters::LevelFilter::INFO
    };
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .with(level)
        .init();

    match cli.command {
        Command::Dump(args) => dump_to_clickhouse(args.config).await,
        Command::Misc(misc_cmd) => match misc_cmd {
            MiscCommand::PrintClickhouseSchema => {
                print_clickhouse_schema();
                Ok(())
            }
        },
    }
}

fn print_clickhouse_schema() {
    let sql_files = [
        (
            "01_create_redis_records_raw.sql",
            include_str!("../../sql/01_create_redis_records_raw.sql"),
        ),
        (
            "02_create_import_batches_completed.sql",
            include_str!("../../sql/02_create_import_batches_completed.sql"),
        ),
        (
            "03_create_redis_records_view.sql",
            include_str!("../../sql/03_create_redis_records_view.sql"),
        ),
    ];

    for (filename, content) in sql_files {
        println!("-- File: {}", filename);
        println!("{}", content);
        println!();
    }
}

async fn dump_to_clickhouse(config_path: PathBuf) -> Result<()> {
    info!("Loading configuration from: {}", config_path.display());

    let config_content = tokio::fs::read_to_string(&config_path)
        .await
        .with_context(|| format!("Failed to read config file: {}", config_path.display()))?;

    let config: Config =
        toml::from_str(&config_content).with_context(|| "Failed to parse configuration file")?;

    info!("Configuration loaded successfully");

    let cluster_name = get_cluster_name(&config.source);
    info!("Cluster: {}", cluster_name);
    info!("Concurrency: {}", config.concurrency);

    // Generate unique batch timestamp
    let batch_timestamp = OffsetDateTime::now_utc();
    info!("Batch timestamp: {}", batch_timestamp);

    // Initialize ClickHouse output
    let clickhouse_output = match &config.output {
        OutputConfig::Clickhouse(clickhouse_config) => {
            info!(
                "Initializing ClickHouse connection to: {}",
                clickhouse_config.address
            );
            ClickHouseOutput::new(clickhouse_config.clone())
                .await
                .with_context(|| "Failed to initialize ClickHouse output")?
        }
    };

    // Get instance identifier based on source type
    let instance = get_instance_identifier(&config.source);
    info!("Instance: {}", instance);

    let batch_info = BatchInfo {
        cluster: cluster_name.to_string(),
        batch: batch_timestamp,
        instance,
    };

    // Get RDB stream based on source type
    let stream = match &config.source {
        SourceConfig::RDBFile { path, .. } => {
            info!("Reading from RDB file: {}", path);
            let file_config = FileConfig {
                path: PathBuf::from(path),
            };
            file_config
                .get_rdb_stream()
                .await
                .with_context(|| format!("Failed to open RDB file: {path}"))?
        }
        SourceConfig::RedisStandalone {
            address,
            username,
            password,
            ..
        } => {
            info!("Connecting to Redis standalone: {}", address);
            let standalone_config = StandaloneConfig {
                address: address.clone(),
                username: username.clone(),
                password: password.clone(),
            };
            standalone_config
                .get_rdb_stream()
                .await
                .with_context(|| format!("Failed to connect to Redis: {address}"))?
        }
    };

    // Process records and write to ClickHouse
    process_records_to_clickhouse(stream, clickhouse_output, batch_info).await
}

async fn process_records_to_clickhouse(
    stream: std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>>,
    clickhouse_output: ClickHouseOutput,
    batch_info: BatchInfo,
) -> Result<()> {
    const BATCH_SIZE: usize = 1_000_000;

    let start_time = Instant::now();
    let mut total_records = 0u64;
    let mut record_buffer: Vec<Record> = Vec::with_capacity(BATCH_SIZE);

    info!("Starting RDB parsing and ClickHouse writing...");

    // Create RecordStream
    let mut record_stream = RecordStream::new(stream);

    // Configure retry strategy: linear backoff up to 1 minute timeout
    let backoff_strategy = ExponentialBackoffBuilder::new()
        .with_initial_interval(std::time::Duration::from_secs(1))
        .with_max_interval(std::time::Duration::from_secs(10))
        .with_max_elapsed_time(Some(std::time::Duration::from_secs(60)))
        .build();

    loop {
        match record_stream.next().await {
            Some(Ok(record)) => {
                total_records += 1;
                record_buffer.push(record);

                // Write batch when buffer is full
                if record_buffer.len() >= BATCH_SIZE {
                    write_batch_with_retry(
                        &clickhouse_output,
                        &record_buffer,
                        &batch_info,
                        &backoff_strategy,
                    )
                    .await?;
                    info!(
                        "Wrote batch of {} records (total: {})",
                        record_buffer.len(),
                        total_records
                    );
                    record_buffer.clear();
                }

                // Log progress every 100,000 records
                if total_records % 100_000 == 0 {
                    debug!("Processed {} records", total_records);
                }
            }
            Some(Err(e)) => {
                error!("Parser error: {}", e);
                return Err(e);
            }
            None => {
                // End of stream - write any remaining records
                if !record_buffer.is_empty() {
                    write_batch_with_retry(
                        &clickhouse_output,
                        &record_buffer,
                        &batch_info,
                        &backoff_strategy,
                    )
                    .await?;
                    info!("Wrote final batch of {} records", record_buffer.len());
                }
                break;
            }
        }
    }

    // Commit the batch to mark it as complete
    info!("Committing batch to mark import as complete...");
    commit_batch_with_retry(&clickhouse_output, &batch_info, &backoff_strategy).await?;

    let duration = start_time.elapsed();
    info!(
        "Successfully processed {} records in {:.2} seconds ({:.2} records/sec)",
        total_records,
        duration.as_secs_f64(),
        total_records as f64 / duration.as_secs_f64()
    );

    Ok(())
}

async fn write_batch_with_retry(
    clickhouse_output: &ClickHouseOutput,
    records: &[Record],
    batch_info: &BatchInfo,
    backoff_strategy: &ExponentialBackoff,
) -> Result<()> {
    let operation = || async {
        clickhouse_output
            .write(records, batch_info)
            .await
            .map_err(|e| {
                warn!("Failed to write batch to ClickHouse: {}", e);
                backoff::Error::transient(e)
            })
    };

    backoff::future::retry(backoff_strategy.clone(), operation)
        .await
        .with_context(|| "Failed to write batch to ClickHouse after retries")
}

async fn commit_batch_with_retry(
    clickhouse_output: &ClickHouseOutput,
    batch_info: &BatchInfo,
    backoff_strategy: &ExponentialBackoff,
) -> Result<()> {
    let operation = || async {
        clickhouse_output
            .commit_batch(batch_info)
            .await
            .map_err(|e| {
                warn!("Failed to commit batch to ClickHouse: {}", e);
                backoff::Error::transient(e)
            })
    };

    backoff::future::retry(backoff_strategy.clone(), operation)
        .await
        .with_context(|| "Failed to commit batch to ClickHouse after retries")
}

fn get_cluster_name(source: &SourceConfig) -> &str {
    match source {
        SourceConfig::RedisStandalone { cluster_name, .. } => cluster_name,
        SourceConfig::RDBFile { cluster_name, .. } => cluster_name,
    }
}

fn get_instance_identifier(source: &SourceConfig) -> String {
    match source {
        SourceConfig::RedisStandalone { address, .. } => address.clone(),
        SourceConfig::RDBFile { instance, .. } => instance.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_instance_identifier() {
        let standalone_config = SourceConfig::RedisStandalone {
            cluster_name: "test".to_string(),
            batch_id: None,
            address: "127.0.0.1:6379".to_string(),
            username: None,
            password: None,
        };
        assert_eq!(
            get_instance_identifier(&standalone_config),
            "127.0.0.1:6379"
        );

        let rdb_config = SourceConfig::RDBFile {
            cluster_name: "test".to_string(),
            batch_id: None,
            path: "/path/to/file.rdb".to_string(),
            instance: "192.168.1.100:6379".to_string(),
        };
        assert_eq!(get_instance_identifier(&rdb_config), "192.168.1.100:6379");
    }
}
