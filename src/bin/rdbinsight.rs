use std::{path::PathBuf, pin::Pin, time::Instant};

use anyhow::{Context, Result, anyhow};
use backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
use clap::{Parser, Subcommand};
use futures_util::{StreamExt, TryStreamExt};
use rdbinsight::{
    config::{Config, OutputConfig},
    output::clickhouse::{BatchInfo, ClickHouseOutput},
    record::{Record, RecordStream},
    source::{RDBStream, RdbSourceConfig},
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
    /// Generate interactive HTML report
    Report(ReportArgs),
    /// Miscellaneous utilities
    #[command(subcommand)]
    Misc(MiscCommand),
}

#[derive(Parser)]
struct DumpArgs {
    /// Path to configuration file
    config: PathBuf,

    /// Batch timestamp in RFC3339 format (defaults to current UTC time)
    #[clap(long, env = "RDBINSIGHT_BATCH_TIMESTAMP")]
    batch_timestamp: Option<String>,
}

#[derive(Parser)]
struct ReportArgs {
    /// Path to configuration file
    #[clap(short, long)]
    config: PathBuf,

    /// Cluster name
    #[clap(long)]
    cluster: String,

    /// Batch timestamp in RFC3339 format (defaults to latest batch)
    #[clap(long)]
    batch: Option<String>,

    /// Output HTML file path
    #[clap(short, long)]
    output: Option<PathBuf>,
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
        Command::Dump(args) => dump_to_clickhouse(args).await,
        Command::Report(args) => run_report(args).await,
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
        println!("-- File: {filename}");
        println!("{content}");
        println!();
    }
}

async fn dump_to_clickhouse(args: DumpArgs) -> Result<()> {
    debug!(
        operation = "config_loading",
        config_path = %args.config.display(),
        "Loading configuration"
    );

    let config_content = tokio::fs::read_to_string(&args.config)
        .await
        .with_context(|| {
            format!(
                "Failed to read config file: {config_path}",
                config_path = args.config.display()
            )
        })?;

    let mut config: Config =
        toml::from_str(&config_content).with_context(|| "Failed to parse configuration file")?;

    debug!(
        operation = "config_parsed",
        "Configuration parsed successfully"
    );

    // Preprocess the source configuration to handle dynamic values
    config
        .source
        .preprocess()
        .await
        .with_context(|| "Failed to preprocess source configuration")?;

    debug!(
        operation = "source_preprocessed",
        "Source configuration preprocessed successfully"
    );

    let cluster_name = config.source.cluster_name().to_string();
    let source_config = config.source;

    let batch_timestamp = if let Some(timestamp_str) = args.batch_timestamp {
        let parsed_timestamp = OffsetDateTime::parse(
            &timestamp_str,
            &time::format_description::well_known::Rfc3339,
        )
        .with_context(|| anyhow!("Failed to parse batch timestamp: {timestamp_str}"))?;
        debug!(
            operation = "batch_timestamp_provided",
            timestamp = %timestamp_str,
            "Using provided batch timestamp"
        );
        parsed_timestamp.to_offset(time::UtcOffset::UTC)
    } else {
        let now = OffsetDateTime::now_utc();
        debug!(
            operation = "batch_timestamp_current",
            timestamp = %now,
            "Using current time as batch timestamp"
        );
        now
    };

    info!(
        operation = "cluster_dump_start",
        cluster = %cluster_name,
        concurrency = %config.concurrency,
        "Starting dump for cluster"
    );

    let clickhouse_output = match &config.output {
        OutputConfig::Clickhouse(clickhouse_config) => {
            debug!(
                operation = "clickhouse_output_init",
                config = ?clickhouse_config,
                "Initializing ClickHouse output"
            );
            ClickHouseOutput::new(clickhouse_config.clone())
                .await
                .with_context(|| "Failed to initialize ClickHouse output")?
        }
    };

    let batch_info = BatchInfo {
        cluster: cluster_name.to_string(),
        batch: batch_timestamp,
    };

    debug!(
        operation = "batch_info_created",
        cluster = %batch_info.cluster,
        timestamp = %batch_info.batch,
        "Batch info created"
    );

    debug!(
        operation = "rdb_streams_fetch",
        "Getting RDB streams from source configuration"
    );
    let streams = source_config
        .get_rdb_streams()
        .await
        .with_context(|| "Failed to get RDB streams")?;

    debug!(
        operation = "concurrent_processing_start",
        stream_count = %streams.len(),
        "Starting concurrent processing of RDB streams"
    );
    let total_streams = streams.len();

    futures_util::stream::iter(streams)
        .map(Ok)
        .try_for_each_concurrent(config.concurrency, |stream| {
            let clickhouse_output = clickhouse_output.clone();
            let batch_info = batch_info.clone();
            async move {
                let instance = stream.instance();
                info!(
                    operation = "instance_processing_start",
                    instance = %instance,
                    "Starting processing"
                );

                process_records_to_clickhouse(stream, clickhouse_output, batch_info)
                    .await
                    .with_context(|| {
                        format!("Failed to process RDB stream from instance: {instance}")
                    })
            }
        })
        .await
        .with_context(|| {
            format!("Failed to process RDB streams ({total_streams} total instances)")
        })?;

    debug!(
        operation = "all_streams_processed",
        total_instances = total_streams,
        "All RDB streams processed successfully"
    );

    // Commit the batch only after all streams have been processed successfully
    debug!(
        operation = "committing_batch",
        cluster = %batch_info.cluster,
        batch = %batch_info.batch,
        total_instances = total_streams,
        "Committing batch after all instances completed"
    );

    let backoff_strategy = ExponentialBackoffBuilder::new()
        .with_initial_interval(std::time::Duration::from_secs(1))
        .with_max_interval(std::time::Duration::from_secs(10))
        .with_max_elapsed_time(Some(std::time::Duration::from_secs(60)))
        .build();

    commit_batch_with_retry(&clickhouse_output, &batch_info, &backoff_strategy)
        .await
        .with_context(|| "Failed to commit batch after processing all streams")?;

    info!(
        operation = "batch_committed",
        cluster = %batch_info.cluster,
        batch = %batch_info.batch,
        total_instances = total_streams,
        "Batch committed successfully for all instances"
    );
    Ok(())
}

async fn process_records_to_clickhouse(
    mut stream: Pin<Box<dyn RDBStream>>,
    clickhouse_output: ClickHouseOutput,
    batch_info: BatchInfo,
) -> Result<()> {
    // Get the instance identifier from the stream
    let instance = stream.instance();

    stream
        .prepare()
        .await
        .context("Failed to prepare RDB stream")?;

    const BATCH_SIZE: usize = 1_000_000;

    let start_time = Instant::now();
    let mut total_records = 0u64;
    let mut record_buffer: Vec<Record> = Vec::with_capacity(BATCH_SIZE);

    let mut record_stream = RecordStream::new(stream);

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

                if record_buffer.len() >= BATCH_SIZE {
                    write_batch_with_retry(
                        &clickhouse_output,
                        &record_buffer,
                        &batch_info,
                        &instance,
                        &backoff_strategy,
                    )
                    .await?;
                    info!(
                        operation = "batch_written",
                        instance = %instance,
                        batch_size = record_buffer.len(),
                        total_records = total_records,
                        "Wrote batch of records"
                    );
                    record_buffer.clear();
                }

                if total_records.is_multiple_of(100_000) {
                    debug!(
                        operation = "progress_update",
                        instance = %instance,
                        processed_records = total_records,
                        "Processing progress"
                    );
                }
            }
            Some(Err(e)) => {
                let elapsed = start_time.elapsed();
                error!(
                    operation = "parser_error",
                    instance = %instance,
                    processed_records = total_records,
                    elapsed_seconds = elapsed.as_secs(),
                    error = %e,
                    "Parser error during processing"
                );

                // Add root cause chain information
                let mut cause_chain = Vec::new();
                let mut current_error: &dyn std::error::Error = e.as_ref();
                while let Some(cause) = current_error.source() {
                    cause_chain.push(format!("{cause}"));
                    current_error = cause;
                }

                if !cause_chain.is_empty() {
                    error!(
                        operation = "error_chain",
                        instance = %instance,
                        error_chain = %cause_chain.join(" -> "),
                        "Error chain details"
                    );
                }

                return Err(e.context(format!(
                    "Failed to process RDB stream from instance {instance} after {total_records} records in {elapsed:?}"
                )));
            }
            None => {
                if !record_buffer.is_empty() {
                    write_batch_with_retry(
                        &clickhouse_output,
                        &record_buffer,
                        &batch_info,
                        &instance,
                        &backoff_strategy,
                    )
                    .await?;
                }
                break;
            }
        }
    }

    let duration = start_time.elapsed();
    info!(
        operation = "instance_processing_completed",
        instance = %instance,
        total_records = total_records,
        duration_seconds = duration.as_secs_f64(),
        records_per_second = total_records as f64 / duration.as_secs_f64(),
        "Instance processing completed"
    );

    Ok(())
}

async fn write_batch_with_retry(
    clickhouse_output: &ClickHouseOutput,
    records: &[Record],
    batch_info: &BatchInfo,
    instance: &str,
    backoff_strategy: &ExponentialBackoff,
) -> Result<()> {
    let operation = || async {
        clickhouse_output
            .write(records, batch_info, instance)
            .await
            .map_err(|e| {
                warn!(
                    operation = "batch_write_retry",
                    instance = %instance,
                    error = %e,
                    "Failed to write batch to ClickHouse, will retry"
                );
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
                warn!(
                    operation = "batch_commit_retry",
                    cluster = %batch_info.cluster,
                    batch = %batch_info.batch,
                    error = %e,
                    "Failed to commit batch to ClickHouse, will retry"
                );
                backoff::Error::transient(e)
            })
    };

    backoff::future::retry(backoff_strategy.clone(), operation)
        .await
        .with_context(|| "Failed to commit batch to ClickHouse after retries")
}

async fn run_report(args: ReportArgs) -> Result<()> {
    rdbinsight::report::run_report(args.config, args.cluster, args.batch, args.output).await
}
