use std::{path::PathBuf, pin::Pin, sync::Arc, time::Instant};

use anyhow::{Context, Result, anyhow};
use backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
use clap::{CommandFactory, Parser, Subcommand, value_parser};
use clap_complete::aot::{Shell, generate as generate_completion};
use futures_util::{StreamExt, TryStreamExt};
use rdbinsight::{
    config::{OutputConfig, ParquetCompression},
    output::{
        clickhouse::{BatchInfo, ClickHouseOutput},
        parquet::ParquetOutput,
    },
    record::{Record, RecordStream},
    source::{RDBStream, RdbSourceConfig},
};
use time::OffsetDateTime;
use tracing::{debug, error, info, warn};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};
use url::Url;

#[derive(Parser)]
struct MainCli {
    #[command(subcommand)]
    command: Command,

    /// Enable verbose logging
    #[clap(short, long, global = true)]
    verbose: bool,

    /// Global concurrency override
    #[arg(long, default_value_t = default_concurrency(), global = true, env = "RDBINSIGHT_CONCURRENCY")]
    concurrency: usize,
}

fn default_concurrency() -> usize {
    // Conservative default that works well for both single instances and small clusters
    (num_cpus::get() / 2).clamp(1, 16)
}

#[derive(Subcommand)]
enum Command {
    /// Dump Redis data to storage
    #[command(subcommand)]
    Dump(DumpCommand),
    /// Generate interactive HTML report
    Report(ReportArgs),
    /// Miscellaneous utilities
    #[command(subcommand)]
    Misc(MiscCommand),
}

#[derive(Subcommand)]
#[allow(clippy::enum_variant_names)]
enum DumpCommand {
    /// Dump from Redis standalone instance
    FromRedis(DumpRedisArgs),
    /// Dump from Redis cluster
    FromCluster(DumpClusterArgs),
    /// Dump from RDB file
    FromFile(DumpFileArgs),
    /// Dump from Codis cluster
    FromCodis(DumpCodisArgs),
}

#[derive(Parser)]
struct DumpRedisArgs {
    /// Redis standalone server address (e.g., 127.0.0.1:6379)
    #[arg(long)]
    addr: String,

    /// Cluster name for this Redis instance
    #[arg(long)]
    cluster: String,

    /// Redis username
    #[arg(long, default_value = "")]
    username: String,

    /// Redis password
    #[arg(long)]
    password: Option<String>,

    /// Batch timestamp (RFC3339, default = now)
    #[arg(long, env = "RDBINSIGHT_BATCH")]
    batch_timestamp: Option<String>,

    #[command(subcommand)]
    output: OutputCommand,
}

#[derive(Parser)]
struct DumpClusterArgs {
    /// Redis cluster node addresses (comma-separated, e.g., 127.0.0.1:7000,127.0.0.1:7001)
    #[arg(long, value_delimiter = ',')]
    nodes: Vec<String>,

    /// Cluster name for this Redis cluster
    #[arg(long)]
    cluster: String,

    /// Redis username
    #[arg(long, default_value = "")]
    username: String,

    /// Redis password
    #[arg(long)]
    password: Option<String>,

    /// Require reading from slave nodes only
    #[arg(long)]
    require_slave: bool,

    /// Batch timestamp (RFC3339, default = now)
    #[arg(long, env = "RDBINSIGHT_BATCH")]
    batch_timestamp: Option<String>,

    #[command(subcommand)]
    output: OutputCommand,
}

#[derive(Parser)]
struct DumpFileArgs {
    /// Path to RDB file
    #[arg(long)]
    path: PathBuf,

    /// Cluster name for this RDB file
    #[arg(long)]
    cluster: String,

    /// Instance identifier (e.g., IP:port)
    #[arg(long)]
    instance: String,

    /// Batch timestamp (RFC3339, default = now)
    #[arg(long, env = "RDBINSIGHT_BATCH")]
    batch_timestamp: Option<String>,

    #[command(subcommand)]
    output: OutputCommand,
}

#[derive(Parser)]
struct DumpCodisArgs {
    /// Codis dashboard address (e.g., http://127.0.0.1:11080)
    #[arg(long)]
    dashboard: String,

    /// Cluster name (optional, will be fetched from dashboard if not provided)
    #[arg(long)]
    cluster: Option<String>,

    /// Redis password
    #[arg(long)]
    password: Option<String>,

    /// Require reading from slave nodes only
    #[arg(long)]
    require_slave: bool,

    /// Batch timestamp (RFC3339, default = now)
    #[arg(long, env = "RDBINSIGHT_BATCH")]
    batch_timestamp: Option<String>,

    #[command(subcommand)]
    output: OutputCommand,
}

#[derive(Subcommand)]
enum OutputCommand {
    /// Output to ClickHouse
    IntoClickhouse(ClickHouseOutputArgs),
    /// Output to Parquet files
    IntoParquet(ParquetOutputArgs),
}

#[derive(Parser)]
struct ClickHouseOutputArgs {
    /// ClickHouse server URL (http[s]://[username[:password]@]host[:port]/[database])
    ///
    /// Default port: 8123
    ///
    /// Default database: rdbinsight
    #[arg(long, env = "RDBINSIGHT_CLICKHOUSE_URL")]
    url: Url,

    /// Automatically create tables if they don't exist
    #[arg(long, env = "RDBINSIGHT_CLICKHOUSE_AUTO_CREATE_TABLES")]
    auto_create_tables: bool,

    /// HTTP proxy URL for ClickHouse connections
    #[arg(long, env = "RDBINSIGHT_CLICKHOUSE_PROXY_URL")]
    proxy_url: Option<String>,
}

#[derive(Parser)]
struct ParquetOutputArgs {
    /// Output directory for Parquet files
    #[arg(long)]
    dir: PathBuf,

    /// Compression algorithm
    #[arg(long, value_enum, default_value_t = ParquetCompression::Zstd)]
    compression: ParquetCompression,
}

#[derive(Parser)]
struct ReportArgs {
    /// Cluster name
    #[clap(long, env = "RDBINSIGHT_CLUSTER")]
    cluster: String,

    /// Batch timestamp in RFC3339 format (defaults to latest batch)
    #[clap(long, env = "RDBINSIGHT_BATCH")]
    batch: Option<String>,

    /// Output HTML file path
    #[clap(short, long, env = "RDBINSIGHT_REPORT_OUTPUT")]
    output: Option<PathBuf>,

    /// ClickHouse server URL (http[s]://[username[:password]@]host[:port]/[database])
    ///
    /// Default port: 8123
    ///
    /// Default database: rdbinsight
    #[arg(long, env = "RDBINSIGHT_CLICKHOUSE_URL")]
    clickhouse_url: Url,

    /// HTTP proxy URL for ClickHouse connections
    #[arg(long, env = "RDBINSIGHT_CLICKHOUSE_PROXY_URL")]
    clickhouse_proxy_url: Option<String>,
}

#[derive(Subcommand)]
enum MiscCommand {
    /// Print recommended ClickHouse schema DDL statements
    #[command(name = "clickhouse-schema")]
    PrintClickhouseSchema,
    /// Generate shell completion script
    #[command(name = "completion")]
    GenCompletion(GenCompletionArgs),
}

#[derive(Parser)]
struct GenCompletionArgs {
    /// Shell type (e.g., bash, zsh, fish, elvish, powershell)
    #[arg(long, value_parser = value_parser!(Shell), env = "RDBINSIGHT_SHELL")]
    shell: Option<Shell>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let main_cli = MainCli::parse();

    let default_directive = if main_cli.verbose { "debug" } else { "info" };
    let level = EnvFilter::builder()
        .with_default_directive(default_directive.parse().expect("Failed to parse level"))
        .from_env()
        .expect("Failed to parse level");
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .with(level)
        .init();

    match main_cli.command {
        Command::Dump(dump_cmd) => dump_records(dump_cmd, main_cli.concurrency).await,
        Command::Report(args) => run_report(args).await,
        Command::Misc(misc_cmd) => match misc_cmd {
            MiscCommand::PrintClickhouseSchema => {
                print_clickhouse_schema();
                Ok(())
            }
            MiscCommand::GenCompletion(args) => {
                let shell = args.shell.or_else(Shell::from_env).ok_or_else(|| {
                    anyhow!("Failed to detect shell from environment; please specify --shell")
                })?;

                let mut cmd = MainCli::command();
                let bin_name = cmd.get_name().to_string();
                generate_completion(shell, &mut cmd, bin_name, &mut std::io::stdout());
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

// Convert new CLI structure to Config
fn dump_command_to_config(
    dump_cmd: DumpCommand,
    concurrency: usize,
) -> Result<(rdbinsight::config::DumpConfig, Option<String>)> {
    use rdbinsight::config::{
        ClickHouseConfig, DumpConfig, OutputConfig, ParquetConfig, SourceConfig,
    };

    let (source_config, batch_timestamp, output_cmd) = match dump_cmd {
        DumpCommand::FromRedis(args) => {
            let source = SourceConfig::RedisStandalone {
                cluster_name: args.cluster,
                address: args.addr,
                username: args.username,
                password: args.password,
            };
            (source, args.batch_timestamp, args.output)
        }
        DumpCommand::FromCluster(args) => {
            let source = SourceConfig::RedisCluster {
                cluster_name: args.cluster,
                addrs: args.nodes,
                username: args.username,
                password: args.password,
                require_slave: args.require_slave,
            };
            (source, args.batch_timestamp, args.output)
        }
        DumpCommand::FromFile(args) => {
            let source = SourceConfig::RDBFile {
                cluster_name: args.cluster,
                path: args.path.to_string_lossy().to_string(),
                instance: args.instance,
            };
            (source, args.batch_timestamp, args.output)
        }
        DumpCommand::FromCodis(args) => {
            let source = SourceConfig::Codis {
                cluster_name: args.cluster,
                dashboard_addr: args.dashboard,
                password: args.password,
                require_slave: args.require_slave,
            };
            (source, args.batch_timestamp, args.output)
        }
    };

    let output_config = match output_cmd {
        OutputCommand::IntoClickhouse(ch_args) => {
            let ch_config =
                ClickHouseConfig::new(ch_args.url, ch_args.auto_create_tables, ch_args.proxy_url)?;
            OutputConfig::Clickhouse(ch_config)
        }
        OutputCommand::IntoParquet(parquet_args) => {
            let parquet_config = ParquetConfig::new(parquet_args.dir, parquet_args.compression)?;
            OutputConfig::Parquet(parquet_config)
        }
    };

    let config = DumpConfig {
        source: source_config,
        output: output_config,
        concurrency,
    };

    // Validate the configuration
    config.validate()?;

    Ok((config, batch_timestamp))
}

async fn dump_records(dump_cmd: DumpCommand, concurrency: usize) -> Result<()> {
    debug!(
        operation = "config_parsing",
        "Parsing CLI arguments into configuration"
    );

    let (mut config, batch_timestamp_arg) = dump_command_to_config(dump_cmd, concurrency)
        .with_context(|| "Failed to parse CLI arguments into configuration")?;

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

    // Get batch timestamp from CLI, environment variable, or current time
    let batch_timestamp = if let Some(timestamp_str) = batch_timestamp_arg {
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

    info!(
        operation = "cluster_dump_start",
        cluster = %cluster_name,
        concurrency = %config.concurrency,
        "Starting dump for cluster"
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

    // Process based on output type
    match &config.output {
        OutputConfig::Clickhouse(clickhouse_config) => {
            debug!(
                operation = "clickhouse_output_init",
                config = ?clickhouse_config,
                "Initializing ClickHouse output"
            );
            let clickhouse_output = ClickHouseOutput::new(clickhouse_config.clone())
                .await
                .with_context(|| "Failed to initialize ClickHouse output")?;

            process_streams_to_clickhouse(
                streams,
                clickhouse_output,
                batch_info,
                config.concurrency,
            )
            .await?;
        }
        OutputConfig::Parquet(parquet_config) => {
            debug!(
                operation = "parquet_output_init",
                config = ?parquet_config,
                "Initializing Parquet output"
            );
            let parquet_output = ParquetOutput::new(
                parquet_config.dir.clone(),
                parquet_config.compression,
                &cluster_name,
                &batch_info,
            )
            .await
            .with_context(|| "Failed to initialize Parquet output")?;

            process_streams_to_parquet(streams, parquet_output, batch_info, config.concurrency)
                .await?;
        }
    }

    info!(
        operation = "dump_completed",
        cluster = %cluster_name,
        total_instances = total_streams,
        "Dump completed successfully for all instances"
    );
    Ok(())
}

async fn process_streams_to_clickhouse(
    streams: Vec<Pin<Box<dyn RDBStream>>>,
    clickhouse_output: ClickHouseOutput,
    batch_info: BatchInfo,
    concurrency: usize,
) -> Result<()> {
    let total_streams = streams.len();

    futures_util::stream::iter(streams)
        .map(Ok)
        .try_for_each_concurrent(concurrency, |stream| {
            let clickhouse_output = clickhouse_output.clone();
            let batch_info = batch_info.clone();
            async move {
                let instance = stream.instance();
                info!(
                    operation = "instance_processing_start",
                    instance = %instance,
                    "Starting processing"
                );

                tokio::spawn(async move {
                    process_records_to_clickhouse(stream, clickhouse_output, batch_info)
                        .await
                        .with_context(|| {
                            format!("Failed to process RDB stream from instance: {instance}")
                        })
                })
                .await??;

                Ok::<(), anyhow::Error>(())
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

async fn process_streams_to_parquet(
    streams: Vec<Pin<Box<dyn RDBStream>>>,
    parquet_output: ParquetOutput,
    batch_info: BatchInfo,
    concurrency: usize,
) -> Result<()> {
    let total_streams = streams.len();

    // We need to share the parquet_output across tasks, so we'll use Arc<Mutex<>>
    use std::sync::Arc;

    use tokio::sync::Mutex;
    let shared_parquet_output = Arc::new(Mutex::new(parquet_output));

    futures_util::stream::iter(streams)
        .map(Ok)
        .try_for_each_concurrent(concurrency, |stream| {
            let shared_parquet_output = shared_parquet_output.clone();
            let batch_info = batch_info.clone();
            async move {
                let instance = stream.instance();
                info!(
                    operation = "instance_processing_start",
                    instance = %instance,
                    "Starting processing"
                );

                tokio::spawn(async move {
                    process_records_to_parquet(stream, shared_parquet_output, batch_info)
                        .await
                        .with_context(|| {
                            format!("Failed to process RDB stream from instance: {instance}")
                        })
                })
                .await??;

                Ok::<(), anyhow::Error>(())
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

    // Finalize the batch directory after all instances are complete
    let parquet_output = Arc::try_unwrap(shared_parquet_output)
        .map_err(|_| anyhow::anyhow!("Failed to take ownership of parquet output"))?
        .into_inner();

    parquet_output
        .finalize_batch()
        .await
        .with_context(|| "Failed to finalize parquet batch")?;

    info!(
        operation = "parquet_batch_finalized",
        cluster = %batch_info.cluster,
        batch = %batch_info.batch,
        total_instances = total_streams,
        "Parquet batch finalized successfully for all instances"
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
    let source_type = stream.source_type();

    stream
        .prepare()
        .await
        .context("Failed to prepare RDB stream")?;

    const BATCH_SIZE: usize = 1_000_000;

    let start_time = Instant::now();
    let mut total_records = 0u64;
    let mut record_buffer: Vec<Record> = Vec::with_capacity(BATCH_SIZE);

    let mut record_stream = RecordStream::new(stream, source_type);

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

async fn process_records_to_parquet(
    mut stream: Pin<Box<dyn RDBStream>>,
    parquet_output: Arc<tokio::sync::Mutex<ParquetOutput>>,
    batch_info: BatchInfo,
) -> Result<()> {
    let instance = stream.instance();
    let source_type = stream.source_type();

    stream
        .prepare()
        .await
        .context("Failed to prepare RDB stream")?;

    const BATCH_SIZE: usize = 1_000_000;

    let start_time = Instant::now();
    let mut total_records = 0u64;
    let mut record_buffer: Vec<Record> = Vec::with_capacity(BATCH_SIZE);

    let mut record_stream = RecordStream::new(stream, source_type);

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
                    write_parquet_batch_with_retry(
                        &parquet_output,
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
                    write_parquet_batch_with_retry(
                        &parquet_output,
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

    // Finalize this instance
    {
        let mut parquet_output = parquet_output.lock().await;
        parquet_output
            .finalize_instance(&instance)
            .await
            .with_context(|| format!("Failed to finalize Parquet instance: {instance}"))?;
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

async fn write_parquet_batch_with_retry(
    parquet_output: &Arc<tokio::sync::Mutex<ParquetOutput>>,
    records: &[Record],
    batch_info: &BatchInfo,
    instance: &str,
    backoff_strategy: &ExponentialBackoff,
) -> Result<()> {
    let operation = || async {
        let mut parquet_output = parquet_output.lock().await;
        parquet_output
            .write(records, batch_info, instance)
            .await
            .map_err(|e| {
                warn!(
                    operation = "parquet_batch_write_retry",
                    instance = %instance,
                    records_count = records.len(),
                    error = %e,
                    "Failed to write batch to Parquet, will retry"
                );
                backoff::Error::transient(e)
            })
    };

    backoff::future::retry(backoff_strategy.clone(), operation)
        .await
        .with_context(|| {
            format!(
                "Failed to write batch of {} records to Parquet for instance {} after retries",
                records.len(),
                instance
            )
        })
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
    let clickhouse_config = rdbinsight::config::ClickHouseConfig::new(
        args.clickhouse_url,
        false, // Reports don't need auto-creation
        args.clickhouse_proxy_url,
    )?;

    // Validate the configuration
    clickhouse_config
        .validate()
        .with_context(|| "Invalid ClickHouse configuration")?;

    rdbinsight::report::run_report_with_config(
        clickhouse_config,
        args.cluster,
        args.batch,
        args.output,
    )
    .await
}
