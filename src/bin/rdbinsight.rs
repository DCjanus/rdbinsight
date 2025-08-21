use std::{
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use anyhow::{Context, Result, anyhow};
use clap::{CommandFactory, Parser, Subcommand, value_parser};
use clap_complete::aot::{Shell, generate as generate_completion};
use futures_util::{StreamExt, TryStreamExt};
use rdbinsight::{
    config::{DumpConfig, ParquetCompression},
    output::{ChunkWriter, ChunkWriterEnum, Output},
    record::RecordStream,
    source::{RDBStream, RdbSourceConfig},
};
use time::OffsetDateTime;
use tracing::{debug, info};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};
use url::Url;

#[derive(Parser)]
struct MainCli {
    #[command(subcommand)]
    command: Command,

    /// Enable verbose logging
    #[clap(short, long, global = true)]
    verbose: bool,
}

fn default_concurrency() -> usize {
    // Conservative default that works well for both single instances and small clusters
    (num_cpus::get() / 2).clamp(1, 8)
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

    /// Concurrency for dump operations
    #[arg(long, default_value_t = default_concurrency(), env = "RDBINSIGHT_CONCURRENCY")]
    concurrency: usize,

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

    /// Concurrency for dump operations
    #[arg(long, default_value_t = default_concurrency(), env = "RDBINSIGHT_CONCURRENCY")]
    concurrency: usize,

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

    /// Concurrency for dump operations
    #[arg(long, default_value_t = default_concurrency(), env = "RDBINSIGHT_CONCURRENCY")]
    concurrency: usize,

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
        Command::Dump(dump_cmd) => dump_records(dump_cmd).await,
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
) -> Result<(rdbinsight::config::DumpConfig, Option<String>)> {
    use rdbinsight::config::{
        ClickHouseConfig, DumpConfig, OutputConfig, ParquetConfig, SourceConfig,
    };

    let (source_config, batch_timestamp, output_cmd, concurrency) = match dump_cmd {
        DumpCommand::FromRedis(args) => {
            let source = SourceConfig::RedisStandalone {
                cluster_name: args.cluster,
                address: args.addr,
                username: args.username,
                password: args.password,
            };
            (source, args.batch_timestamp, args.output, args.concurrency)
        }
        DumpCommand::FromCluster(args) => {
            let source = SourceConfig::RedisCluster {
                cluster_name: args.cluster,
                addrs: args.nodes,
                username: args.username,
                password: args.password,
                require_slave: args.require_slave,
            };
            (source, args.batch_timestamp, args.output, args.concurrency)
        }
        DumpCommand::FromFile(args) => {
            let source = SourceConfig::RDBFile {
                cluster_name: args.cluster,
                path: args.path.to_string_lossy().to_string(),
                instance: args.instance,
            };
            // For file dump, we use concurrency of 1 since we only process one file
            (source, args.batch_timestamp, args.output, 1)
        }
        DumpCommand::FromCodis(args) => {
            let source = SourceConfig::Codis {
                cluster_name: args.cluster,
                dashboard_addr: args.dashboard,
                password: args.password,
                require_slave: args.require_slave,
            };
            (source, args.batch_timestamp, args.output, args.concurrency)
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

async fn dump_records(dump_cmd: DumpCommand) -> Result<()> {
    let (mut config, batch_timestamp_arg): (DumpConfig, Option<String>) =
        dump_command_to_config(dump_cmd)
            .with_context(|| "Failed to parse CLI arguments into configuration")?;

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

    info!(
        operation = "cluster_dump_start",
        cluster = %cluster_name,
        batch = %batch_timestamp,
        concurrency = %config.concurrency,
        "Starting dump for cluster"
    );

    let streams: Vec<Pin<Box<dyn RDBStream>>> = source_config
        .get_rdb_streams()
        .await
        .with_context(|| "Failed to get RDB streams")?;
    let total_streams = streams.len();

    process_rdb_streams(
        streams,
        config.output,
        cluster_name.to_string(),
        batch_timestamp,
        config.concurrency,
    )
    .await?;

    info!(
        operation = "dump_completed",
        cluster = %cluster_name,
        total_instances = total_streams,
        "Dump completed successfully for all instances"
    );
    Ok(())
}

// Simplified global state management
struct BatchContext {
    global_processed: Arc<AtomicU64>,
    completed_instances: Arc<AtomicU64>,
    start_time: Instant,
    total_instances: usize,
}

// Progress state for each coroutine
struct InstanceProgress {
    instance_name: String,
    local_count: u64,
    global_processed: Arc<AtomicU64>,
    completed_instances: Arc<AtomicU64>,
    start_time: Instant,
    total_instances: usize,
}

type SharedBatchContext = Arc<BatchContext>;

fn create_batch_context(total_instances: usize) -> SharedBatchContext {
    Arc::new(BatchContext {
        global_processed: Arc::new(AtomicU64::new(0)),
        completed_instances: Arc::new(AtomicU64::new(0)),
        start_time: Instant::now(),
        total_instances,
    })
}

fn create_instance_progress(
    instance_name: String,
    batch_context: &SharedBatchContext,
) -> InstanceProgress {
    InstanceProgress {
        instance_name,
        local_count: 0,
        global_processed: Arc::clone(&batch_context.global_processed),
        completed_instances: Arc::clone(&batch_context.completed_instances),
        start_time: batch_context.start_time,
        total_instances: batch_context.total_instances,
    }
}

async fn handle_rdb_stream(
    mut stream: Pin<Box<dyn RDBStream>>,
    instance: String,
    mut writer: ChunkWriterEnum,
    mut progress: InstanceProgress,
) -> Result<()> {
    let source_type = stream.source_type();

    stream
        .prepare()
        .await
        .with_context(|| anyhow!("Failed to prepare RDB stream for instance {instance}"))?;

    writer
        .prepare_instance()
        .await
        .with_context(|| anyhow!("Failed to prepare writer for instance {instance}"))?;

    let mut record_stream = RecordStream::new(stream, source_type);

    while let Some(next) = record_stream.next().await {
        let record =
            next.with_context(|| anyhow!("Failed to parse record from instance {instance}"))?;

        writer
            .write_record(record)
            .await
            .context("Failed to write record")?;

        progress.local_count += 1;
        let global_count = progress.global_processed.fetch_add(1, Ordering::Relaxed) + 1;

        if progress.local_count.is_multiple_of(1_000_000) {
            let rps = global_count as f64 / progress.start_time.elapsed().as_secs_f64();
            let completed = progress.completed_instances.load(Ordering::Relaxed) as usize;

            log_instance_progress(
                &progress.instance_name,
                progress.local_count,
                global_count,
                completed,
                progress.total_instances,
                rps,
                "Progress update",
            );
        }
    }

    writer
        .finalize_instance()
        .await
        .with_context(|| anyhow!("Failed to finalize writer for instance {instance}"))?;

    // Always output final statistics when instance completes
    let global_count = progress.global_processed.load(Ordering::Relaxed);
    let rps = global_count as f64 / progress.start_time.elapsed().as_secs_f64();
    let completed = progress.completed_instances.fetch_add(1, Ordering::Relaxed) + 1;

    log_instance_progress(
        &progress.instance_name,
        progress.local_count,
        global_count,
        completed as usize,
        progress.total_instances,
        rps,
        "Instance completed",
    );

    Ok(())
}

async fn process_rdb_streams(
    streams: Vec<Pin<Box<dyn RDBStream>>>,
    output_config: rdbinsight::config::OutputConfig,
    cluster: String,
    batch_ts: OffsetDateTime,
    concurrency: usize,
) -> Result<()> {
    let output = output_config
        .create_output(cluster.clone(), batch_ts)
        .with_context(|| "Failed to create output from configuration")?;

    output
        .prepare_batch()
        .await
        .with_context(|| "Failed to prepare batch for output")?;

    let mut tasks = Vec::with_capacity(streams.len());
    for stream in streams {
        let instance = stream.instance().to_string();
        let writer = output
            .create_writer(&instance)
            .await
            .with_context(|| anyhow!("Failed to create writer for instance {instance}"))?;
        tasks.push((stream, instance, writer));
    }

    let batch_context = create_batch_context(tasks.len());

    futures_util::stream::iter(tasks)
        .map(Ok)
        .try_for_each_concurrent(concurrency.max(1), |(stream, instance, writer)| {
            let instance_progress = create_instance_progress(instance.clone(), &batch_context);
            async move {
                tokio::spawn(handle_rdb_stream(
                    stream,
                    instance,
                    writer,
                    instance_progress,
                ))
                .await
                .with_context(|| "Failed to join spawned direct-writer task")?
                .with_context(|| "Failed to handle RDB stream")?;
                Ok::<(), anyhow::Error>(())
            }
        })
        .await?;

    Box::new(output)
        .finalize_batch()
        .await
        .with_context(|| "Failed to finalize batch for output")?;

    Ok(())
}

async fn run_report(args: ReportArgs) -> Result<()> {
    let clickhouse_config = rdbinsight::config::ClickHouseConfig::new(
        args.clickhouse_url,
        false, // Reports don't need auto-creation
        args.clickhouse_proxy_url,
    )?;

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

fn log_instance_progress(
    instance_name: &str,
    local_records: u64,
    global_records: u64,
    completed_instances: usize,
    total_instances: usize,
    rps: f64,
    message: &str,
) {
    let mut formatter = human_format::Formatter::new();
    formatter.with_separator("");
    let local_records_formatted = formatter.format(local_records as f64);
    let global_records_formatted = formatter.format(global_records as f64);
    let rps_formatted = formatter.format(rps);

    // TODO: 1 billion being formatted as 1.00G rather than 1.00B, let's re-implement it
    info!(
        operation = "progress_update",
        instance = %instance_name,
        instance_progressed = %local_records_formatted,
        global_progressed = %global_records_formatted,
        completed_instances = completed_instances,
        total_instances = total_instances,
        rps = %rps_formatted,
        "{message}"
    );
}
