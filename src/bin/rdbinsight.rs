use std::{collections::HashMap, path::PathBuf, pin::Pin, sync::Arc, time::Instant};

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
use tokio::sync::Mutex;
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

    /// Global concurrency override
    #[arg(long, default_value_t = default_concurrency(), global = true, env = "RDBINSIGHT_CONCURRENCY"
    )]
    concurrency: usize,
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
    let (mut config, batch_timestamp_arg): (DumpConfig, Option<String>) =
        dump_command_to_config(dump_cmd, concurrency)
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

fn log_progress(
    start_time: Instant,
    processed_records: u64,
    completed_instances: usize,
    total_instances: usize,
    instance: &str,
    instance_processed_records: u64,
) {
    let elapsed = start_time.elapsed().as_secs_f64().max(0.000_001);
    let rps = processed_records as f64 / elapsed;
    info!(
        operation = "progress_update",
        instance = %instance,
        processed_records = processed_records,
        instance_processed_records = instance_processed_records,
        completed_instances = completed_instances,
        total_instances = total_instances,
        rps = rps,
        "Progress update"
    );
}

struct ProgressState {
    processed_records: u64,
    completed_instances: usize,
    start_time: Instant,
    last_log: Instant,
    total_instances: usize,
    per_instance_records: HashMap<String, u64>,
}

type SharedProgress = Arc<Mutex<ProgressState>>;

fn init_progress(total_instances: usize) -> SharedProgress {
    Arc::new(Mutex::new(ProgressState {
        processed_records: 0,
        completed_instances: 0,
        start_time: Instant::now(),
        last_log: Instant::now(),
        total_instances,
        per_instance_records: HashMap::new(),
    }))
}

async fn update_progress_after_write(
    progress: &SharedProgress,
    added_records: u64,
    instance: &str,
) {
    let mut state = progress.lock().await;
    state.processed_records = state.processed_records.saturating_add(added_records);
    let entry = state
        .per_instance_records
        .entry(instance.to_string())
        .or_insert(0);
    *entry = entry.saturating_add(added_records);

    let instance_processed_records = *state.per_instance_records.get(instance).unwrap_or(&0);
    log_progress(
        state.start_time,
        state.processed_records,
        state.completed_instances,
        state.total_instances,
        instance,
        instance_processed_records,
    );
    state.last_log = Instant::now();
}

async fn mark_instance_completed(progress: &SharedProgress, instance: &str) {
    let mut state = progress.lock().await;
    state.completed_instances = state.completed_instances.saturating_add(1);
    let instance_processed_records = *state.per_instance_records.get(instance).unwrap_or(&0);
    log_progress(
        state.start_time,
        state.processed_records,
        state.completed_instances,
        state.total_instances,
        instance,
        instance_processed_records,
    );
    state.last_log = Instant::now();
}

async fn handle_rdb_stream(
    mut stream: Pin<Box<dyn RDBStream>>,
    instance: String,
    mut writer: ChunkWriterEnum,
    progress: SharedProgress,
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

    const REPORT_INTERVAL: u64 = 1_000_000;

    let mut record_stream = RecordStream::new(stream, source_type);
    let mut processed_since_report: u64 = 0;

    while let Some(next) = record_stream.next().await {
        let record =
            next.with_context(|| anyhow!("Failed to parse record from instance {instance}"))?;

        writer
            .write_record(record)
            .await
            .context("Failed to write record")?;

        processed_since_report = processed_since_report.saturating_add(1);
        if processed_since_report >= REPORT_INTERVAL {
            update_progress_after_write(&progress, processed_since_report, &instance).await;
            processed_since_report = 0;
        }
    }

    if processed_since_report > 0 {
        update_progress_after_write(&progress, processed_since_report, &instance).await;
    }

    writer
        .finalize_instance()
        .await
        .with_context(|| anyhow!("Failed to finalize writer for instance {instance}"))?;

    mark_instance_completed(&progress, &instance).await;

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

    let progress = init_progress(tasks.len());

    futures_util::stream::iter(tasks)
        .map(Ok)
        .try_for_each_concurrent(concurrency.max(1), |(stream, instance, writer)| {
            let progress = progress.clone();
            async move {
                tokio::spawn(handle_rdb_stream(stream, instance, writer, progress))
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
