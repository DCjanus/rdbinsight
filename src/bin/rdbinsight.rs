use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use anyhow::{Context, Result, anyhow, ensure};
use clap::{Args, CommandFactory, Parser, Subcommand, value_parser};
use clap_complete::aot::{Shell, generate as generate_completion};
use futures_util::{StreamExt, TryStreamExt};
use rdbinsight::{
    config::{DumpConfig, ParquetCompression},
    metric,
    output::{ChunkWriter, ChunkWriterEnum, Output},
    record::RecordStream,
    source::{RDBStream, RdbSourceConfig},
};
use time::OffsetDateTime;
use tracing::{debug, info};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};
use url::Url;

shadow_rs::shadow!(build);

#[derive(Parser)]
#[command(version = build::VERSION)]
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
    Dump(DumpArgs),
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
    FromStandalone(DumpStandaloneArgs),
    /// Dump from Redis cluster
    FromCluster(DumpClusterArgs),
    /// Dump from RDB file
    FromFile(DumpFileArgs),
    /// Dump from Codis cluster
    FromCodis(DumpCodisArgs),
}

#[derive(Args)]
struct DumpArgs {
    /// Optional Prometheus endpoint URL (http://host:port)
    #[clap(long, env = "RDBINSIGHT_PROMETHEUS")]
    prometheus: Option<Url>,

    /// Number of parallel connections for dump operations
    ///
    /// Default: CPU cores / 2 (min 1, max 8)
    #[arg(long, default_value_t = default_concurrency(), env = "RDBINSIGHT_CONCURRENCY")]
    concurrency: usize,

    #[command(subcommand)]
    cmd: DumpCommand,
}

#[derive(Args)]
struct DumpStandaloneArgs {
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

#[derive(Args)]
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

#[derive(Args)]
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

#[derive(Args)]
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
    /// ClickHouse server URL (must include explicit port and ?database=<db>)
    /// Example: http[s]://[user[:pass]@]host:8123?database=<db>
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

    /// Compression algorithm for final merged files
    #[arg(long, value_enum, default_value_t = ParquetCompression::Zstd)]
    compression: ParquetCompression,

    /// Max rows per run segment before flushing to disk
    #[arg(long, default_value_t = 65536)]
    run_rows: usize,
}

#[derive(Parser)]
struct ReportArgs {
    /// Report command variants
    #[command(subcommand)]
    cmd: ReportCommand,
}

#[derive(Subcommand)]
enum ReportCommand {
    /// Generate report from ClickHouse
    #[command(name = "from-clickhouse")]
    FromClickhouse(ReportFromClickHouseArgs),
}

#[derive(Args)]
struct ReportFromClickHouseArgs {
    /// Cluster name
    #[clap(long, env = "RDBINSIGHT_CLUSTER")]
    cluster: String,

    /// Batch timestamp in RFC3339 format (defaults to latest batch)
    #[clap(long, env = "RDBINSIGHT_BATCH")]
    batch: Option<String>,

    /// Output HTML file path
    #[clap(short, long, env = "RDBINSIGHT_REPORT_OUTPUT")]
    output: Option<PathBuf>,

    /// ClickHouse server URL (must include explicit port and ?database=<db>)
    /// Example: http[s]://[user[:pass]@]host:8123?database=<db>
    #[arg(long, env = "RDBINSIGHT_CLICKHOUSE_URL")]
    url: Url,

    /// HTTP proxy URL for ClickHouse connections
    #[arg(long, env = "RDBINSIGHT_CLICKHOUSE_PROXY_URL")]
    proxy_url: Option<String>,
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

    metric::init_metrics();

    match main_cli.command {
        Command::Dump(dump_args) => {
            if let Some(ref prometheus_url) = dump_args.prometheus {
                let addr = parse_prometheus_addr(prometheus_url)
                    .await
                    .with_context(|| "Invalid --prometheus URL; expected http://host:port")?;

                info!(operation = "metrics_start", listen = %addr, path = "/metrics", "Starting Prometheus metrics endpoint");
                tokio::spawn(metric::run_metrics_server(addr));
            }

            dump_records(dump_args.cmd, dump_args.concurrency).await
        }
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
    global_concurrency: usize,
) -> Result<(rdbinsight::config::DumpConfig, Option<String>)> {
    use rdbinsight::config::{
        ClickHouseConfig, DumpConfig, OutputConfig, ParquetConfig, SourceConfig,
    };

    let (source_config, batch_timestamp, output_cmd, concurrency) = match dump_cmd {
        DumpCommand::FromStandalone(args) => {
            let source = SourceConfig::RedisStandalone {
                cluster_name: args.cluster,
                address: args.addr,
                username: args.username,
                password: args.password,
            };
            (
                source,
                args.batch_timestamp,
                args.output,
                global_concurrency,
            )
        }
        DumpCommand::FromCluster(args) => {
            let source = SourceConfig::RedisCluster {
                cluster_name: args.cluster,
                addrs: args.nodes,
                username: args.username,
                password: args.password,
                require_slave: args.require_slave,
            };
            (
                source,
                args.batch_timestamp,
                args.output,
                global_concurrency,
            )
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
            (
                source,
                args.batch_timestamp,
                args.output,
                global_concurrency,
            )
        }
    };

    let output_config = match output_cmd {
        OutputCommand::IntoClickhouse(ch_args) => {
            let ch_config =
                ClickHouseConfig::new(ch_args.url, ch_args.auto_create_tables, ch_args.proxy_url)?;
            OutputConfig::Clickhouse(ch_config)
        }
        OutputCommand::IntoParquet(parquet_args) => {
            let parquet_config = ParquetConfig::new(
                parquet_args.dir,
                parquet_args.compression,
                parquet_args.run_rows,
            )?;
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

async fn dump_records(dump_cmd: DumpCommand, global_concurrency: usize) -> Result<()> {
    let (mut config, batch_timestamp_arg): (DumpConfig, Option<String>) =
        dump_command_to_config(dump_cmd, global_concurrency)
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

async fn parse_prometheus_addr(url: &Url) -> Result<SocketAddr> {
    ensure!(url.scheme() == "http", "only http scheme is supported");

    let port = url.port().ok_or_else(|| {
        anyhow!("--prometheus URL must include an explicit port, e.g., http://0.0.0.0:9901")
    })?;

    match url.host() {
        Some(url::Host::Ipv4(ip)) => Ok(SocketAddr::new(IpAddr::V4(ip), port)),
        Some(url::Host::Ipv6(ip)) => Ok(SocketAddr::new(IpAddr::V6(ip), port)),
        Some(url::Host::Domain(domain)) => {
            if domain.eq_ignore_ascii_case("localhost") {
                Ok(SocketAddr::new(
                    IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                    port,
                ))
            } else {
                Err(anyhow!(
                    "--prometheus host must be an IP address (IPv4/IPv6) or 'localhost'"
                ))
            }
        }
        None => Err(anyhow!("--prometheus URL must include host")),
    }
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
    cluster: String,
    batch_label: String,
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

        metric::inc_record(&cluster, &batch_label, &progress.instance_name);

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
    let batch_label = batch_ts
        .format(&time::format_description::well_known::Rfc3339)
        .context("Failed to format batch timestamp")?;
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
            let cluster_clone = cluster.clone();
            let batch_clone = batch_label.clone();
            async move {
                tokio::spawn(handle_rdb_stream(
                    stream,
                    instance,
                    writer,
                    instance_progress,
                    cluster_clone,
                    batch_clone,
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
    match args.cmd {
        ReportCommand::FromClickhouse(sub) => {
            let clickhouse_config = rdbinsight::config::ClickHouseConfig::new(
                sub.url,
                false, // Reports don't need auto-creation
                sub.proxy_url,
            )?;

            rdbinsight::report::run_report_with_config(
                clickhouse_config,
                sub.cluster,
                sub.batch,
                sub.output,
            )
            .await
        }
    }
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
    let local_records_formatted = format_number(local_records as f64);
    let global_records_formatted = format_number(global_records as f64);
    let rps_formatted = format_number(rps);

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

/// Format numbers into human-readable format
/// Uses units K, M, B, T where B represents billion (10^9)
/// Handles special f64 values: NaN, infinity, and negative numbers
fn format_number(num: f64) -> String {
    // Handle special f64 values
    if num.is_nan() {
        return "NaN".to_string();
    }
    if num.is_infinite() {
        return if num.is_sign_positive() {
            "inf".to_string()
        } else {
            "-inf".to_string()
        };
    }
    if num == 0.0 {
        return "0".to_string();
    }

    // Handle negative numbers by formatting absolute value and adding sign back
    let is_negative = num < 0.0;
    let abs_num = num.abs();

    let mut suffix = "";
    let mut divisor = 1.0;

    // Define units, using B as billion (10^9)
    let units = [
        (1_000_000_000_000.0, "T"), // trillion
        (1_000_000_000.0, "B"),     // billion (not G)
        (1_000_000.0, "M"),         // million
        (1_000.0, "K"),             // thousand
    ];

    for (threshold, unit) in &units {
        if abs_num >= *threshold {
            suffix = unit;
            divisor = *threshold;
            break;
        }
    }

    if divisor == 1.0 {
        let result = format!("{}", abs_num as u64);
        return if is_negative {
            format!("-{}", result)
        } else {
            result
        };
    }

    // Use integer arithmetic to avoid floating point precision issues
    let scaled = (abs_num * 100.0) / divisor;
    let integer_part = (scaled / 100.0) as u64;
    let decimal_part = (scaled % 100.0) as u32;

    let formatted_str = format!("{}.{:02}{}", integer_part, decimal_part, suffix);

    if is_negative {
        format!("-{}", formatted_str)
    } else {
        formatted_str
    }
}

#[cfg(test)]
mod tests {
    use super::format_number;

    #[test]
    fn test_format_number_special_values() {
        // Test NaN
        assert_eq!(format_number(f64::NAN), "NaN");

        // Test positive infinity
        assert_eq!(format_number(f64::INFINITY), "inf");

        // Test negative infinity
        assert_eq!(format_number(f64::NEG_INFINITY), "-inf");

        // Test zero
        assert_eq!(format_number(0.0), "0");
        assert_eq!(format_number(-0.0), "0");
    }

    #[test]
    fn test_format_number_small_numbers() {
        // Test numbers less than 1000
        assert_eq!(format_number(0.0), "0");
        assert_eq!(format_number(1.0), "1");
        assert_eq!(format_number(999.0), "999");

        // Test negative small numbers
        assert_eq!(format_number(-1.0), "-1");
        assert_eq!(format_number(-999.0), "-999");
    }

    #[test]
    fn test_format_number_thousand_unit() {
        // Test K unit (1000 - 999,999)
        assert_eq!(format_number(1000.0), "1.00K");
        assert_eq!(format_number(1500.0), "1.50K");
        assert_eq!(format_number(999999.0), "999.99K");

        // Test negative K unit
        assert_eq!(format_number(-1000.0), "-1.00K");
        assert_eq!(format_number(-1500.0), "-1.50K");

        // Test edge cases
        assert_eq!(format_number(999.0), "999"); // Should not be formatted as K
        assert_eq!(format_number(1001.0), "1.00K"); // Should be formatted as K with 2 decimals
    }

    #[test]
    fn test_format_number_million_unit() {
        // Test M unit (1,000,000 - 999,999,999)
        assert_eq!(format_number(1_000_000.0), "1.00M");
        assert_eq!(format_number(1_500_000.0), "1.50M");
        assert_eq!(format_number(999_999_999.0), "999.99M");

        // Test negative M unit
        assert_eq!(format_number(-1_000_000.0), "-1.00M");
        assert_eq!(format_number(-1_500_000.0), "-1.50M");
    }

    #[test]
    fn test_format_number_billion_unit() {
        // Test B unit (1,000,000,000 - 999,999,999,999)
        assert_eq!(format_number(1_000_000_000.0), "1.00B");
        assert_eq!(format_number(1_500_000_000.0), "1.50B");
        assert_eq!(format_number(999_999_999_999.0), "999.99B");

        // Test negative B unit
        assert_eq!(format_number(-1_000_000_000.0), "-1.00B");
        assert_eq!(format_number(-1_500_000_000.0), "-1.50B");
    }

    #[test]
    fn test_format_number_trillion_unit() {
        // Test T unit (>= 1,000,000,000,000)
        assert_eq!(format_number(1_000_000_000_000.0), "1.00T");
        assert_eq!(format_number(1_500_000_000_000.0), "1.50T");
        assert_eq!(format_number(999_999_999_999_999.0), "999.99T");

        // Test negative T unit
        assert_eq!(format_number(-1_000_000_000_000.0), "-1.00T");
        assert_eq!(format_number(-1_500_000_000_000.0), "-1.50T");
    }

    #[test]
    fn test_format_number_decimal_places() {
        // With the new logic, all formatted numbers now show 2 decimal places
        assert_eq!(format_number(100_000.0), "100.00K");
        assert_eq!(format_number(1_500_000.0), "1.50M");
        assert_eq!(format_number(250_000_000.0), "250.00M");

        assert_eq!(format_number(10_000.0), "10.00K");
        assert_eq!(format_number(15_000.0), "15.00K");
        assert_eq!(format_number(99_999.0), "99.99K");

        assert_eq!(format_number(1_000.0), "1.00K");
        assert_eq!(format_number(1_500.0), "1.50K");
        assert_eq!(format_number(9_999.0), "9.99K");
    }

    #[test]
    fn test_format_number_edge_cases() {
        // Test very large numbers
        assert_eq!(format_number(1_000_000_000_000_000.0), "1000.00T");

        // Test numbers just at unit boundaries
        assert_eq!(format_number(999_999.0), "999.99K");
        assert_eq!(format_number(1_000_000.0), "1.00M");
        assert_eq!(format_number(999_999_999.0), "999.99M");
        assert_eq!(format_number(1_000_000_000.0), "1.00B");
        assert_eq!(format_number(999_999_999_999.0), "999.99B");
        assert_eq!(format_number(1_000_000_000_000.0), "1.00T");

        // Test floating point precision
        assert_eq!(format_number(1_000_000_000.5), "1.00B");
    }
}
