use std::{path::PathBuf, time::Instant};

use anyhow::{Context, Result};
use clap::Parser;
use futures_util::StreamExt;
use rdbinsight::{
    config::{Config, SourceConfig},
    parser::core::raw::RDBStr,
    record::{Record, RecordStream, RecordType},
    source::{RdbSourceConfig, file::Config as FileConfig, standalone::Config as StandaloneConfig},
};
use tracing::{debug, error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser)]
struct Cli {
    /// Path to configuration file
    config_file: PathBuf,

    /// Enable verbose logging
    #[clap(short, long)]
    verbose: bool,
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

    parse_from_config(cli.config_file).await
}

async fn parse_from_config(config_path: PathBuf) -> Result<()> {
    info!("Loading configuration from: {}", config_path.display());

    let config_content = tokio::fs::read_to_string(&config_path)
        .await
        .with_context(|| format!("Failed to read config file: {}", config_path.display()))?;

    let config: Config =
        toml::from_str(&config_content).with_context(|| "Failed to parse configuration file")?;

    info!("Configuration loaded successfully");
    info!("Cluster: {}", get_cluster_name(&config.source));
    info!("Concurrency: {}", config.concurrency);

    match config.source {
        SourceConfig::RDBFile { path, .. } => parse_from_file(PathBuf::from(path)).await,
        SourceConfig::RedisStandalone {
            address,
            username,
            password,
            ..
        } => parse_from_standalone(address, username, password).await,
        _ => {
            error!("Source type not supported in minimal implementation");
            anyhow::bail!(
                "Unsupported source type. Only RDB file and Redis standalone are supported."
            );
        }
    }
}

async fn parse_from_file(path: PathBuf) -> Result<()> {
    info!("Parsing RDB file: {}", path.display());

    let file_config = FileConfig { path: path.clone() };
    let stream = file_config
        .get_rdb_stream()
        .await
        .with_context(|| format!("Failed to open RDB file: {}", path.display()))?;

    parse_rdb_stream(stream).await
}

async fn parse_from_standalone(
    address: String,
    username: Option<String>,
    password: Option<String>,
) -> Result<()> {
    info!("Connecting to Redis standalone: {address}");

    let standalone_config = StandaloneConfig {
        address: address.clone(),
        username,
        password,
    };

    let stream = standalone_config
        .get_rdb_stream()
        .await
        .with_context(|| format!("Failed to connect to Redis: {address}"))?;

    parse_rdb_stream(stream).await
}

async fn parse_rdb_stream(
    stream: std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>>,
) -> Result<()> {
    let start_time = Instant::now();
    let mut total_records = 0u64;
    let total_bytes_read = 0u64;

    // Statistics counters
    let mut stats = RecordStats::default();

    info!("Starting RDB parsing...");

    // Create RecordStream that will manage buffer and reading internally
    let mut record_stream = RecordStream::new(stream);

    loop {
        // Try to get next record using Stream trait
        match record_stream.next().await {
            Some(Ok(record)) => {
                total_records += 1;
                stats.update(&record);

                // Output record information to stdout (simulating ClickHouse output)
                output_record(&record, total_records);

                // Log progress every 1000000 records
                if total_records % 1000000 == 0 {
                    debug!("Processed {total_records} records");
                }
            }
            Some(Err(e)) => {
                error!("Parser error: {e}");
                return Err(e);
            }
            None => {
                // End of stream
                info!("Reached end of RDB stream");
                break;
            }
        }
    }

    let duration = start_time.elapsed();

    // Print final statistics
    print_final_stats(total_records, total_bytes_read, duration, &stats);

    Ok(())
}

#[derive(Default)]
struct RecordStats {
    strings: u64,
    lists: u64,
    sets: u64,
    zsets: u64,
    hashes: u64,
    streams: u64,
    modules: u64,
    total_size: u64,
    expired_keys: u64,
    keys_with_idle: u64,
    keys_with_freq: u64,
}

impl RecordStats {
    fn update(&mut self, record: &Record) {
        // Update type counters
        match record.r#type {
            RecordType::String => self.strings += 1,
            RecordType::List => self.lists += 1,
            RecordType::Set => self.sets += 1,
            RecordType::ZSet => self.zsets += 1,
            RecordType::Hash => self.hashes += 1,
            RecordType::Stream => self.streams += 1,
            RecordType::Module => self.modules += 1,
        }

        // Update size
        self.total_size += record.rdb_size;

        // Update metadata counters
        if record.expire_at_ms.is_some() {
            self.expired_keys += 1;
        }
        if record.idle_seconds.is_some() {
            self.keys_with_idle += 1;
        }
        if record.freq.is_some() {
            self.keys_with_freq += 1;
        }
    }
}

fn rdb_str_to_string(rdb_str: &RDBStr) -> String {
    match rdb_str {
        RDBStr::Str(bytes) => String::from_utf8_lossy(bytes).to_string(),
        RDBStr::Int(value) => value.to_string(),
    }
}

fn format_expiry(expire_at_ms: Option<u64>) -> String {
    match expire_at_ms {
        Some(ms) => {
            // Convert milliseconds to seconds for OffsetDateTime
            let seconds = (ms / 1000) as i64;
            match time::OffsetDateTime::from_unix_timestamp(seconds) {
                Ok(utc_dt) => {
                    // Try to convert to local timezone, fallback to UTC if failed
                    let dt = utc_dt.to_offset(
                        time::UtcOffset::local_offset_at(utc_dt).unwrap_or(time::UtcOffset::UTC),
                    );

                    // Use RFC 3339 format
                    dt.format(&time::format_description::well_known::Rfc3339)
                        .unwrap_or_else(|_| format!("{ms}ms"))
                }
                Err(_) => format!("{ms}ms"),
            }
        }
        None => "Never".to_string(),
    }
}

fn output_record(record: &Record, record_number: u64) {
    println!(
        "{}\t{record_number}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        record.type_name().to_uppercase(),
        record.db,
        rdb_str_to_string(&record.key),
        record.encoding_name(),
        record.member_count.unwrap_or(0),
        record.rdb_size,
        format_expiry(record.expire_at_ms),
        record.idle_seconds.unwrap_or(0),
        record.freq.unwrap_or(0),
    );
}

fn print_final_stats(
    total_records: u64,
    total_bytes: u64,
    duration: std::time::Duration,
    stats: &RecordStats,
) {
    println!("\n=== Parsing Complete ===");
    println!("Total records processed: {total_records}");
    println!(
        "Total bytes read: {total_bytes} ({:.2} MB)",
        total_bytes as f64 / 1024.0 / 1024.0
    );
    println!(
        "Total RDB size: {} ({:.2} MB)",
        stats.total_size,
        stats.total_size as f64 / 1024.0 / 1024.0
    );
    println!("Processing time: {:.2} seconds", duration.as_secs_f64());
    println!(
        "Records per second: {:.2}",
        total_records as f64 / duration.as_secs_f64()
    );
    println!(
        "Throughput: {:.2} MB/s",
        (total_bytes as f64 / 1024.0 / 1024.0) / duration.as_secs_f64()
    );

    println!("\n=== Record Statistics ===");
    println!("Strings: {}", stats.strings);
    println!("Lists: {}", stats.lists);
    println!("Sets: {}", stats.sets);
    println!("ZSets: {}", stats.zsets);
    println!("Hashes: {}", stats.hashes);
    println!("Streams: {}", stats.streams);
    println!("Modules: {}", stats.modules);

    println!("\n=== Metadata Statistics ===");
    println!("Keys with expiration: {}", stats.expired_keys);
    println!("Keys with idle time: {}", stats.keys_with_idle);
    println!("Keys with frequency: {}", stats.keys_with_freq);
}

fn get_cluster_name(source: &SourceConfig) -> &str {
    match source {
        SourceConfig::RedisStandalone { cluster_name, .. } => cluster_name,
        SourceConfig::RedisSentinel { cluster_name, .. } => cluster_name,
        SourceConfig::RedisCluster { cluster_name, .. } => cluster_name,
        SourceConfig::Codis { cluster_name, .. } => cluster_name.as_deref().unwrap_or("unknown"),
        SourceConfig::RDBFile { cluster_name, .. } => cluster_name,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_expiry() {
        // Test None case
        assert_eq!(format_expiry(None), "Never");

        // Test Some case with a known timestamp
        // 2024-01-01 00:00:00 UTC = 1704067200 seconds = 1704067200000 ms
        let test_ms = 1704067200000u64;
        let result = format_expiry(Some(test_ms));

        // Should be RFC 3339 format (either local timezone or UTC)
        // The exact output depends on local timezone, but should contain the date
        assert!(result.contains("2024-01-01") || result.contains("2023-12-31"));
        assert!(result.contains("T"));
        assert!(result.contains(":"));

        // Test invalid timestamp (should fallback to ms format)
        let invalid_ms = u64::MAX;
        let result = format_expiry(Some(invalid_ms));
        assert!(result.ends_with("ms"));
    }
}
