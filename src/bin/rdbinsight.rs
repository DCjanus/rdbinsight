use std::{path::PathBuf, time::Instant};

use anyhow::{Context, Result};
use clap::Parser;
use rdbinsight::{
    config::{Config, SourceConfig},
    parser::{
        core::{buffer::Buffer, raw::RDBStr},
        model::Item,
        rdb_file::RDBFileParser,
    },
    source::{RdbSourceConfig, file::Config as FileConfig, standalone::Config as StandaloneConfig},
};
use tokio::io::AsyncReadExt;
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
    mut stream: std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>>,
) -> Result<()> {
    let start_time = Instant::now();
    let mut parser = RDBFileParser::default();
    let mut buffer = Buffer::new(16 * 1024 * 1024); // 16MB buffer
    let mut total_items = 0u64;
    let mut total_bytes_read = 0u64;

    // Statistics counters
    let mut stats = ItemStats::default();

    info!("Starting RDB parsing...");

    loop {
        // Try to parse next item
        match parser.poll_next(&mut buffer) {
            Ok(Some(item)) => {
                total_items += 1;
                stats.update(&item);

                // Output item information to stdout (simulating ClickHouse output)
                output_item(&item, total_items);

                // Log progress every 10000 items
                if total_items % 1000000 == 0 {
                    debug!("Processed {total_items} items");
                }
            }
            Ok(None) => {
                // End of stream
                info!("Reached end of RDB stream");
                break;
            }
            Err(e) => {
                if e.downcast_ref::<rdbinsight::parser::error::NeedMoreData>()
                    .is_some()
                {
                    // Need more data, read from stream
                    let mut read_buf = vec![0u8; 64 * 1024]; // 64KB read buffer
                    match stream.read(&mut read_buf).await {
                        Ok(0) => {
                            // EOF reached
                            buffer.set_finished();
                            info!("Stream finished, processing remaining data...");
                        }
                        Ok(n) => {
                            total_bytes_read += n as u64;
                            buffer
                                .extend(&read_buf[..n])
                                .with_context(|| "Failed to extend buffer")?;
                        }
                        Err(e) => {
                            error!("Failed to read from stream: {e}");
                            return Err(e.into());
                        }
                    }
                } else {
                    error!("Parser error: {e}");
                    return Err(e);
                }
            }
        }
    }

    let duration = start_time.elapsed();

    // Print final statistics
    print_final_stats(total_items, total_bytes_read, duration, &stats);

    Ok(())
}

#[derive(Default)]
struct ItemStats {
    strings: u64,
    lists: u64,
    sets: u64,
    zsets: u64,
    hashes: u64,
    streams: u64,
    modules: u64,
    functions: u64,
    aux_fields: u64,
    db_switches: u64,
    expiries: u64,
    other: u64,
}

impl ItemStats {
    fn update(&mut self, item: &Item) {
        match item {
            Item::StringRecord { .. } => self.strings += 1,
            Item::ListRecord { .. } => self.lists += 1,
            Item::SetRecord { .. } => self.sets += 1,
            Item::ZSetRecord { .. } | Item::ZSet2Record { .. } => self.zsets += 1,
            Item::HashRecord { .. } => self.hashes += 1,
            Item::StreamRecord { .. } => self.streams += 1,
            Item::ModuleRecord { .. } | Item::ModuleAux { .. } => self.modules += 1,
            Item::FunctionRecord { .. } => self.functions += 1,
            Item::Aux { .. } => self.aux_fields += 1,
            Item::SelectDB { .. } => self.db_switches += 1,
            Item::ExpiryMs { .. } => self.expiries += 1,
            _ => self.other += 1,
        }
    }
}

fn rdb_str_to_string(rdb_str: &RDBStr) -> String {
    match rdb_str {
        RDBStr::Str(bytes) => String::from_utf8_lossy(bytes).to_string(),
        RDBStr::Int(value) => value.to_string(),
    }
}

fn output_item(item: &Item, item_number: u64) {
    match item {
        Item::Aux { key, val } => {
            println!(
                "AUX\t{item_number}\t{}\t{}",
                rdb_str_to_string(key),
                rdb_str_to_string(val)
            );
        }
        Item::SelectDB { db } => {
            println!("SELECT_DB\t{item_number}\t{db}");
        }
        Item::ResizeDB {
            table_size,
            ttl_table_size,
        } => {
            println!("RESIZE_DB\t{item_number}\t{table_size}\t{ttl_table_size}");
        }
        Item::StringRecord {
            key,
            rdb_size,
            encoding,
        } => {
            println!(
                "STRING\t{item_number}\t{}\t{rdb_size}\t{encoding:?}",
                rdb_str_to_string(key)
            );
        }
        Item::ListRecord {
            key,
            rdb_size,
            encoding,
            member_count,
        } => {
            println!(
                "LIST\t{item_number}\t{}\t{rdb_size}\t{encoding:?}\t{member_count}",
                rdb_str_to_string(key)
            );
        }
        Item::SetRecord {
            key,
            rdb_size,
            encoding,
            member_count,
        } => {
            println!(
                "SET\t{item_number}\t{}\t{rdb_size}\t{encoding:?}\t{member_count}",
                rdb_str_to_string(key)
            );
        }
        Item::ZSetRecord {
            key,
            rdb_size,
            encoding,
            member_count,
        } => {
            println!(
                "ZSET\t{item_number}\t{}\t{rdb_size}\t{encoding:?}\t{member_count}",
                rdb_str_to_string(key)
            );
        }
        Item::ZSet2Record {
            key,
            rdb_size,
            encoding,
            member_count,
        } => {
            println!(
                "ZSET2\t{item_number}\t{}\t{rdb_size}\t{encoding:?}\t{member_count}",
                rdb_str_to_string(key)
            );
        }
        Item::HashRecord {
            key,
            rdb_size,
            encoding,
            pair_count,
        } => {
            println!(
                "HASH\t{item_number}\t{}\t{rdb_size}\t{encoding:?}\t{pair_count}",
                rdb_str_to_string(key)
            );
        }
        Item::StreamRecord {
            key,
            rdb_size,
            encoding,
            message_count,
        } => {
            println!(
                "STREAM\t{item_number}\t{}\t{rdb_size}\t{encoding:?}\t{message_count}",
                rdb_str_to_string(key)
            );
        }
        Item::ModuleRecord { key, rdb_size } => {
            println!(
                "MODULE\t{item_number}\t{}\t{rdb_size}",
                rdb_str_to_string(key)
            );
        }
        Item::ModuleAux { rdb_size } => {
            println!("MODULE_AUX\t{item_number}\t{rdb_size}");
        }
        Item::FunctionRecord { rdb_size } => {
            println!("FUNCTION\t{item_number}\t{rdb_size}");
        }
        Item::ExpiryMs { expire_at_ms } => {
            println!("EXPIRY_MS\t{item_number}\t{expire_at_ms}");
        }
        Item::Idle { idle_seconds } => {
            println!("IDLE\t{item_number}\t{idle_seconds}");
        }
        Item::Freq { freq } => {
            println!("FREQ\t{item_number}\t{freq}");
        }
        Item::SlotInfo {
            slot_id,
            slot_size,
            expires_slot_size,
        } => {
            println!("SLOT_INFO\t{item_number}\t{slot_id}\t{slot_size}\t{expires_slot_size}");
        }
    }
}

fn print_final_stats(
    total_items: u64,
    total_bytes: u64,
    duration: std::time::Duration,
    stats: &ItemStats,
) {
    println!("\n=== Parsing Complete ===");
    println!("Total items processed: {total_items}");
    println!(
        "Total bytes read: {total_bytes} ({:.2} MB)",
        total_bytes as f64 / 1024.0 / 1024.0
    );
    println!("Processing time: {:.2} seconds", duration.as_secs_f64());
    println!(
        "Items per second: {:.2}",
        total_items as f64 / duration.as_secs_f64()
    );
    println!(
        "Throughput: {:.2} MB/s",
        (total_bytes as f64 / 1024.0 / 1024.0) / duration.as_secs_f64()
    );

    println!("\n=== Item Statistics ===");
    println!("Strings: {}", stats.strings);
    println!("Lists: {}", stats.lists);
    println!("Sets: {}", stats.sets);
    println!("ZSets: {}", stats.zsets);
    println!("Hashes: {}", stats.hashes);
    println!("Streams: {}", stats.streams);
    println!("Modules: {}", stats.modules);
    println!("Functions: {}", stats.functions);
    println!("Aux fields: {}", stats.aux_fields);
    println!("DB switches: {}", stats.db_switches);
    println!("Expiries: {}", stats.expiries);
    println!("Other: {}", stats.other);
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
