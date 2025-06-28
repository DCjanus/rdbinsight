use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use redis::{Client, InfoDict, aio::MultiplexedConnection};
use tracing::{debug, error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser)]
#[command(name = "fill_redis_memory")]
#[command(about = "Fill Redis instance with random data until reaching target memory size")]
struct Cli {
    /// Redis URL (e.g., redis://localhost:6379, redis://user:pass@host:port/db)
    #[arg(help = "Redis connection URL")]
    redis_url: String,

    /// Target memory size in human readable format (e.g., 100M, 1G, 2.5G, 100MiB, 1GiB)
    #[arg(help = "Target memory size (e.g., 100M, 1G, 2.5G, 100MiB, 1GiB)")]
    target_memory: String,

    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,
}

// Fixed value size for all keys (16 bytes)
const VALUE_SIZE: usize = 64;

/// Custom memory size utilities using 1024 base (binary units)
mod memory_size {
    use anyhow::{Result, anyhow};

    const UNITS: &[(&str, u64)] = &[
        ("B", 1),
        ("K", 1024),
        ("KB", 1024),
        ("KIB", 1024),
        ("M", 1024 * 1024),
        ("MB", 1024 * 1024),
        ("MIB", 1024 * 1024),
        ("G", 1024 * 1024 * 1024),
        ("GB", 1024 * 1024 * 1024),
        ("GIB", 1024 * 1024 * 1024),
        ("T", 1024_u64.pow(4)),
        ("TB", 1024_u64.pow(4)),
        ("TIB", 1024_u64.pow(4)),
        ("P", 1024_u64.pow(5)),
        ("PB", 1024_u64.pow(5)),
        ("PIB", 1024_u64.pow(5)),
    ];

    /// Parse memory size string to bytes using 1024 base
    pub fn parse(input: &str) -> Result<u64> {
        let input = input.trim().to_uppercase();

        if input.is_empty() {
            return Err(anyhow!("Empty memory size string"));
        }

        // Handle pure numbers (assume bytes)
        if let Ok(bytes) = input.parse::<u64>() {
            return Ok(bytes);
        }

        // Find the unit part
        let (number_str, unit_str) = split_number_unit(&input)?;

        // Parse the number part
        let number: f64 = number_str
            .parse()
            .map_err(|_| anyhow!("Invalid number format: '{}'", number_str))?;

        if number < 0.0 {
            return Err(anyhow!("Memory size cannot be negative"));
        }

        // Find matching unit
        let unit_multiplier = UNITS
            .iter()
            .find(|(unit, _)| *unit == unit_str)
            .map(|(_, multiplier)| *multiplier)
            .ok_or_else(|| anyhow!("Unknown unit: '{}'", unit_str))?;

        let bytes = (number * unit_multiplier as f64) as u64;
        Ok(bytes)
    }

    /// Format bytes to human readable string using 1024 base
    pub fn format(bytes: u64) -> String {
        if bytes == 0 {
            return "0 B".to_string();
        }

        let units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
        let mut size = bytes as f64;
        let mut unit_index = 0;

        while size >= 1024.0 && unit_index < units.len() - 1 {
            size /= 1024.0;
            unit_index += 1;
        }

        if unit_index == 0 {
            format!("{} {}", bytes, units[unit_index])
        } else if size.fract() == 0.0 {
            format!("{:.0} {}", size, units[unit_index])
        } else {
            format!("{:.1} {}", size, units[unit_index])
        }
    }

    fn split_number_unit(input: &str) -> Result<(String, String)> {
        let mut number_end = 0;

        for (i, ch) in input.char_indices() {
            if ch.is_ascii_digit() || ch == '.' {
                number_end = i + 1;
            } else {
                break;
            }
        }

        if number_end == 0 {
            return Err(anyhow!("No number found in input: '{}'", input));
        }

        let number_part = input[..number_end].to_string();
        let unit_part = input[number_end..].trim().to_string();

        if unit_part.is_empty() {
            Ok((number_part, "B".to_string()))
        } else {
            Ok((number_part, unit_part))
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_parse() {
            assert_eq!(parse("1024").unwrap(), 1024);
            assert_eq!(parse("1K").unwrap(), 1024);
            assert_eq!(parse("1KB").unwrap(), 1024);
            assert_eq!(parse("1KiB").unwrap(), 1024);
            assert_eq!(parse("1M").unwrap(), 1024 * 1024);
            assert_eq!(
                parse("1.5G").unwrap(),
                (1.5 * 1024.0 * 1024.0 * 1024.0) as u64
            );
        }

        #[test]
        fn test_format() {
            assert_eq!(format(0), "0 B");
            assert_eq!(format(1024), "1 KiB");
            assert_eq!(format(1536), "1.5 KiB");
            assert_eq!(format(1024 * 1024), "1 MiB");
        }
    }
}

// Helper function to format bytes using binary units (1024 base)
fn format_binary_size(bytes: u64) -> String {
    memory_size::format(bytes)
}

// Helper function to parse memory size using 1024 base
fn parse_memory_size(input: &str) -> Result<u64> {
    memory_size::parse(input)
}

// Calculate optimal batch size based on remaining memory to fill
fn calculate_batch_size(current_memory: u64, target_memory: u64) -> u64 {
    let remaining_bytes = target_memory.saturating_sub(current_memory);

    let estimated_bytes_per_key = VALUE_SIZE as u64 + 32;

    let remaining_keys = remaining_bytes / estimated_bytes_per_key;

    (remaining_keys / 100).clamp(100, 1_000_000)
}

struct MemoryFiller {
    conn: MultiplexedConnection,
    target_bytes: u64,
    prefix: String,
    value_data: String,
    verbose: bool,
    total_keys_written: u64,
    batch_count: u64,
    last_report_time: Instant,
}

impl MemoryFiller {
    fn new(conn: MultiplexedConnection, target_bytes: u64, cli: &Cli) -> Self {
        Self {
            conn,
            target_bytes,
            prefix: "fill_mem".to_string(),
            value_data: generate_fixed_value(VALUE_SIZE),
            verbose: cli.verbose,
            total_keys_written: 0,
            batch_count: 0,
            last_report_time: Instant::now(),
        }
    }

    async fn fill_memory(&mut self) -> Result<()> {
        info!("Starting memory fill process...");
        info!("Using fixed value size: {} bytes", VALUE_SIZE);

        // Main processing loop
        loop {
            // Check current memory usage
            let current_memory = self.get_current_memory().await?;

            if self.batch_count == 0 {
                info!(
                    "Initial memory usage: {}",
                    format_binary_size(current_memory)
                );
            }

            // Check if target is reached
            if current_memory >= self.target_bytes {
                if self.batch_count == 0 {
                    info!("No data writing needed. Exiting.");
                } else {
                    self.report_completion(current_memory);
                }
                break;
            }

            // Calculate optimal batch size for current situation
            let batch_size = calculate_batch_size(current_memory, self.target_bytes);

            // Write a batch
            self.batch_count += 1;

            match self.process_single_batch(batch_size).await {
                Ok(written) => {
                    self.total_keys_written += written;
                    debug!(
                        "Wrote {} keys in batch {} (total: {}) - batch size: {}",
                        written, self.batch_count, self.total_keys_written, batch_size
                    );

                    if self.should_report_progress() {
                        self.report_progress(current_memory, batch_size);
                    }
                }
                Err(e) => {
                    error!("Failed to write batch {}: {}", self.batch_count, e);
                    error!("Exiting due to write failure");
                    return Err(e);
                }
            }
        }

        info!(
            "Memory fill completed. Total keys written: {} in {} batches",
            self.total_keys_written, self.batch_count
        );
        Ok(())
    }

    async fn get_current_memory(&mut self) -> Result<u64> {
        let info: InfoDict = redis::cmd("INFO")
            .arg("memory")
            .query_async(&mut self.conn)
            .await
            .with_context(|| "Failed to get Redis INFO")?;
        get_memory_from_info_dict(&info)
    }

    fn should_report_progress(&self) -> bool {
        self.verbose || self.last_report_time.elapsed() >= Duration::from_secs(1)
    }

    fn report_progress(&mut self, current_memory: u64, batch_size: u64) {
        let progress = (current_memory as f64 / self.target_bytes as f64) * 100.0;
        info!(
            "Batch {}: Memory usage: {} / {} ({:.1}%) - {} keys written (batch size: {})",
            self.batch_count,
            format_binary_size(current_memory),
            format_binary_size(self.target_bytes),
            progress,
            self.total_keys_written,
            batch_size
        );
        self.last_report_time = Instant::now();
    }

    fn report_completion(&self, current_memory: u64) {
        let progress = (current_memory as f64 / self.target_bytes as f64) * 100.0;
        info!("Target memory size reached!");
        info!(
            "Final: Memory usage: {} / {} ({:.1}%) - {} keys written in {} batches",
            format_binary_size(current_memory),
            format_binary_size(self.target_bytes),
            progress,
            self.total_keys_written,
            self.batch_count
        );
    }

    async fn process_single_batch(&mut self, batch_size: u64) -> Result<u64> {
        let mut pipe = redis::pipe();

        // Add all SET commands to the pipeline with random keys
        for _ in 0..batch_size {
            let random_id: u64 = rand::random();
            let key = format!("{}:{}", self.prefix, random_id);
            pipe.set(&key, &self.value_data).ignore();
        }

        // Execute the pipeline
        let _: () = pipe
            .query_async(&mut self.conn)
            .await
            .with_context(|| "Failed to execute batch write")?;

        Ok(batch_size)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging
    let level = if cli.verbose {
        tracing::level_filters::LevelFilter::DEBUG
    } else {
        tracing::level_filters::LevelFilter::INFO
    };
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .with(level)
        .init();

    // Parse target memory size using custom parser for binary units
    let target_bytes = parse_memory_size(&cli.target_memory)?;

    info!("Starting Redis memory fill tool");
    info!("Redis URL: {}", cli.redis_url);
    info!(
        "Target memory: {} ({} bytes)",
        format_binary_size(target_bytes),
        target_bytes
    );
    info!("Value size: {} bytes (fixed)", VALUE_SIZE);

    // Connect to Redis
    let client = Client::open(cli.redis_url.as_str())
        .with_context(|| format!("Failed to create Redis client for URL: {}", cli.redis_url))?;

    let conn = client
        .get_multiplexed_async_connection()
        .await
        .with_context(|| "Failed to connect to Redis")?;

    // Test connection
    let _: String = redis::cmd("PING")
        .query_async(&mut conn.clone())
        .await
        .with_context(|| "Failed to ping Redis server")?;
    info!("Successfully connected to Redis");

    // Start filling memory
    let mut filler = MemoryFiller::new(conn, target_bytes, &cli);
    filler.fill_memory().await
}

fn get_memory_from_info_dict(info: &InfoDict) -> Result<u64> {
    info.get("used_memory")
        .ok_or_else(|| anyhow!("used_memory not found in INFO output"))
}

fn generate_fixed_value(size: usize) -> String {
    // Generate a fixed string of the specified size
    "x".repeat(size)
}
