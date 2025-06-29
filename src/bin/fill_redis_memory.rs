use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use rand::Rng;
use redis::{Client, InfoDict, aio::MultiplexedConnection};
use tracing::{debug, error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser)]
#[command(name = "fill_redis_memory")]
#[command(about = "Fill Redis instance with various data types until reaching target memory size")]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum DataType {
    String,
    Hash,
    List,
    Set,
    ZSet,
    Stream,
}

impl DataType {
    fn as_str(&self) -> &'static str {
        match self {
            DataType::String => "string",
            DataType::Hash => "hash",
            DataType::List => "list",
            DataType::Set => "set",
            DataType::ZSet => "zset",
            DataType::Stream => "stream",
        }
    }
}

// Fixed data type distribution
const DATA_TYPE_WEIGHTS: &[(DataType, u8)] = &[
    (DataType::String, 40),
    (DataType::Hash, 20),
    (DataType::List, 20),
    (DataType::Set, 10),
    (DataType::ZSet, 5),
    (DataType::Stream, 5),
];

struct DataTypeDistribution;

impl DataTypeDistribution {
    fn select_type(&self, rng: &mut impl Rng) -> DataType {
        let rand_val = rng.random_range(0..100);
        let mut cumulative = 0;

        for (data_type, weight) in DATA_TYPE_WEIGHTS {
            cumulative += weight;
            if rand_val < cumulative {
                return *data_type;
            }
        }

        // Fallback
        DataType::String
    }
}

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
    let estimated_bytes_per_key = 200; // Average across different data types
    let remaining_keys = remaining_bytes / estimated_bytes_per_key;
    let raw_batch_size = (remaining_keys / 100).clamp(10, 1_000_000);

    // Round to nearest power of 10
    round_to_power_of_10(raw_batch_size)
}

// Round a number to the nearest power of 10
fn round_to_power_of_10(n: u64) -> u64 {
    if n == 0 {
        return 10;
    }

    // Find the power of 10 that n falls between
    let log10 = (n as f64).log10();
    let lower_power = 10_u64.pow(log10.floor() as u32);
    let upper_power = 10_u64.pow(log10.ceil() as u32);

    // Choose the closer one
    if n - lower_power <= upper_power - n {
        lower_power
    } else {
        upper_power
    }
}

struct MemoryFiller {
    conn: MultiplexedConnection,
    target_bytes: u64,
    prefix: String,
    verbose: bool,
    total_keys_written: u64,
    batch_count: u64,
    last_report_time: Instant,
    distribution: DataTypeDistribution,
    type_counters: HashMap<DataType, u64>,
}

impl MemoryFiller {
    fn new(conn: MultiplexedConnection, target_bytes: u64, cli: &Cli) -> Self {
        let distribution = DataTypeDistribution;
        let mut type_counters = HashMap::new();

        // Initialize counters for all data types
        for (data_type, _) in DATA_TYPE_WEIGHTS {
            type_counters.insert(*data_type, 0);
        }

        Self {
            conn,
            target_bytes,
            prefix: "fill_mem".to_string(),
            verbose: cli.verbose,
            total_keys_written: 0,
            batch_count: 0,
            last_report_time: Instant::now(),
            distribution,
            type_counters,
        }
    }

    async fn fill_memory(&mut self) -> Result<()> {
        info!("Starting memory fill process with multiple data types...");
        self.log_distribution_info();

        // Check and display Redis memory configuration
        self.log_redis_memory_config().await?;

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

        self.report_final_stats();
        Ok(())
    }

    fn log_distribution_info(&self) {
        info!("Data type distribution:");
        for (data_type, weight) in DATA_TYPE_WEIGHTS {
            info!("  {}: {}%", data_type.as_str(), weight);
        }
    }

    async fn log_redis_memory_config(&mut self) -> Result<()> {
        let info: InfoDict = redis::cmd("INFO")
            .arg("memory")
            .query_async(&mut self.conn)
            .await
            .with_context(|| "Failed to get Redis INFO for memory config")?;

        let current_memory = get_memory_from_info_dict(&info)?;
        let max_memory = get_maxmemory_from_info_dict(&info)?;

        info!("Redis memory configuration:");
        info!(
            "  Current memory usage: {}",
            format_binary_size(current_memory)
        );

        if max_memory > 0 {
            info!("  Max memory limit: {}", format_binary_size(max_memory));
            let usage_percentage = (current_memory as f64 / max_memory as f64) * 100.0;
            info!("  Memory usage: {:.1}%", usage_percentage);

            // Get memory policy if available
            if let Some(policy) = info.get::<String>("maxmemory_policy") {
                info!("  Eviction policy: {}", policy);
            }

            // Adjust target if it exceeds max memory
            if self.target_bytes > max_memory {
                info!(
                    "  Target memory ({}) exceeds maxmemory limit, adjusting to {}",
                    format_binary_size(self.target_bytes),
                    format_binary_size(max_memory)
                );
                self.target_bytes = max_memory;
            }
        } else {
            info!("  Max memory limit: No limit set");
        }

        Ok(())
    }

    async fn get_current_memory(&mut self) -> Result<u64> {
        let info: InfoDict = redis::cmd("INFO")
            .arg("memory")
            .query_async(&mut self.conn)
            .await
            .with_context(|| "Failed to get Redis INFO")?;

        let current_memory = get_memory_from_info_dict(&info)?;
        let max_memory = get_maxmemory_from_info_dict(&info)?;

        // Check if Redis has a memory limit configured and adjust target if needed
        if max_memory > 0 && self.target_bytes > max_memory {
            debug!(
                "Target memory size ({}) exceeds Redis maxmemory limit ({}). \
                 Adjusting target to maxmemory limit.",
                format_binary_size(self.target_bytes),
                format_binary_size(max_memory)
            );
            self.target_bytes = max_memory;
        }

        // Warn if we're getting close to the limit
        if max_memory > 0 && current_memory > max_memory * 9 / 10 {
            debug!(
                "Current memory usage ({}) is approaching Redis maxmemory limit ({})",
                format_binary_size(current_memory),
                format_binary_size(max_memory)
            );
        }

        Ok(current_memory)
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

    fn report_final_stats(&self) {
        info!(
            "Memory fill completed. Total keys written: {} in {} batches",
            self.total_keys_written, self.batch_count
        );
        info!("Keys by data type:");
        for (data_type, count) in &self.type_counters {
            if *count > 0 {
                let percentage = (*count as f64 / self.total_keys_written as f64) * 100.0;
                info!("  {}: {} ({:.1}%)", data_type.as_str(), count, percentage);
            }
        }
    }

    async fn process_single_batch(&mut self, batch_size: u64) -> Result<u64> {
        let mut pipe = redis::pipe();
        let mut rng = rand::rng();

        // Add commands for different data types to the pipeline
        for _ in 0..batch_size {
            let data_type = self.distribution.select_type(&mut rng);
            let random_id: u64 = rng.random();
            let key = format!("{}:{}:{}", self.prefix, data_type.as_str(), random_id);

            self.add_data_to_pipeline(&mut pipe, &key, data_type, &mut rng);

            // Update counter
            *self.type_counters.get_mut(&data_type).unwrap() += 1;
        }

        // Execute the pipeline
        let _: () = pipe
            .query_async(&mut self.conn)
            .await
            .with_context(|| "Failed to execute batch write")?;

        Ok(batch_size)
    }

    fn add_data_to_pipeline(
        &self,
        pipe: &mut redis::Pipeline,
        key: &str,
        data_type: DataType,
        rng: &mut impl Rng,
    ) {
        match data_type {
            DataType::String => {
                let value = format!("value_{}", rng.random::<u32>());
                pipe.set(key, value).ignore();
            }
            DataType::Hash => {
                let field_count = rng.random_range(1..=16);
                for i in 0..field_count {
                    pipe.hset(
                        key,
                        format!("field{}", i),
                        format!("value_{}", rng.random::<u32>()),
                    )
                    .ignore();
                }
            }
            DataType::List => {
                let item_count = rng.random_range(1..=16);
                for i in 0..item_count {
                    pipe.lpush(key, format!("item_{}_{}", i, rng.random::<u16>()))
                        .ignore();
                }
            }
            DataType::Set => {
                let member_count = rng.random_range(1..=16);
                for i in 0..member_count {
                    pipe.sadd(key, format!("member_{}_{}", i, rng.random::<u16>()))
                        .ignore();
                }
            }
            DataType::ZSet => {
                let member_count = rng.random_range(1..=16);
                for i in 0..member_count {
                    let score = rng.random::<f64>() * 100.0;
                    let member = format!("player_{}_{}", i, rng.random::<u16>());
                    pipe.zadd(key, member, score).ignore();
                }
            }
            DataType::Stream => {
                let entry_count = rng.random_range(1..=16);
                for i in 0..entry_count {
                    let mut cmd = redis::cmd("XADD");
                    cmd.arg(key)
                        .arg("*")
                        .arg("field1")
                        .arg(format!("value_{}_{}", i, rng.random::<u16>()))
                        .arg("field2")
                        .arg(format!("value_{}_{}", i, rng.random::<u16>()));
                    pipe.add_command(cmd).ignore();
                }
            }
        }
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

    info!("Starting Redis memory fill tool with multiple data types");
    info!("Redis URL: {}", cli.redis_url);
    info!(
        "Target memory: {} ({} bytes)",
        format_binary_size(target_bytes),
        target_bytes
    );

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

fn get_maxmemory_from_info_dict(info: &InfoDict) -> Result<u64> {
    // maxmemory returns 0 if no limit is set
    Ok(info.get("maxmemory").unwrap_or(0))
}
