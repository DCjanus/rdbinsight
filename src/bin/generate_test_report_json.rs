use std::collections::HashMap;

use anyhow::Result;
use bytes::Bytes;
use rdbinsight::report::{
    ReportData,
    querier::{PrefixRecord, TopKeyRecord},
};

// Deterministic pseudo-random number generator for consistent output
struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next(&mut self) -> u64 {
        // Linear congruential generator with well-known constants
        self.state = self.state.wrapping_mul(1103515245).wrapping_add(12345);
        self.state
    }

    fn gen_range(&mut self, min: u64, max: u64) -> u64 {
        min + (self.next() % (max - min + 1))
    }

    fn choose<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        let index = self.next() as usize % items.len();
        &items[index]
    }
}

fn main() -> Result<()> {
    let report_data = generate_test_report_data();

    // Use BTreeMap to ensure deterministic ordering of JSON keys
    let json_value = serde_json::to_value(&report_data)?;

    // Pretty print with 2-space indentation for consistency
    let json_string = serde_json::to_string_pretty(&json_value)?;

    println!("{json_string}");

    Ok(())
}

fn generate_test_report_data() -> ReportData {
    ReportData {
        cluster: "production-redis-cluster".to_string(),
        batch: "2024-12-20T14:30:45.123456Z".to_string(),
        prefix_records: generate_prefix_records(),
        top_keys: generate_top_keys(),
    }
}

fn generate_prefix_records() -> Vec<PrefixRecord> {
    let mut rng = DeterministicRng::new(42); // Fixed seed for consistency
    let mut records = Vec::new();

    // Define test cluster topology
    let instances = [
        ("redis-master-01:6379", 0),
        ("redis-master-02:6379", 1),
        ("redis-slave-01:6379", 0),
        ("redis-cache-01:6380", 0),
    ];

    let data_types = ["string", "hash", "list", "set", "zset", "stream"];

    // Define hierarchical prefix patterns with their base frequencies
    let prefix_patterns = [
        ("", 1.0), // Root level (cluster totals)
        ("user:", 0.4),
        ("user:session:", 0.2),
        ("user:session:web:", 0.12),
        ("user:session:mobile:", 0.08),
        ("user:profile:", 0.15),
        ("user:profile:basic:", 0.08),
        ("user:profile:extended:", 0.07),
        ("cache:", 0.3),
        ("cache:page:", 0.18),
        ("cache:page:home:", 0.1),
        ("cache:page:product:", 0.08),
        ("cache:api:", 0.12),
        ("cache:api:v1:", 0.07),
        ("cache:api:v2:", 0.05),
        ("order:", 0.25),
        ("order:details:", 0.18),
        ("order:details:items:", 0.12),
        ("order:details:shipping:", 0.06),
        ("product:", 0.2),
        ("product:catalog:", 0.12),
        ("product:inventory:", 0.08),
        ("queue:", 0.15),
        ("queue:email:", 0.1),
        ("queue:sms:", 0.05),
        ("log:", 0.1),
        ("log:error:", 0.06),
        ("log:access:", 0.04),
        ("config:", 0.08),
        ("config:app:", 0.05),
        ("config:feature:", 0.03),
        ("session:", 0.12),
        ("session:admin:", 0.08),
        ("auth:", 0.1),
        ("auth:token:", 0.06),
        ("api:", 0.18),
        ("api:response:", 0.12),
        ("api:rate_limit:", 0.06),
        ("backup:", 0.08),
        ("backup:daily:", 0.05),
        ("sync:", 0.06),
        ("sync:user:", 0.04),
    ];

    // Generate records for each instance/db/type combination
    for (instance, db) in instances {
        for data_type in data_types {
            // Calculate base size for this classification
            let base_size = calculate_base_size(&mut rng, instance, data_type);
            let base_count = base_size / 4096; // Assume avg 4KB per key

            for (prefix, frequency) in prefix_patterns {
                // Skip some prefix/type combinations to make it more realistic
                if should_skip_combination(&mut rng, prefix, data_type) {
                    continue;
                }

                let adjusted_size = (base_size as f64 * frequency) as u64;
                let adjusted_count = (base_count as f64 * frequency) as u64;

                // Only include if significant enough
                if adjusted_size > 1000000 {
                    // > 1MB
                    records.push(PrefixRecord {
                        prefix_base64: Bytes::from(prefix.to_string()),
                        instance: instance.to_string(),
                        db,
                        r#type: data_type.to_string(),
                        rdb_size: adjusted_size,
                        key_count: adjusted_count,
                    });
                }
            }
        }
    }

    // Sort by instance, db, type, then by size descending for consistency
    records.sort_by(|a, b| {
        a.instance
            .cmp(&b.instance)
            .then_with(|| a.db.cmp(&b.db))
            .then_with(|| a.r#type.cmp(&b.r#type))
            .then_with(|| b.rdb_size.cmp(&a.rdb_size))
    });

    records
}

fn calculate_base_size(rng: &mut DeterministicRng, instance: &str, data_type: &str) -> u64 {
    // Different instances have different capacity patterns
    let instance_multiplier = match instance {
        "redis-master-01:6379" => 1.0,
        "redis-master-02:6379" => 0.6,
        "redis-slave-01:6379" => 0.4,
        "redis-cache-01:6380" => 0.8,
        _ => 0.5,
    };

    // Different data types have different typical sizes
    let type_base_size = match data_type {
        "string" => 500_000_000, // 500MB
        "hash" => 300_000_000,   // 300MB
        "list" => 150_000_000,   // 150MB
        "set" => 80_000_000,     // 80MB
        "zset" => 120_000_000,   // 120MB
        "stream" => 60_000_000,  // 60MB
        _ => 100_000_000,
    };

    // Add some deterministic variation
    let variation = 0.8 + (rng.gen_range(0, 40) as f64 / 100.0); // 0.8 to 1.2

    (type_base_size as f64 * instance_multiplier * variation) as u64
}

fn should_skip_combination(rng: &mut DeterministicRng, prefix: &str, data_type: &str) -> bool {
    // Some logical combinations that are less likely
    let skip_patterns = [
        ("user:session:", "hash"), // Sessions typically strings
        ("queue:", "set"),         // Queues are typically lists
        ("log:", "zset"),          // Logs typically lists
        ("cache:page:", "stream"), // Page cache typically strings
        ("backup:", "stream"),     // Backups typically strings
        ("auth:token:", "list"),   // Tokens typically strings
    ];

    for (pattern_prefix, pattern_type) in skip_patterns {
        if prefix.starts_with(pattern_prefix) && data_type == pattern_type {
            return rng.gen_range(0, 100) < 70; // 70% chance to skip
        }
    }

    // Random skipping for variety
    rng.gen_range(0, 100) < 15 // 15% chance to skip any combination
}

fn generate_top_keys() -> Vec<TopKeyRecord> {
    let mut rng = DeterministicRng::new(84); // Different seed for top keys
    let mut keys = Vec::new();

    // Define key patterns with their characteristics
    let key_patterns = [
        ("user:session:web:active_users_{}", "hash", 1_500_000, 4000),
        (
            "cache:page:home:popular_products_{}",
            "string",
            1_200_000,
            1,
        ),
        ("leaderboard:daily:user_scores_{}", "zset", 900_000, 8000),
        ("queue:email:high_priority_{}", "list", 600_000, 2000),
        ("tags:product:electronics_{}", "set", 400_000, 1000),
        ("events:user:activity_stream_{}", "stream", 350_000, 600),
        ("order:details:items:bulk_{}", "hash", 300_000, 120),
        ("api:response:v2:catalog_{}", "string", 280_000, 1),
        ("session:admin:dashboard_{}", "string", 250_000, 1),
        ("shopping:cart:mega_{}", "hash", 220_000, 180),
        ("backup:daily:snapshot_{}", "string", 200_000, 1),
        ("metrics:hourly:performance_{}", "string", 180_000, 1),
        ("user:profile:extended:premium_{}", "string", 160_000, 1),
        ("cdn:images:carousel_{}", "string", 140_000, 1),
        ("config:feature:experimental_{}", "string", 120_000, 1),
        ("ranking:product:bestsellers_{}", "zset", 110_000, 400),
        ("log:error:critical_{}", "list", 100_000, 50),
        ("analytics:realtime:behavior_{}", "hash", 90_000, 250),
        ("geo:location:coordinates_{}", "string", 80_000, 1),
        ("admin:settings:global_{}", "hash", 70_000, 60),
    ];

    let instances = [
        "redis-master-01:6379",
        "redis-master-02:6379",
        "redis-slave-01:6379",
        "redis-cache-01:6380",
    ];

    let encodings = HashMap::from([
        ("string", vec!["string:raw", "string:embstr", "string:int"]),
        ("hash", vec!["hash:hashtable", "hash:listpack"]),
        ("list", vec!["list:quicklist", "list:linkedlist"]),
        ("set", vec!["set:hashtable", "set:intset"]),
        ("zset", vec!["zset:skiplist", "zset:listpack"]),
        ("stream", vec!["stream:radix"]),
    ]);

    for (i, (pattern, data_type, base_size, base_members)) in key_patterns.iter().enumerate() {
        let instance = instances[i % instances.len()];
        let db = if instance.contains("master-02") { 1 } else { 0 };

        // Generate a deterministic suffix
        let suffix = generate_key_suffix(&mut rng, i);
        let key_name = pattern.replace("{}", &suffix);

        // Add some size variation
        let size_variation = 0.7 + (rng.gen_range(0, 60) as f64 / 100.0);
        let final_size = (*base_size as f64 * size_variation) as u64;

        let member_variation = 0.8 + (rng.gen_range(0, 40) as f64 / 100.0);
        let final_members = (*base_members as f64 * member_variation) as u64;

        let encoding = rng.choose(&encodings[data_type]);

        // Generate expiration (some keys expire, some don't)
        let expire_at = if rng.gen_range(0, 100) < 60 {
            Some(generate_expiration_time(&mut rng, i))
        } else {
            None
        };

        keys.push(TopKeyRecord {
            key_base64: Bytes::from(key_name),
            rdb_size: final_size,
            member_count: Some(final_members),
            r#type: data_type.to_string(),
            instance: instance.to_string(),
            db,
            encoding: encoding.to_string(),
            expire_at,
        });
    }

    // Sort by size descending for consistency
    keys.sort_by(|a, b| b.rdb_size.cmp(&a.rdb_size));

    keys
}

fn generate_key_suffix(rng: &mut DeterministicRng, base_index: usize) -> String {
    let templates = [
        "12345678901234567890",
        "global_trending",
        "2024_12_20",
        "notifications_queue",
        "categories_mapping",
        "user_987654321",
        "order_202412201430",
        "cached_full",
        "super_admin_dashboard_state",
        "user_123456789",
        "20241220",
        "data_aggregated",
        "features_456789",
        "banner_holiday_collection",
        "toggle_map",
        "electronics_2024_q4",
        "application_errors_buffer",
        "tracking",
        "cache_global",
        "configuration_master",
    ];

    let index = (base_index + rng.gen_range(0, 5) as usize) % templates.len();
    templates[index].to_string()
}

fn generate_expiration_time(rng: &mut DeterministicRng, base_index: usize) -> String {
    let base_times = [
        "2024-12-27T14:30:45.123456Z",
        "2024-12-21T02:30:45.123456Z",
        "2024-12-25T14:30:45.123456Z",
        "2025-01-20T14:30:45.123456Z",
        "2024-12-20T16:30:45.123456Z",
        "2024-12-20T18:30:45.123456Z",
        "2024-12-22T14:30:45.123456Z",
        "2024-12-21T14:30:45.123456Z",
        "2024-12-31T23:59:59.999999Z",
        "2025-01-01T00:00:00.000000Z",
    ];

    let index = (base_index + rng.gen_range(0, 3) as usize) % base_times.len();
    base_times[index].to_string()
}
