//! Generate test report JSON with realistic, diverse dataset for testing report functionality.
//! Creates individual keys as single source of truth, then derives all aggregate views.

use std::collections::{BTreeMap, HashMap};

use anyhow::Result;
use bytes::Bytes;
use crc::{CRC_32_ISO_HDLC, Crc};
use rdbinsight::report::model::{
    BigKey, ClusterIssues, DbAggregate, InstanceAggregate, PrefixAggregate, ReportData,
    TopKeyRecord, TypeAggregate,
};

// Deterministic RNG using LCG to ensure consistent output
struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_f64(&mut self) -> f64 {
        self.next() as f64 / u64::MAX as f64
    }

    fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_mul(6364136223846793005).wrapping_add(1);
        self.state
    }

    fn gen_range(&mut self, min: u64, max: u64) -> u64 {
        if min >= max {
            return min;
        }
        min + (self.next() % (max - min))
    }

    fn choose<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        let index = self.next() as usize % items.len();
        &items[index]
    }
}

#[derive(Debug, Clone)]
struct KeyInfo {
    key: Bytes,
    prefix: String,
    r#type: String,
    rdb_size: u64,
    member_count: Option<u64>,
    encoding: String,
    expire_at: Option<String>,
    instance: String,
    db: u32,
}

struct KeyProfile {
    prefix: &'static str,
    data_type: &'static str,
    frequency: f64,
    size_range: (u64, u64),
    member_count_range: Option<(u64, u64)>,
    expiration_chance: f64,
    encodings: &'static [&'static str],
}

const RNG_SEED: u64 = 2024;
const TOTAL_KEYS_TO_GENERATE: usize = 50_000;
const TOP_N_COUNT: usize = 20;

const INSTANCES: &[(&str, u32)] = &[
    ("redis-m-1:6379", 0),
    ("redis-m-2:6379", 0),
    ("redis-m-3:6379", 0),
    ("redis-r-1-1:6379", 0),
    ("redis-r-2-1:6379", 0),
    ("redis-r-3-1:6379", 0),
    ("redis-cache-1:6380", 1),
    ("redis-cache-2:6380", 1),
    ("redis-special:6381", 2),
];

const WORDS_FOR_KEYS: &[&str] = &[
    "user",
    "profile",
    "session",
    "cache",
    "product",
    "order",
    "detail",
    "item",
    "id",
    "data",
    "image",
    "config",
    "setting",
    "log",
    "error",
    "queue",
    "task",
    "job",
    "status",
    "lock",
    "leaderboard",
    "score",
    "rank",
    "metrics",
    "stats",
    "event",
    "stream",
    "message",
    "auth",
];

// Key profiles define shape and distribution of keys in dataset
const KEY_PROFILES: &[KeyProfile] = &[
    KeyProfile {
        prefix: "user:session:web:",
        data_type: "string",
        frequency: 0.12,
        size_range: (50, 500),
        member_count_range: None,
        expiration_chance: 0.95,
        encodings: &["raw", "embstr"],
    },
    KeyProfile {
        prefix: "user:session:api:",
        data_type: "string",
        frequency: 0.12,
        size_range: (100, 800),
        member_count_range: None,
        expiration_chance: 0.95,
        encodings: &["raw", "embstr"],
    },
    KeyProfile {
        prefix: "cache:page:desktop:",
        data_type: "string",
        frequency: 0.08,
        size_range: (5_000, 150_000),
        member_count_range: None,
        expiration_chance: 0.8,
        encodings: &["raw"],
    },
    KeyProfile {
        prefix: "cache:page:mobile:",
        data_type: "string",
        frequency: 0.08,
        size_range: (1_000, 80_000),
        member_count_range: None,
        expiration_chance: 0.8,
        encodings: &["raw"],
    },
    KeyProfile {
        prefix: "user:profile:",
        data_type: "hash",
        frequency: 0.12,
        size_range: (500, 8_000),
        member_count_range: Some((5, 50)),
        expiration_chance: 0.0,
        encodings: &["listpack", "hashtable"],
    },
    KeyProfile {
        prefix: "product:catalog:full:",
        data_type: "hash",
        frequency: 0.02,
        size_range: (20_000, 500_000),
        member_count_range: Some((20, 200)),
        expiration_chance: 0.0,
        encodings: &["hashtable"],
    },
    KeyProfile {
        prefix: "product:catalog:summary:",
        data_type: "hash",
        frequency: 0.03,
        size_range: (2_000, 50_000),
        member_count_range: Some((5, 50)),
        expiration_chance: 0.1,
        encodings: &["listpack", "hashtable"],
    },
    KeyProfile {
        prefix: "leaderboard:daily:",
        data_type: "zset",
        frequency: 0.04,
        size_range: (1_000, 50_000),
        member_count_range: Some((50, 2_000)),
        expiration_chance: 1.0,
        encodings: &["listpack", "skiplist"],
    },
    KeyProfile {
        prefix: "leaderboard:weekly:",
        data_type: "zset",
        frequency: 0.04,
        size_range: (5_000, 250_000),
        member_count_range: Some((100, 10_000)),
        expiration_chance: 0.1,
        encodings: &["skiplist"],
    },
    KeyProfile {
        prefix: "queue:jobs:high_prio:",
        data_type: "list",
        frequency: 0.03,
        size_range: (10_000, 800_000),
        member_count_range: Some((100, 5_000)),
        expiration_chance: 0.0,
        encodings: &["quicklist"],
    },
    KeyProfile {
        prefix: "queue:jobs:low_prio:",
        data_type: "list",
        frequency: 0.03,
        size_range: (100, 100_000),
        member_count_range: Some((1, 1_000)),
        expiration_chance: 0.0,
        encodings: &["quicklist"],
    },
    KeyProfile {
        prefix: "social:followers:",
        data_type: "set",
        frequency: 0.06,
        size_range: (1_000, 400_000),
        member_count_range: Some((10, 20_000)),
        expiration_chance: 0.0,
        encodings: &["intset", "hashtable"],
    },
    KeyProfile {
        prefix: "events:stream:raw:",
        data_type: "stream",
        frequency: 0.04,
        size_range: (100_000, 2_000_000),
        member_count_range: Some((100, 5_000)),
        expiration_chance: 0.0,
        encodings: &["listpacks"],
    },
    KeyProfile {
        prefix: "",
        data_type: "string",
        frequency: 0.03,
        size_range: (10, 1000),
        member_count_range: None,
        expiration_chance: 0.5,
        encodings: &["raw", "embstr"],
    },
    KeyProfile {
        prefix: "tmp:",
        data_type: "string",
        frequency: 0.01,
        size_range: (50, 5000),
        member_count_range: None,
        expiration_chance: 0.9,
        encodings: &["raw", "embstr"],
    },
    KeyProfile {
        prefix: "config:feature:",
        data_type: "string",
        frequency: 0.005,
        size_range: (20, 200),
        member_count_range: None,
        expiration_chance: 0.1,
        encodings: &["embstr"],
    },
    KeyProfile {
        prefix: "rate:limit:",
        data_type: "string",
        frequency: 0.02,
        size_range: (10, 100),
        member_count_range: None,
        expiration_chance: 0.95,
        encodings: &["int"],
    },
    KeyProfile {
        prefix: "metrics:hourly:",
        data_type: "hash",
        frequency: 0.015,
        size_range: (500, 10000),
        member_count_range: Some((10, 100)),
        expiration_chance: 0.8,
        encodings: &["listpack"],
    },
    KeyProfile {
        prefix: "lock:resource:",
        data_type: "string",
        frequency: 0.01,
        size_range: (20, 150),
        member_count_range: None,
        expiration_chance: 0.99,
        encodings: &["embstr"],
    },
    KeyProfile {
        prefix: "job:state:",
        data_type: "hash",
        frequency: 0.02,
        size_range: (200, 2000),
        member_count_range: Some((5, 30)),
        expiration_chance: 0.7,
        encodings: &["listpack", "hashtable"],
    },
    KeyProfile {
        prefix: "experiment:",
        data_type: "set",
        frequency: 0.008,
        size_range: (1000, 50000),
        member_count_range: Some((100, 5000)),
        expiration_chance: 0.3,
        encodings: &["intset", "hashtable"],
    },
    KeyProfile {
        prefix: "Legacy_Data:",
        data_type: "hash",
        frequency: 0.005,
        size_range: (500, 20000),
        member_count_range: Some((5, 100)),
        expiration_chance: 0.1,
        encodings: &["hashtable"],
    },
    KeyProfile {
        prefix: "integration:webhook:",
        data_type: "list",
        frequency: 0.01,
        size_range: (1000, 100000),
        member_count_range: Some((10, 1000)),
        expiration_chance: 0.8,
        encodings: &["quicklist"],
    },
    KeyProfile {
        prefix: "game:daily:",
        data_type: "zset",
        frequency: 0.008,
        size_range: (2000, 80000),
        member_count_range: Some((50, 3000)),
        expiration_chance: 0.9,
        encodings: &["listpack", "skiplist"],
    },
    KeyProfile {
        prefix: "notify:pending:",
        data_type: "list",
        frequency: 0.012,
        size_range: (500, 50000),
        member_count_range: Some((5, 500)),
        expiration_chance: 0.85,
        encodings: &["quicklist"],
    },
    KeyProfile {
        prefix: "analytics:daily:",
        data_type: "hash",
        frequency: 0.01,
        size_range: (2000, 100000),
        member_count_range: Some((20, 500)),
        expiration_chance: 0.3,
        encodings: &["listpack", "hashtable"],
    },
];

fn main() -> Result<()> {
    let mut rng = DeterministicRng::new(RNG_SEED);
    let mut keys = generate_all_keys(&mut rng);

    let report_data = ReportData {
        cluster: "production-simulated-cluster".to_string(),
        batch: "2024-12-20T14:30:45.123456Z".to_string(),
        db_aggregates: aggregate_by_db(&keys),
        type_aggregates: aggregate_by_type(&keys),
        instance_aggregates: aggregate_by_instance(&keys),
        top_prefixes: aggregate_top_prefixes(&keys),
        top_keys: select_top_keys(&mut keys),
        cluster_issues: ClusterIssues {
            big_keys: vec![
                // String type big key - largest string (5MB)
                BigKey {
                    key: Bytes::from("cache:page:desktop:home:index.html:12345"),
                    instance: "redis-cache-1:6380".to_string(),
                    db: 1,
                    r#type: "string".to_string(),
                    rdb_size: 1024 * 1024 * 2 + 42,
                },
                // List type big key - largest list (8MB)
                BigKey {
                    key: Bytes::from("queue:jobs:high_prio:batch-processing:67890"),
                    instance: "redis-m-2:6379".to_string(),
                    db: 0,
                    r#type: "list".to_string(),
                    rdb_size: 1024 * 1024 * 1024 * 3 + 42,
                },
            ],
            codis_slot_skew: false,
            redis_cluster_slot_skew: false,
        },
    };

    let json_value = serde_json::to_value(&report_data)?;
    let json_string = serde_json::to_string_pretty(&json_value)?;
    println!("{json_string}");

    Ok(())
}

fn generate_all_keys(rng: &mut DeterministicRng) -> Vec<KeyInfo> {
    let mut keys = Vec::with_capacity(TOTAL_KEYS_TO_GENERATE);
    let cumulative_freq: Vec<f64> = KEY_PROFILES
        .iter()
        .map(|p| p.frequency)
        .scan(0.0, |acc, f| {
            *acc += f;
            Some(*acc)
        })
        .collect();

    for i in 0..TOTAL_KEYS_TO_GENERATE {
        let profile = select_profile(rng, &cumulative_freq);
        let (key_name, key_bytes) = generate_key_name(rng, profile, i);
        let (instance, db) = assign_to_instance(&key_bytes);

        // Make a few keys non-utf8
        let final_key_bytes = if i % 1000 == 123 {
            let mut b = key_name.into_bytes();
            b.extend_from_slice(b"\xff\xfe\xfd");
            Bytes::from(b)
        } else {
            key_bytes
        };

        keys.push(KeyInfo {
            key: final_key_bytes,
            prefix: profile.prefix.to_string(),
            r#type: profile.data_type.to_string(),
            rdb_size: rng.gen_range(profile.size_range.0, profile.size_range.1),
            member_count: profile
                .member_count_range
                .map(|(min, max)| rng.gen_range(min, max)),
            encoding: rng.choose(profile.encodings).to_string(),
            expire_at: if rng.next_f64() < profile.expiration_chance {
                Some("2025-01-15T10:00:00.000000Z".to_string())
            } else {
                None
            },
            instance: instance.to_string(),
            db,
        });
    }
    keys
}

fn aggregate_by_db(keys: &[KeyInfo]) -> Vec<DbAggregate> {
    let mut agg: HashMap<u32, (u64, u64)> = HashMap::new();
    for key in keys {
        let entry = agg.entry(key.db).or_default();
        entry.0 += 1;
        entry.1 += key.rdb_size;
    }
    let mut result: Vec<DbAggregate> = agg
        .into_iter()
        .map(|(db, (key_count, total_size))| DbAggregate {
            db: db.into(),
            key_count,
            total_size,
        })
        .collect();
    result.sort_by_key(|d| d.db);
    result
}

fn aggregate_by_type(keys: &[KeyInfo]) -> Vec<TypeAggregate> {
    let mut agg: HashMap<String, (u64, u64)> = HashMap::new();
    for key in keys {
        let entry = agg.entry(key.r#type.clone()).or_default();
        entry.0 += 1;
        entry.1 += key.rdb_size;
    }
    let mut result: Vec<TypeAggregate> = agg
        .into_iter()
        .map(|(data_type, (key_count, total_size))| TypeAggregate {
            data_type,
            key_count,
            total_size,
        })
        .collect();
    result.sort_by(|a, b| b.total_size.cmp(&a.total_size));
    result
}

fn aggregate_by_instance(keys: &[KeyInfo]) -> Vec<InstanceAggregate> {
    let mut agg: HashMap<String, (u64, u64)> = HashMap::new();
    for key in keys {
        let entry = agg.entry(key.instance.clone()).or_default();
        entry.0 += 1;
        entry.1 += key.rdb_size;
    }
    let mut result: Vec<InstanceAggregate> = agg
        .into_iter()
        .map(|(instance, (key_count, total_size))| InstanceAggregate {
            instance,
            key_count,
            total_size,
        })
        .collect();
    result.sort_by(|a, b| b.total_size.cmp(&a.total_size));
    result
}

fn aggregate_top_prefixes(keys: &[KeyInfo]) -> Vec<PrefixAggregate> {
    let mut rng = DeterministicRng::new(RNG_SEED + 1000);
    let mut leaf_prefixes: BTreeMap<String, (u64, u64)> = BTreeMap::new();

    // Aggregate leaf prefixes and handle keys without prefix as "(other)"
    for key in keys {
        let prefix = if key.prefix.is_empty() {
            "(other)".to_string()
        } else {
            key.prefix.clone()
        };
        let entry = leaf_prefixes.entry(prefix).or_default();
        entry.0 += 1;
        entry.1 += key.rdb_size;
    }

    // Generate synthetic entries for parent prefixes to create "misc" sections
    let mut synthetic_entries: BTreeMap<String, (u64, u64)> = BTreeMap::new();
    let all_prefixes: Vec<String> = leaf_prefixes.keys().cloned().collect();

    for prefix in &all_prefixes {
        if prefix == "(other)" {
            continue;
        }

        let mut parts: Vec<&str> = prefix.split(':').filter(|s| !s.is_empty()).collect();
        while parts.len() > 1 {
            parts.pop();
            let mut parent_prefix = parts.join(":");
            parent_prefix.push(':');

            // Generate synthetic entries for keys belonging directly to parent prefix
            if !synthetic_entries.contains_key(&parent_prefix) {
                let base_size = leaf_prefixes
                    .get(prefix)
                    .map(|(_, size)| *size)
                    .unwrap_or(0);
                // Add 5-15% of child size as "misc" for flame graph realism
                let misc_factor = 0.05 + rng.next_f64() * 0.10;
                let misc_size = (base_size as f64 * misc_factor) as u64;
                let misc_count = 1 + rng.gen_range(1, 10);

                synthetic_entries.insert(parent_prefix.clone(), (misc_count, misc_size));
            }
        }
    }

    // Create final hierarchical aggregation
    let mut final_agg: BTreeMap<String, (u64, u64)> = BTreeMap::new();

    for (prefix, (key_count, total_size)) in &leaf_prefixes {
        let leaf_entry = final_agg.entry(prefix.clone()).or_default();
        leaf_entry.0 += *key_count;
        leaf_entry.1 += *total_size;
    }

    for (prefix, (key_count, total_size)) in &synthetic_entries {
        let direct_entry = final_agg.entry(prefix.clone()).or_default();
        direct_entry.0 += *key_count;
        direct_entry.1 += *total_size;
    }

    // Traverse hierarchy for leaf and synthetic entries
    let all_entries: Vec<(String, (u64, u64))> = leaf_prefixes
        .iter()
        .chain(synthetic_entries.iter())
        .map(|(k, v)| (k.clone(), *v))
        .collect();

    for (prefix, (key_count, total_size)) in &all_entries {
        if prefix == "(other)" {
            continue;
        }

        let mut parts: Vec<&str> = prefix.split(':').filter(|s| !s.is_empty()).collect();
        while parts.len() > 1 {
            parts.pop();
            let mut parent_prefix = parts.join(":");
            parent_prefix.push(':');

            let parent_entry = final_agg.entry(parent_prefix).or_default();
            parent_entry.0 += *key_count;
            parent_entry.1 += *total_size;
        }
    }

    let mut result: Vec<PrefixAggregate> = final_agg
        .into_iter()
        .map(|(prefix, (key_count, total_size))| PrefixAggregate {
            prefix: prefix.into(),
            total_size,
            key_count,
        })
        .collect();

    // Sort alphabetically for consistent flame graph order
    result.sort_by(|a, b| a.prefix.as_ref().cmp(b.prefix.as_ref()));

    result
}

fn select_top_keys(keys: &mut [KeyInfo]) -> Vec<TopKeyRecord> {
    keys.sort_by(|a, b| b.rdb_size.cmp(&a.rdb_size));
    keys.iter()
        .take(TOP_N_COUNT)
        .map(|k| TopKeyRecord {
            key: k.key.clone(),
            rdb_size: k.rdb_size,
            member_count: k.member_count,
            r#type: k.r#type.clone(),
            instance: k.instance.clone(),
            db: k.db.into(),
            encoding: k.encoding.clone(),
            expire_at: k.expire_at.clone(),
        })
        .collect()
}

fn select_profile(rng: &mut DeterministicRng, cumulative_freq: &[f64]) -> &'static KeyProfile {
    let r = rng.next_f64() * cumulative_freq.last().unwrap_or(&1.0);
    let index = cumulative_freq.iter().position(|&f| f >= r).unwrap_or(0);
    &KEY_PROFILES[index]
}

fn generate_key_name(
    rng: &mut DeterministicRng,
    profile: &KeyProfile,
    index: usize,
) -> (String, Bytes) {
    let word1 = rng.choose(WORDS_FOR_KEYS);
    let word2 = rng.choose(WORDS_FOR_KEYS);
    let id = rng.gen_range(1_000_000, 9_999_999);
    let key_name = format!("{}{}:{}:{}:{}", profile.prefix, word1, word2, id, index);
    (key_name.clone(), Bytes::from(key_name))
}

fn assign_to_instance(key: &[u8]) -> (&'static str, u32) {
    let checksum = Crc::<u32>::new(&CRC_32_ISO_HDLC).checksum(key);
    let index = checksum as usize % INSTANCES.len();
    INSTANCES[index]
}
