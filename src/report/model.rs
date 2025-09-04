use bytes::Bytes;
use serde::Serialize;
use serde_with::{base64::Base64, serde_as};

use crate::helper::AnyResult;

#[serde_as]
#[derive(Debug, Clone, Serialize)]
pub struct TopKeyRecord {
    #[serde_as(as = "Base64")]
    #[serde(rename = "key_base64")]
    pub key: Bytes,
    pub rdb_size: u64,
    pub member_count: Option<u64>,
    pub r#type: String,
    pub instance: String,
    pub db: u64,
    pub encoding: String,
    /// Expiration time as an RFC3339 UTC string (e.g. `2025-01-15T10:00:00Z`).
    pub expire_at: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DbAggregate {
    pub db: u64,
    pub key_count: u64,
    pub total_size: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TypeAggregate {
    pub data_type: String,
    pub key_count: u64,
    pub total_size: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct InstanceAggregate {
    pub instance: String,
    pub key_count: u64,
    pub total_size: u64,
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Eq, PartialEq)]
pub struct PrefixAggregate {
    #[serde_as(as = "Base64")]
    #[serde(rename = "prefix_base64")]
    pub prefix: Bytes,
    pub total_size: u64,
    pub key_count: u64,
}

#[serde_as]
#[derive(Debug, Clone, Serialize)]
pub struct BigKey {
    #[serde_as(as = "Base64")]
    #[serde(rename = "key_base64")]
    pub key: Bytes,
    pub instance: String,
    pub db: u64,
    pub r#type: String,
    pub rdb_size: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClusterIssues {
    pub big_keys: Vec<BigKey>,
    pub codis_slot_skew: bool,
    pub redis_cluster_slot_skew: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReportData {
    /// Logical cluster name that this report describes.
    /// Providers should pass through the input label; data-source agnostic.
    pub cluster: String,
    /// Identifier of the analyzed batch/snapshot.
    /// Providers should return a human-readable, monotonically sortable string (RFC3339 recommended).
    pub batch: String,
    /// Aggregates per logical DB index.
    /// - key_count: number of keys in the DB
    /// - total_size: sum of `rdb_size` over all keys in the DB
    ///
    /// Sorting: ascending by `db`.
    pub db_aggregates: Vec<DbAggregate>,
    /// Aggregates per Redis data type (e.g., "string", "list", ...).
    /// - key_count: number of keys of the type
    /// - total_size: sum of `rdb_size` over keys of the type
    ///
    /// Sorting: descending by `total_size`.
    pub type_aggregates: Vec<TypeAggregate>,
    /// Aggregates per Redis instance.
    /// - key_count: number of keys on the instance
    /// - total_size: sum of `rdb_size` over keys on the instance
    ///
    /// Sorting: descending by `total_size`.
    pub instance_aggregates: Vec<InstanceAggregate>,
    /// Top 100 keys by `rdb_size` across the batch.
    /// Sorting: descending by `rdb_size`. When `rdb_size` values are equal, keys MUST be ordered
    /// by their raw bytes in ascending lexicographic order (i.e., smallest key first).
    /// Limit: at most 100 items.
    pub top_keys: Vec<TopKeyRecord>,
    /// Significant key prefixes discovered from all keys in the batch.
    /// Aggregation: for a byte prefix P, `key_count` and `total_size` are computed over keys whose bytes start with P.
    /// Inclusion: only prefixes with `total_size >= floor(total_size_all_keys / 100)`, with a minimum threshold of 1 byte.
    /// Structure: nested prefixes may exist; when identical stats occur for a prefix and its descendant, providers may keep the more specific prefix.
    /// Sorting: ascending by raw prefix bytes (lexicographic).
    pub top_prefixes: Vec<PrefixAggregate>,
    /// Cluster-level issue summary derived from the batch.
    /// - big_keys: largest key per data type exceeding thresholds (string > 1 MiB; non-string > 1 GiB), sorted by `rdb_size` descending
    /// - codis_slot_skew: true if any Codis slot maps to more than one instance within the batch
    /// - redis_cluster_slot_skew: true if any Redis Cluster slot maps to more than one instance within the batch
    pub cluster_issues: ClusterIssues,
}

#[async_trait::async_trait]
pub trait ReportDataProvider {
    async fn generate_report_data(&self) -> AnyResult<ReportData>;
}
