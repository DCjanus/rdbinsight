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
#[derive(Debug, Clone, Serialize)]
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
    pub cluster: String,
    pub batch: String,
    pub db_aggregates: Vec<DbAggregate>,
    pub type_aggregates: Vec<TypeAggregate>,
    pub instance_aggregates: Vec<InstanceAggregate>,
    pub top_keys: Vec<TopKeyRecord>,
    pub top_prefixes: Vec<PrefixAggregate>,
    pub cluster_issues: ClusterIssues,
}

#[async_trait::async_trait]
pub trait ReportDataProvider {
    async fn generate_report_data(&self) -> AnyResult<ReportData>;
}
