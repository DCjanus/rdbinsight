use bytes::Bytes;
use rdbinsight::{
    config::ClickHouseConfig,
    output::clickhouse::{BatchInfo, ClickHouseOutput},
    record::{Record, RecordEncoding, RecordType},
    report::{ReportGenerator, get_latest_batch_for_cluster},
};
mod common;
use common::clickhouse::start_clickhouse;
use time::OffsetDateTime;
use tracing::info;
use url::Url;

use crate::common::init_log_for_debug;

fn make_records() -> Vec<Record> {
    use rdbinsight::parser::{core::raw::RDBStr, model::StringEncoding};

    vec![
        Record::builder()
            .db(0)
            .key(RDBStr::Str(Bytes::from("user:1")))
            .r#type(RecordType::String)
            .encoding(RecordEncoding::String(StringEncoding::Raw))
            .rdb_size(100)
            .member_count(Some(1))
            .build(),
        Record::builder()
            .db(0)
            .key(RDBStr::Str(Bytes::from("user:2")))
            .r#type(RecordType::String)
            .encoding(RecordEncoding::String(StringEncoding::Raw))
            .rdb_size(200)
            .member_count(Some(1))
            .build(),
        Record::builder()
            .db(1)
            .key(RDBStr::Str(Bytes::from("hash:1")))
            .r#type(RecordType::Hash)
            .encoding(RecordEncoding::Hash(
                rdbinsight::parser::model::HashEncoding::Raw,
            ))
            .rdb_size(300)
            .member_count(Some(2))
            .build(),
    ]
}

#[tokio::test]
async fn test_report_generate_data_with_clickhouse() {
    // init simple logging for debug
    init_log_for_debug();

    // 1) start clickhouse
    info!("Starting ClickHouse container");
    let env = start_clickhouse(None).await.unwrap();
    info!("ClickHouse container started: {}", env.host_url);

    // 2) build clickhouse config and output (auto-create tables)
    let ch_url = Url::parse(&env.host_url).unwrap();
    let ch_config = ClickHouseConfig::new(ch_url, true, None).unwrap();
    let cluster = "test-cluster".to_string();
    let batch_ts = OffsetDateTime::now_utc();
    let output = ClickHouseOutput::new(ch_config.clone(), cluster.clone(), batch_ts)
        .await
        .unwrap();

    // 3) write some records into this batch and commit
    let batch_info = BatchInfo {
        cluster: cluster.clone(),
        batch: batch_ts,
    };

    // write same records for 2 instances to exercise instance aggregates
    let records = make_records();
    output
        .write(&records, &batch_info, "10.0.0.1:6379")
        .await
        .unwrap();
    output
        .write(&records, &batch_info, "10.0.0.2:6379")
        .await
        .unwrap();
    output.commit_batch(&batch_info).await.unwrap();

    // 4) build report generator for the same batch and fetch data
    let batch_str = get_latest_batch_for_cluster(&ch_config, &cluster)
        .await
        .unwrap();
    let generator = ReportGenerator::new(ch_config.clone(), cluster.clone(), batch_str.clone())
        .await
        .unwrap();
    let data = generator.generate_data().await.unwrap();

    // 5) assertions
    assert_eq!(data.cluster, cluster);
    assert_eq!(data.batch, batch_str);

    // db aggregates: db0 has two string keys across 2 instances => 4 rows; sizes 100+200 twice = 600
    // db1 has one hash key across 2 instances => 2 rows; size 300 twice = 600
    let mut db_map = std::collections::BTreeMap::new();
    for a in &data.db_aggregates {
        db_map.insert(a.db, (a.key_count, a.total_size));
    }
    assert_eq!(db_map.get(&0), Some(&(4, 600)));
    assert_eq!(db_map.get(&1), Some(&(2, 600)));

    // type aggregates: string has 4 rows (100,200 repeated) total 600; hash has 2 rows total 600
    let mut type_map = std::collections::BTreeMap::new();
    for a in &data.type_aggregates {
        type_map.insert(a.data_type.clone(), (a.key_count, a.total_size));
    }
    assert_eq!(type_map.get("string"), Some(&(4, 600)));
    assert_eq!(type_map.get("hash"), Some(&(2, 600)));

    // instance aggregates: two instances, each wrote 3 keys => 3 rows per instance
    let mut inst_map = std::collections::BTreeMap::new();
    for a in &data.instance_aggregates {
        inst_map.insert(a.instance.clone(), (a.key_count, a.total_size));
    }
    assert_eq!(inst_map.get("10.0.0.1:6379"), Some(&(3, 600)));
    assert_eq!(inst_map.get("10.0.0.2:6379"), Some(&(3, 600)));

    // top_keys should contain our 6 keys, largest first (300 hash entries then 200,100 strings)
    assert_eq!(data.top_keys.len(), 6);
    assert!(data.top_keys[0].rdb_size >= data.top_keys[1].rdb_size);

    // cluster issues: no big keys and no slot skew expected
    assert!(data.cluster_issues.big_keys.is_empty());
    assert!(!data.cluster_issues.codis_slot_skew);
    assert!(!data.cluster_issues.redis_cluster_slot_skew);
}

#[tokio::test]
async fn test_report_generate_data_with_empty_cluster() {
    // init simple logging for debug
    init_log_for_debug();

    // 1) start clickhouse
    info!("Starting ClickHouse container");
    let env = start_clickhouse(None).await.unwrap();
    info!("ClickHouse container started: {}", env.host_url);

    // 2) build clickhouse config and output (auto-create tables)
    let ch_url = Url::parse(&env.host_url).unwrap();
    let ch_config = ClickHouseConfig::new(ch_url, true, None).unwrap();
    let cluster = "empty-cluster".to_string();
    let batch_ts = OffsetDateTime::now_utc();
    let output = ClickHouseOutput::new(ch_config.clone(), cluster.clone(), batch_ts)
        .await
        .unwrap();

    // 3) create an empty batch and commit (no data written)
    let batch_info = BatchInfo {
        cluster: cluster.clone(),
        batch: batch_ts,
    };

    // Only commit the batch without writing any records
    output.commit_batch(&batch_info).await.unwrap();

    // 4) build report generator for the empty batch and fetch data
    let batch_str = get_latest_batch_for_cluster(&ch_config, &cluster)
        .await
        .unwrap();
    let generator = ReportGenerator::new(ch_config.clone(), cluster.clone(), batch_str.clone())
        .await
        .unwrap();
    let data = generator.generate_data().await.unwrap();

    // 5) assertions for empty cluster
    assert_eq!(data.cluster, cluster);
    assert_eq!(data.batch, batch_str);

    // All aggregates should be empty for empty cluster
    assert!(data.db_aggregates.is_empty());
    assert!(data.type_aggregates.is_empty());
    assert!(data.instance_aggregates.is_empty());
    assert!(data.top_keys.is_empty());
    assert!(data.top_prefixes.is_empty());

    // cluster issues should also be empty
    assert!(data.cluster_issues.big_keys.is_empty());
    assert!(!data.cluster_issues.codis_slot_skew);
    assert!(!data.cluster_issues.redis_cluster_slot_skew);
}
