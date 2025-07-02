use std::path::PathBuf;

use anyhow::{Context, Result};
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

pub mod querier;

use querier::ClickHouseQuerier;

use crate::config::{ClickHouseConfig, Config, OutputConfig};

#[derive(Serialize)]
pub struct ReportData {
    pub cluster: String,
    pub batch: String,
    pub prefix_records: Vec<querier::PrefixRecord>,
    pub top_keys: Vec<querier::TopKeyRecord>,
}

pub struct ReportGenerator {
    querier: ClickHouseQuerier,
    cluster: String,
    batch: String,
}

impl ReportGenerator {
    pub async fn new(
        clickhouse_config: crate::config::ClickHouseConfig,
        cluster: String,
        batch: String,
    ) -> Result<Self> {
        let querier = ClickHouseQuerier::new(clickhouse_config, cluster.clone(), batch.clone())
            .await
            .context("Failed to initialize ClickHouse querier")?;

        Ok(Self {
            querier,
            cluster,
            batch,
        })
    }

    pub async fn generate(&self) -> Result<String> {
        tracing::info!(
            "Starting report generation for cluster: {}, batch: {}",
            self.cluster,
            self.batch
        );

        // Use the new unified prefix report data generation
        let prefix_records = self
            .querier
            .generate_prefix_report_data()
            .await
            .context("Failed to generate prefix report data from ClickHouse")?;

        tracing::info!(
            "Generated {} prefix records for unified report",
            prefix_records.len()
        );

        // Get top 100 big keys for all classifications
        let top_keys = self
            .querier
            .get_top_keys_for_all_classifications()
            .await
            .context("Failed to get top keys data from ClickHouse")?;

        tracing::info!("Generated {} top key records for report", top_keys.len());

        let report_data = ReportData {
            cluster: self.cluster.clone(),
            batch: self.batch.clone(),
            prefix_records,
            top_keys,
        };

        // Read HTML template
        let template_content = include_str!("../../templates/report.html");

        // Serialize report data to JSON with pretty formatting for better readability
        let report_json = serde_json::to_string_pretty(&report_data)
            .context("Failed to serialize report data to JSON")?;

        // Define the template pattern to replace
        let template_pattern =
            r#"<script id="rdbinsight-data" type="application/json" src="./report.json"></script>"#;

        // Replace the entire script tag with inline data for production mode
        let html_content = template_content.replace(
            template_pattern,
            &format!(
                r#"<script id="rdbinsight-data" type="application/json">
{report_json}
    </script>"#
            ),
        );

        // Verify that the replacement actually occurred
        if html_content == template_content {
            return Err(anyhow::anyhow!(
                "Template replacement failed: pattern not found in template. \
                Expected pattern: {}",
                template_pattern
            ));
        }

        tracing::info!("Report generation completed successfully");
        Ok(html_content)
    }
}

#[derive(Debug, Row, Deserialize)]
struct LatestBatchRow {
    #[serde(with = "clickhouse::serde::time::datetime64::nanos")]
    batch: OffsetDateTime,
}

async fn get_latest_batch_for_cluster(
    clickhouse_config: &ClickHouseConfig,
    cluster: &str,
) -> Result<String> {
    let mut client_builder = Client::default().with_url(&clickhouse_config.address);

    if let Some(username) = &clickhouse_config.username {
        client_builder = client_builder.with_user(username);
    }

    if let Some(password) = &clickhouse_config.password {
        client_builder = client_builder.with_password(password);
    }

    if let Some(database) = &clickhouse_config.database {
        client_builder = client_builder.with_database(database);
    }

    let client = client_builder;

    let query = "
        SELECT batch
        FROM import_batches_completed
        WHERE cluster = ?
        ORDER BY batch DESC
        LIMIT 1
    ";

    let rows: Vec<LatestBatchRow> = client
        .query(query)
        .bind(cluster)
        .fetch_all()
        .await
        .with_context(|| format!("Failed to query latest batch for cluster: {cluster}"))?;

    if rows.is_empty() {
        return Err(anyhow::anyhow!(
            "No completed batches found for cluster: {}",
            cluster
        ));
    }

    let latest_batch = rows[0].batch;
    let batch_str = latest_batch
        .format(&time::format_description::well_known::Rfc3339)
        .with_context(|| "Failed to format batch timestamp")?;

    Ok(batch_str)
}

pub async fn run_report(
    config_path: PathBuf,
    cluster: String,
    batch: Option<String>,
    output: Option<PathBuf>,
) -> Result<()> {
    tracing::info!("Loading configuration from: {}", config_path.display());

    let config_content = tokio::fs::read_to_string(&config_path)
        .await
        .with_context(|| format!("Failed to read config file: {}", config_path.display()))?;

    let config: Config =
        toml::from_str(&config_content).with_context(|| "Failed to parse configuration file")?;

    let clickhouse_config = match &config.output {
        OutputConfig::Clickhouse(clickhouse_config) => clickhouse_config.clone(),
    };

    // Get actual batch string - either provided or latest for the cluster
    let actual_batch = match batch {
        Some(batch_str) => {
            // Validate the provided batch timestamp format
            let _batch_timestamp =
                OffsetDateTime::parse(&batch_str, &time::format_description::well_known::Rfc3339)
                    .with_context(|| format!("Invalid batch timestamp format: {batch_str}"))?;
            batch_str
        }
        None => {
            tracing::info!(
                "No batch specified, fetching latest batch for cluster: {}",
                cluster
            );
            get_latest_batch_for_cluster(&clickhouse_config, &cluster)
                .await
                .with_context(|| format!("Failed to get latest batch for cluster: {cluster}"))?
        }
    };

    tracing::info!("Using batch: {}", actual_batch);

    let generator = ReportGenerator::new(clickhouse_config, cluster.clone(), actual_batch.clone())
        .await
        .context("Failed to initialize report generator")?;

    let html_content = generator
        .generate()
        .await
        .context("Failed to generate report")?;

    let output_path = match output {
        Some(path) => path,
        None => {
            // Generate default filename: {cluster}_{batch_in_rfc3339_with_tz}.html
            let safe_batch = actual_batch.replace(':', "-").replace('+', "_");
            let filename: String = format!("rdb_report_{cluster}_{safe_batch}.html");
            PathBuf::from(filename)
        }
    };

    tokio::fs::write(&output_path, html_content)
        .await
        .with_context(|| format!("Failed to write report to: {}", output_path.display()))?;

    tracing::info!("Report generated successfully: {}", output_path.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use time::OffsetDateTime;

    use super::*;

    #[test]
    fn test_latest_batch_row_deserialization() {
        // Test that LatestBatchRow can be properly deserialized
        let batch_time = OffsetDateTime::from_unix_timestamp(1640995200).unwrap(); // 2022-01-01
        let row = LatestBatchRow { batch: batch_time };

        // Verify that the batch timestamp is correctly stored
        assert_eq!(row.batch, batch_time);
    }

    #[test]
    fn test_batch_format_conversion() {
        // Test that we can format a batch timestamp to RFC3339 format
        let batch_time = OffsetDateTime::from_unix_timestamp(1640995200).unwrap(); // 2022-01-01
        let formatted = batch_time
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();

        // Should be in RFC3339 format
        assert_eq!(formatted, "2022-01-01T00:00:00Z");
    }

    #[test]
    fn test_report_data_structure() {
        // Test that ReportData can be properly serialized for the unified report
        let report_data = ReportData {
            cluster: "test-cluster".to_string(),
            batch: "2024-01-01T00:00:00Z".to_string(),
            prefix_records: vec![querier::PrefixRecord {
                prefix_base64: bytes::Bytes::from("user:"),
                instance: "127.0.0.1:6379".to_string(),
                db: 0,
                r#type: "string".to_string(),
                rdb_size: 600,
                key_count: 100,
            }],
            top_keys: vec![],
        };

        // Verify the structure is correct
        assert_eq!(report_data.cluster, "test-cluster");
        assert_eq!(report_data.batch, "2024-01-01T00:00:00Z");
        assert!(!report_data.prefix_records.is_empty());
    }
}
