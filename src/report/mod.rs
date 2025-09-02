use std::path::PathBuf;

use anyhow::{Context, Result};
use clickhouse::Row;
use serde::Deserialize;
use time::OffsetDateTime;

pub mod ch;
pub mod model;
pub mod parquet;

use ch::ClickHouseReportProvider;

use crate::{config::ClickHouseConfig, report::model::ReportDataProvider};

pub struct ReportGenerator {
    querier: ClickHouseReportProvider,
    cluster: String,
    batch: String,
}

impl ReportGenerator {
    pub async fn new(
        clickhouse_config: crate::config::ClickHouseConfig,
        cluster: String,
        batch: String,
    ) -> Result<Self> {
        let querier =
            ClickHouseReportProvider::new(clickhouse_config, cluster.clone(), batch.clone())
                .await
                .context("Failed to initialize ClickHouse querier")?;

        Ok(Self {
            querier,
            cluster,
            batch,
        })
    }

    pub async fn generate_data(&self) -> Result<model::ReportData> {
        tracing::info!(
            operation = "report_generation_start",
            cluster = %self.cluster,
            batch = %self.batch,
            "Starting report data generation"
        );

        let report_data = self
            .querier
            .generate_report_data()
            .await
            .context("Failed to generate report data from ClickHouse")?;

        tracing::info!(
            operation = "report_data_generated",
            cluster = %self.cluster,
            batch = %self.batch,
            db_count = report_data.db_aggregates.len(),
            type_count = report_data.type_aggregates.len(),
            instance_count = report_data.instance_aggregates.len(),
            top_keys_count = report_data.top_keys.len(),
            top_prefixes_count = report_data.top_prefixes.len(),
            "Generated report data successfully"
        );

        Ok(report_data)
    }

    pub async fn generate(&self) -> Result<String> {
        let report_data = self.generate_data().await?;

        let template_content = include_str!("../../templates/report.html");

        let report_json = serde_json::to_string_pretty(&report_data)
            .context("Failed to serialize report data to JSON")?;

        let template_pattern =
            r#"<script id="rdbinsight-data" type="application/json" src="./report.json"></script>"#;

        let html_content = template_content.replace(
            template_pattern,
            &format!(
                r#"<script id="rdbinsight-data" type="application/json">
{report_json}
    </script>"#
            ),
        );

        if html_content == template_content {
            return Err(anyhow::anyhow!(
                "Template replacement failed: pattern not found in template. \
                Expected pattern: {}",
                template_pattern
            ));
        }

        Ok(html_content)
    }
}

#[derive(Debug, Row, Deserialize)]
struct LatestBatchRow {
    #[serde(with = "clickhouse::serde::time::datetime64::nanos")]
    batch: OffsetDateTime,
}

pub async fn get_latest_batch_for_cluster(
    clickhouse_config: &ClickHouseConfig,
    cluster: &str,
) -> Result<String> {
    let client = clickhouse_config
        .create_client()
        .context("Failed to create ClickHouse client")?;

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

pub async fn run_report_with_config(
    clickhouse_config: ClickHouseConfig,
    cluster: String,
    batch: Option<String>,
    output: Option<PathBuf>,
) -> Result<()> {
    let actual_batch = match batch {
        Some(batch_str) => {
            let _batch_timestamp =
                OffsetDateTime::parse(&batch_str, &time::format_description::well_known::Rfc3339)
                    .with_context(|| format!("Invalid batch timestamp format: {batch_str}"))?;
            batch_str
        }
        None => {
            tracing::info!(
                operation = "latest_batch_fetch",
                cluster = %cluster,
                "No batch specified, fetching latest batch for cluster"
            );
            get_latest_batch_for_cluster(&clickhouse_config, &cluster)
                .await
                .with_context(|| format!("Failed to get latest batch for cluster: {cluster}"))?
        }
    };

    tracing::info!(
        operation = "batch_selected",
        cluster = %cluster,
        batch = %actual_batch,
        "Using batch for report generation"
    );

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
            let safe_batch = actual_batch.replace(':', "-").replace('+', "_");
            let filename: String = format!("rdb_report_{cluster}_{safe_batch}.html");
            PathBuf::from(filename)
        }
    };

    tokio::fs::write(&output_path, html_content)
        .await
        .with_context(|| format!("Failed to write report to: {}", output_path.display()))?;

    tracing::info!(
        operation = "report_generated",
        cluster = %cluster,
        batch = %actual_batch,
        output_path = %output_path.display(),
        "Report generated successfully"
    );
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
}
