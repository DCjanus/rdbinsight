use anyhow::{Context, Result};

pub mod ch;
pub mod model;
pub mod parquet;

pub fn render_report_html(report_data: &model::ReportData) -> Result<String> {
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
            "Template replacement failed: pattern not found in template. Expected pattern: {}",
            template_pattern
        ));
    }

    Ok(html_content)
}
