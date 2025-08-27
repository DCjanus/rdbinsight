use std::path::Path;

use anyhow::Result;
use time::{OffsetDateTime, UtcOffset};

/// Format batch directory name in UTC format: YYYY-MM-DD_HH-mm-ss.SSSZ
pub fn format_batch_dir(datetime: OffsetDateTime) -> String {
    let utc = datetime.to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}_{:02}-{:02}-{:02}.{:03}Z",
        utc.year(),
        utc.month() as u8,
        utc.day(),
        utc.hour(),
        utc.minute(),
        utc.second(),
        utc.millisecond()
    )
}

/// Create temporary batch directory name with tmp_ prefix
pub fn make_tmp_batch_dir(name: &str) -> String {
    format!("tmp_{name}")
}

/// Hadoop-style directory component for cluster
/// e.g. cluster=my_cluster
pub fn cluster_dir_name(cluster: &str) -> String {
    format!("cluster={cluster}")
}

/// Hadoop-style directory component for final batch directory
/// e.g. batch=2025-08-14_12-34-56.123Z
pub fn final_batch_dir_name(batch_slug: &str) -> String {
    format!("batch={batch_slug}")
}

/// Hadoop-style directory component for temporary batch directory
/// e.g. _tmp_batch=2025-08-14_12-34-56.123Z
pub fn temp_batch_dir_name(batch_slug: &str) -> String {
    format!("_tmp_batch={batch_slug}")
}

/// Sanitize instance filename for filesystem compatibility
/// Handles domains and hostnames with ports
pub fn sanitize_instance_filename(instance: &str) -> String {
    instance
        .replace(':', "-") // Port separator: host:6379 -> host-6379
        .replace(' ', "_") // Spaces in hostnames
}

/// Construct run segment filename for an instance, 6-digit zero-padded index (zero-based)
/// e.g. <instance>.000000.parquet for the first run
pub fn run_segment_filename(instance_sanitized: &str, index_zero_based: usize) -> String {
    format!("{instance_sanitized}.{index_zero_based:0>6}.parquet")
}

/// Construct final instance parquet filename
/// e.g. <instance>.parquet
pub fn final_instance_filename(instance_sanitized: &str) -> String {
    format!("{instance_sanitized}.parquet")
}

/// Ensure a directory exists, creating it and all parent directories if necessary
pub async fn ensure_dir(path: &Path) -> Result<()> {
    use anyhow::{Context, ensure};

    ensure!(!path.is_file(), "Path is a file: {}", path.display());

    tokio::fs::create_dir_all(path)
        .await
        .with_context(|| {
            format!(
                "Failed to create directory and parents for path: {} (check permissions and disk space)",
                path.display()
            )
        })?;

    tracing::debug!(
        operation = "directory_created",
        path = %path.display(),
        "Directory created successfully"
    );

    Ok(())
}

/// Atomically finalize a batch directory by renaming the temporary batch dir to the final batch dir
pub async fn finalize_batch_dir(temp_batch_dir: &Path, final_batch_dir: &Path) -> Result<()> {
    use anyhow::Context;

    tokio::fs::rename(temp_batch_dir, final_batch_dir)
        .await
        .with_context(|| {
            format!(
                "Failed to rename batch directory from {} to {} (ensure no other process is accessing these files)",
                temp_batch_dir.display(),
                final_batch_dir.display()
            )
        })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use time::{Date, Month, Time};

    use super::*;

    #[test]
    fn test_format_batch_dir() {
        // Create a specific UTC datetime for testing
        let date = Date::from_calendar_date(2025, Month::August, 14).unwrap();
        let time = Time::from_hms_milli(12, 34, 56, 123).unwrap();
        let utc = OffsetDateTime::new_utc(date, time);

        let result = format_batch_dir(utc);
        assert_eq!(result, "2025-08-14_12-34-56.123Z");
    }

    #[test]
    fn test_make_tmp_batch_dir() {
        let result = make_tmp_batch_dir("2025-08-14_12-34-56.123Z");
        assert_eq!(result, "tmp_2025-08-14_12-34-56.123Z");
    }

    #[test]
    fn test_hadoop_style_dir_names() {
        let cluster = cluster_dir_name("prod");
        assert_eq!(cluster, "cluster=prod");

        let slug = "2025-08-14_12-34-56.123Z";
        assert_eq!(final_batch_dir_name(slug), "batch=2025-08-14_12-34-56.123Z");
        assert_eq!(
            temp_batch_dir_name(slug),
            "_tmp_batch=2025-08-14_12-34-56.123Z"
        );
    }

    #[test]
    fn test_sanitize_instance_filename() {
        // Test IP with port
        let result = sanitize_instance_filename("127.0.0.1:6379");
        assert_eq!(result, "127.0.0.1-6379");

        // Test hostname with port
        let result = sanitize_instance_filename("redis-server:6379");
        assert_eq!(result, "redis-server-6379");

        // Test domain with port
        let result = sanitize_instance_filename("redis.example.com:6379");
        assert_eq!(result, "redis.example.com-6379");

        // Test hostname with spaces
        let result = sanitize_instance_filename("redis server:6379");
        assert_eq!(result, "redis_server-6379");

        // Test with multiple colons
        let result = sanitize_instance_filename("host:6379:backup");
        assert_eq!(result, "host-6379-backup");

        // Test with no special characters
        let result = sanitize_instance_filename("localhost");
        assert_eq!(result, "localhost");
    }

    #[test]
    fn test_run_and_final_filenames() {
        let inst = "node-1-6379";
        assert_eq!(run_segment_filename(inst, 0), "node-1-6379.000000.parquet");
        assert_eq!(run_segment_filename(inst, 42), "node-1-6379.000042.parquet");
        assert_eq!(final_instance_filename(inst), "node-1-6379.parquet");
    }

    #[tokio::test]
    async fn test_ensure_dir() {
        let temp_dir = tempdir().unwrap();
        let test_path = temp_dir.path().join("level1").join("level2").join("level3");

        // Directory should not exist initially
        assert!(!test_path.exists());

        // Create directory
        ensure_dir(&test_path).await.unwrap();

        // Directory should exist now
        assert!(test_path.exists());
        assert!(test_path.is_dir());

        // Calling again should be idempotent (no error)
        ensure_dir(&test_path).await.unwrap();
        assert!(test_path.exists());
    }

    #[tokio::test]
    async fn test_ensure_dir_with_existing_directory() {
        let temp_dir = tempdir().unwrap();
        let test_path = temp_dir.path();

        // Directory already exists
        assert!(test_path.exists());

        // Should not error when directory already exists
        ensure_dir(test_path).await.unwrap();
        assert!(test_path.exists());
    }
}
