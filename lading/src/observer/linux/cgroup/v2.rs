use std::{
    io,
    path::{Path, PathBuf},
};

use metrics::gauge;
use tokio::fs;
use tracing::debug;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Parse int error: {0}")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("Parse float error: {0}")]
    ParseFloat(#[from] std::num::ParseFloatError),
    #[error("Cgroup v2 not found")]
    CgroupV2NotFound,
}

/// Determines the cgroup v2 path for a given PID.
pub(crate) async fn get_path(pid: i32) -> Result<PathBuf, Error> {
    let path = format!("/proc/{pid}/cgroup");
    let content = fs::read_to_string(path).await?;

    for line in content.lines() {
        let mut fields = line.split(':');
        let hierarchy_id = fields.next().ok_or(Error::CgroupV2NotFound)?;
        let controllers = fields.next().ok_or(Error::CgroupV2NotFound)?;
        let cgroup_path = fields.next().ok_or(Error::CgroupV2NotFound)?;

        if hierarchy_id == "0" && controllers.is_empty() {
            // cgroup v2 detected
            let cgroup_mount_point = "/sys/fs/cgroup"; // Default mount point
            let full_cgroup_path = PathBuf::from(cgroup_mount_point)
                .join(cgroup_path.strip_prefix('/').unwrap_or(cgroup_path));
            return Ok(full_cgroup_path);
        }
    }

    Err(Error::CgroupV2NotFound)
}

/// Polls for any cgroup metrics that can be read, v2 version.
pub(crate) async fn poll(path: PathBuf, labels: &[(String, String)]) -> Result<(), Error> {
    let mut entries = fs::read_dir(&path).await?;

    while let Some(entry) = entries.next_entry().await? {
        let metadata = entry.metadata().await?;
        if metadata.is_file() {
            let file_name = entry.file_name();
            let metric_prefix = match file_name.to_str() {
                Some(s) => String::from(s),
                None => {
                    // Skip files with non-UTF-8 names
                    continue;
                }
            };
            let file_path = entry.path();

            let content = fs::read_to_string(&file_path).await?;
            let content = content.trim();

            // Cgroup files that have values are either single-valued or
            // key-value pairs. For single-valued files, we create a single
            // metric and for key-value pairs, we create metrics with the same
            // scheme as single-valued files but tack on the key to the metric
            // name.
            if let Ok(value) = content.parse::<f64>() {
                // Single-valued
                gauge!(metric_prefix, labels).set(value);
            } else {
                // Key-value pairs
                if kv_pairs(&file_path, content, &metric_prefix, labels).is_err() {
                    // File may fail to parse, for instance cgroup.controllers
                    // is a list of strings.
                    continue;
                }
            }
        }
    }

    Ok(())
}

fn kv_pairs(
    file_path: &Path,
    content: &str,
    metric_prefix: &str,
    labels: &[(String, String)],
) -> Result<(), Error> {
    for line in content.lines() {
        let mut parts = line.split_whitespace();
        if let Some(key) = parts.next() {
            if let Some(value_str) = parts.next() {
                let value: f64 = value_str.parse()?;
                let metric_name = format!("{metric_prefix}.{key}");
                gauge!(metric_name, labels).set(value);
            } else {
                debug!(
                    "[{path}] missing value in key/value pair: {content}",
                    path = file_path.to_string_lossy(),
                );
            }
        } else {
            debug!(
                "[{path} missing key in key/value pair: {content}",
                path = file_path.to_string_lossy(),
            );
        }
        let key = parts.next().expect("malformed key-value pair");
        let value_str = parts.next().expect("malformed key-value pair");
        let value: f64 = value_str.parse()?;
        let metric_name = format!("{metric_prefix}.{key}");
        gauge!(metric_name, labels).set(value);
    }
    Ok(())
}
