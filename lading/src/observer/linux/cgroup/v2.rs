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
pub(crate) async fn poll(file_path: &Path, labels: &[(String, String)]) -> Result<(), Error> {
    // Read all files in the cgroup `path` and create metrics for them. If we
    // lack permissions to read we skip the file. We do not use ? to allow for
    // the maximal number of files to be read.
    match fs::read_dir(&file_path).await {
        Ok(mut entries) => {
            match entries.next_entry().await {
                Ok(maybe_entry) => {
                    if let Some(entry) = maybe_entry {
                        match entry.metadata().await {
                            Ok(metadata) => {
                                if metadata.is_file() {
                                    let file_name = entry.file_name();
                                    let metric_prefix = match file_name.to_str() {
                                        Some(s) => String::from(s),
                                        None => {
                                            // Skip files with non-UTF-8 names
                                            return Ok(());
                                        }
                                    };
                                    let file_path = entry.path();

                                    match fs::read_to_string(&file_path).await {
                                        Ok(content) => {
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
                                                if kv_pairs(
                                                    &file_path,
                                                    content,
                                                    &metric_prefix,
                                                    labels,
                                                )
                                                .is_err()
                                                {
                                                    // File may fail to parse, for instance cgroup.controllers
                                                    // is a list of strings.
                                                    return Ok(());
                                                }
                                            }
                                        }
                                        Err(err) => {
                                            debug!(
                                                "[{path}] failed to read cgroup file contents: {err:?}",
                                                path = file_path.to_string_lossy()
                                            );
                                        }
                                    }
                                }
                            }
                            Err(err) => {
                                debug!(
                                    "[{path}] failed to read metadata for cgroup file: {err:?}",
                                    path = file_path.to_string_lossy()
                                );
                            }
                        }
                    }
                }
                Err(err) => {
                    debug!(
                        "[{path}] failed to read entry in cgroup directory: {err:?}",
                        path = file_path.to_string_lossy()
                    );
                }
            }
        }
        Err(err) => {
            debug!("Failed to read cgroup directory: {err:?}",);
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
                    "[{path}] missing value in key/value pair, skipping",
                    path = file_path.to_string_lossy(),
                );
            }
        } else {
            debug!(
                "[{path} missing key in key/value pair, skipping",
                path = file_path.to_string_lossy(),
            );
        }
    }
    Ok(())
}
