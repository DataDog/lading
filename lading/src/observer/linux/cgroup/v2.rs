use core::f64;
use std::{
    io,
    path::{Path, PathBuf},
};

use metrics::gauge;
use tokio::fs;
use tracing::{debug, error, warn};

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
            loop {
                match entries.next_entry().await {
                    Ok(Some(entry)) => {
                        match entry.metadata().await {
                            Ok(metadata) => {
                                if metadata.is_file() {
                                    let file_name = entry.file_name();
                                    let metric_prefix = if let Some(s) = file_name.to_str() {
                                        format!("cgroup.v2.{s}")
                                    } else {
                                        // Skip files with non-UTF-8 names
                                        warn!("Encountered non-UTF-8 file name in cgroup v2 directory. What a weird thing to happen.");
                                        continue;
                                    };
                                    let file_path = entry.path();

                                    match fs::read_to_string(&file_path).await {
                                        Ok(content) => {
                                            let content = content.trim();

                                            // The format of cgroupv2 interface
                                            // files is defined here:
                                            // https://docs.kernel.org/admin-guide/cgroup-v2.html#interface-files
                                            //
                                            // This implementation parses only new-line separated files with a
                                            // single value which may be "max" or a number. It also parses
                                            // key-value pairs of the "flat keyed" style.

                                            // Single value
                                            if content == "max" {
                                                gauge!(metric_prefix, labels).set(f64::MAX);
                                            } else if let Ok(value) = content.parse::<f64>() {
                                                gauge!(metric_prefix, labels).set(value);
                                            } else {
                                                // Flat keyed style key-value pairs
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
                                                    continue;
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
                    Ok(None) => {
                        break;
                    }
                    Err(err) => {
                        debug!(
                            "[{path}] failed to read entry in cgroup directory: {err:?}",
                            path = file_path.to_string_lossy()
                        );
                    }
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
                let value: f64 = match value_str {
                    "max" => f64::MAX,
                    s => s.parse()?,
                };
                let metric_name = format!("{metric_prefix}.{key}");
                gauge!(metric_name, labels).set(value);
            } else {
                debug!(
                    "[{path}] missing value in key/value pair, skipping",
                    path = file_path.to_string_lossy(),
                );
                return Ok(());
            }
        } else {
            debug!(
                "[{path} missing key in key/value pair, skipping",
                path = file_path.to_string_lossy(),
            );
            return Ok(());
        }
    }
    Ok(())
}
