pub(crate) mod cpu;

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
    #[error("Parsing PSI error: {0}")]
    ParsingPsi(String),
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
                                            if file_name == "memory.pressure"
                                                || file_name == "io.pressure"
                                                || file_name == "cpu.pressure"
                                            {
                                                if let Err(err) =
                                                    parse_pressure(&content, &metric_prefix, labels)
                                                {
                                                    debug!("[{path}] Failed to parse PSI contents: {err:?}",
                                                        path = file_path.to_string_lossy()
                                                    );
                                                }
                                                continue;
                                            }

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

fn parse_pressure(content: &str, prefix: &str, labels: &[(String, String)]) -> Result<(), Error> {
    for line in content.lines() {
        parse_pressure_line(line, prefix, |metric: String, value: f64| {
            gauge!(metric, labels).set(value);
        })?;
    }
    Ok(())
}

fn parse_pressure_line<F>(line: &str, prefix: &str, mut f: F) -> Result<(), Error>
where
    F: FnMut(String, f64),
{
    // [some|full] avg10=FLOAT avg60=FLOAT avg300=FLOAT total=FLOAT
    let mut parts = line.split_whitespace();
    if let Some(category) = parts.next() {
        for field in parts {
            let Some((key, val)) = field.split_once('=') else {
                return Err(Error::ParsingPsi(format!("Invalid psi field: {field}")));
            };
            // It might be that total is an integer but for the sake of
            // simplicity we'll parse as f64. It has to become a float anyway
            // when we write it out as a metric.
            let value = val
                .parse::<f64>()
                .map_err(|err| Error::ParsingPsi(format!("{val} -> {err}")))?;

            let metric_name = format!("{prefix}.{category}.{key}");
            f(metric_name, value);
        }
    } else {
        warn!("Unexpected blank category in psi file, skipping line: {line}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::parse_pressure_line;

    #[test]
    fn parse_pressure_line_multiple_fields() {
        let line = "some avg10=0.42 avg60=1.0 total=42";
        let prefix = "cgroup.v2.memory.pressure";

        let mut results = Vec::new();
        let res = parse_pressure_line(line, prefix, |metric, value| {
            results.push((metric, value));
        });

        assert!(res.is_ok());
        assert_eq!(results.len(), 3);

        assert_eq!(
            results[0],
            (String::from("cgroup.v2.memory.pressure.some.avg10"), 0.42)
        );
        assert_eq!(
            results[1],
            (String::from("cgroup.v2.memory.pressure.some.avg60"), 1.0)
        );
        assert_eq!(
            results[2],
            (String::from("cgroup.v2.memory.pressure.some.total"), 42.0)
        );
    }

    #[test]
    fn parse_pressure_line_blank_line() {
        let line = "";
        let prefix = "cgroup.v2.memory.pressure";

        let mut results = Vec::new();
        let res = parse_pressure_line(line, prefix, |metric, value| {
            results.push((metric, value));
        });

        assert!(res.is_ok());
        assert!(results.is_empty());
    }

    #[test]
    fn parse_pressure_line_incomplete() {
        let line = "some";
        let prefix = "cgroup.v2.memory.pressure";

        let mut results = Vec::new();
        let res = parse_pressure_line(line, prefix, |metric, value| {
            results.push((metric, value));
        });

        assert!(res.is_ok());
        assert!(results.is_empty());
    }

    #[test]
    fn parse_pressure_line_malformed_field() {
        let line = "some avg10=0.0 avg60?";
        let prefix = "cgroup.v2.memory.pressure";

        let mut results = Vec::new();
        let res = parse_pressure_line(line, prefix, |metric, value| {
            results.push((metric, value));
        });

        // Intentionally grab as many fields as possible
        assert!(res.is_err());
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0],
            (String::from("cgroup.v2.memory.pressure.some.avg10"), 0.0)
        );
    }

    #[test]
    fn parse_pressure_line_invalid_value() {
        let line = "some avg10=hello";
        let prefix = "cgroup.v2.memory.pressure";

        let mut results = Vec::new();
        let res = parse_pressure_line(line, prefix, |metric, value| {
            results.push((metric, value));
        });

        assert!(res.is_err());
        assert!(results.is_empty());
    }
}
