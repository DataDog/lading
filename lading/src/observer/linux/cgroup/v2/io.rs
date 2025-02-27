//! IO controller for cgroup v2.

use metrics::{counter, gauge};
use tracing::warn;

pub(crate) fn max(content: &str, metric_prefix: &str, labels: &[(String, String)]) {
    // Lines of the form
    //
    // <major>:<minor> <read_limit> <write_limit>
    //
    // Where limit is u64|max. All values are counters. We add a label called
    // "device" that is "<major>:<minor>".
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let mut parts = line.split_whitespace();
        let Some(device) = parts.next() else {
            continue; // skip invalid line
        };

        let read_limit = match parts.next() {
            Some("max") => f64::MAX,
            Some(value) => {
                if let Ok(value) = value.parse::<f64>() {
                    value
                } else {
                    warn!(
                        "[{metric_prefix}] Failed to parse read limit for device {device}: {value}",
                    );
                    continue;
                }
            }
            None => {
                warn!("[{metric_prefix}] Missing read limit for device {device}",);
                continue;
            }
        };

        let write_limit = match parts.next() {
            Some("max") => f64::MAX,
            Some(value) => {
                if let Ok(value) = value.parse::<f64>() {
                    value
                } else {
                    warn!(
                        "[{metric_prefix}] Failed to parse write limit for device {device}: {value}",
                    );
                    continue;
                }
            }
            None => {
                warn!("[{metric_prefix}] Missing write limit for device {device}",);
                continue;
            }
        };

        let tags = labels
            .iter()
            .cloned()
            .chain(std::iter::once(("device".to_string(), device.to_string())))
            .collect::<Vec<_>>();

        gauge!(format!("{metric_prefix}.read_limit"), &tags).set(read_limit);
        gauge!(format!("{metric_prefix}.write_limit"), &tags).set(write_limit);
    }
}

pub(crate) fn stat(content: &str, metric_prefix: &str, labels: &[(String, String)]) {
    // Lines of the form
    //
    // <major>:<minor> key1=value1 key2=value2 ...
    //
    // All values are counters, u64. We add a label called "device" that is
    // "<major>:<minor>".
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let mut parts = line.split_whitespace();
        let Some(device) = parts.next() else {
            continue; // skip invalid line
        };

        let tags = labels
            .iter()
            .cloned()
            .chain(std::iter::once(("device".to_string(), device.to_string())))
            .collect::<Vec<_>>();

        for pair in parts {
            let mut kv = pair.split('=');
            let Some(key) = kv.next() else {
                warn!("[{metric_prefix}] Missing key in pair '{pair}' on device {device}",);
                continue;
            };
            let Some(value_str) = kv.next() else {
                warn!("[{metric_prefix}] Missing value in pair '{pair}' on device {device}",);
                continue;
            };

            let Ok(value) = value_str.parse::<u64>() else {
                warn!(
                    "[{metric_prefix}] Failed to parse value for key {key} on device {device}: {value_str}",
                );
                continue;
            };

            counter!(format!("{metric_prefix}.{key}"), &tags).absolute(value);
        }
    }
}
