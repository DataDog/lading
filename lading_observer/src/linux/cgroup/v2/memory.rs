//! Parser functions for memory cgroup v2.
//!
//! Functions map to [Linux docs](https://www.kernel.org/doc/Documentation/cgroup-v2.txt).

use metrics::{counter, gauge};
use tracing::warn;

/// Memory stats
///
/// Metrics produced vary by Linux kernel version.
///
/// All keys except the following are produced as gauges. What follows are
/// counters:
///
/// * `pgfault`
/// * `pgmajfault`
/// * `workingset_refault`
/// * `workingset_activate`
/// * `workingset_nodereclaim`
/// * `pgrefill`
/// * `pgscan`
/// * `pgsteal`
/// * `pgactivate`
/// * `pgdeactivate`
/// * `pglazyfree`
/// * `pglazyfreed`
pub(crate) fn stat(content: &str, metric_prefix: &str, labels: &[(String, String)]) {
    let counter_keys = [
        "pgfault",
        "pgmajfault",
        "workingset_refault",
        "workingset_activate",
        "workingset_nodereclaim",
        "pgrefill",
        "pgscan",
        "pgsteal",
        "pgactivate",
        "pgdeactivate",
        "pglazyfree",
        "pglazyfreed",
    ];

    for line in content.lines() {
        let mut parts = line.split_whitespace();
        let Some(key) = parts.next() else {
            // empty line, skip it
            continue;
        };
        let Some(value_str) = parts.next() else {
            warn!("[{metric_prefix}] missing value in key/value pair, skipping");
            continue;
        };

        if counter_keys.contains(&key) {
            let Ok(value) = value_str.parse::<u64>() else {
                warn!("[{metric_prefix}] Failed to parse {key}: {content}");
                continue;
            };
            counter!(format!("{metric_prefix}.{key}"), labels).absolute(value);
        } else {
            let Ok(value) = value_str.parse::<f64>() else {
                warn!("[{metric_prefix}] Failed to parse {key}: {content}");
                continue;
            };
            gauge!(format!("{metric_prefix}.{key}"), labels).set(value);
        }
    }
}
