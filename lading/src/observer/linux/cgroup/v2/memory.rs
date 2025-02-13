//! Parser functions for memory cgroup v2.
//!
//! Functions map to [Linux docs](https://www.kernel.org/doc/Documentation/cgroup-v2.txt).

use std::str::FromStr;

use metrics::{counter, gauge};
use tracing::warn;

/// Memory events
///
/// Metrics produced:
///
/// * <counter> `memory.events.low`: The number of times the cgroup is reclaimed
///   due to high memory pressure even though its usage is under the low
///   boundary.
/// * <counter> `memory.events.high`: The number of times processes of the
///   cgroup are throttled and routed to perform direct memory reclaim because
///   the high memory boundary was exceeded.
/// * <counter> `memory.events.max`: The number of times the cgroup's memory
///   usage was about to go over the max boundary.
/// * <counter> `memory.events.oom`: The number of time the cgroup's memory
///   usage was reached the limit and allocation was about to fail.
#[tracing::instrument(skip_all)]
pub(crate) fn events(content: &str, metric_prefix: String, labels: &[(String, String)]) {
    kv_counter(content, &metric_prefix, labels);
}

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
        let key = match parts.next() {
            Some(k) => k,
            None => {
                // empty line, skip it
                continue;
            }
        };
        let value_str = if let Some(v) = parts.next() {
            v
        } else {
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

#[inline]
#[tracing::instrument(skip_all)]
pub(crate) fn single_value(content: &str, metric_prefix: String, labels: &[(String, String)]) {
    // Content is a single-vaue file with an integer value.
    if content == "max" {
        gauge!(metric_prefix, labels).set(f64::MAX);
    } else if let Ok(value) = content.parse::<f64>() {
        gauge!(metric_prefix, labels).set(value);
    } else {
        warn!("[{metric_prefix}] Failed to parse: {content}");
    }
}

pub(crate) fn kv_gauge(content: &str, metric_prefix: &str, labels: &[(String, String)]) {
    kv::<_, f64>(content, metric_prefix, labels, |metric, labels, value| {
        gauge!(metric, labels).set(value);
    });
}

pub(crate) fn kv_counter(content: &str, metric_prefix: &str, labels: &[(String, String)]) {
    kv::<_, u64>(content, metric_prefix, labels, |metric, labels, value| {
        counter!(metric, labels).absolute(value);
    });
}

fn kv<F, T>(content: &str, metric_prefix: &str, labels: &[(String, String)], f: F)
where
    F: Fn(String, &[(String, String)], T),
    T: FromStr + num_traits::bounds::Bounded,
{
    for line in content.lines() {
        let mut parts = line.split_whitespace();
        if let Some(key) = parts.next() {
            if let Some(value_str) = parts.next() {
                let metric_name = format!("{metric_prefix}.{key}");
                if content == "max" {
                    f(metric_name, labels, T::max_value());
                } else if let Ok(value) = value_str.parse::<T>() {
                    f(metric_name, labels, value);
                } else {
                    warn!("[{metric_prefix}] Failed to parse {key}: {content}");
                }
            } else {
                warn!("[{metric_prefix}] missing value in key/value pair, skipping");
            }
        } else {
            warn!("[{metric_prefix}] missing value in key/value pair, skipping");
        }
    }
}
