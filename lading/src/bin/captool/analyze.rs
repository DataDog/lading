//! Analysis operations for capture files
//!
//! This module provides metric listing and analysis operations for both
//! JSONL and Parquet capture formats.

pub(crate) mod jsonl;
pub(crate) mod parquet;

use std::collections::BTreeSet;

/// A unique metric (name + kind)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct MetricInfo {
    /// Metric kind ("counter" or "gauge")
    pub(crate) kind: String,
    /// Metric name
    pub(crate) name: String,
}

/// Statistics for a metric series (metric + specific label set)
#[derive(Debug)]
pub(crate) struct SeriesStats {
    /// Label key-value pairs for this series
    pub(crate) labels: BTreeSet<String>,
    /// Minimum value
    pub(crate) min: f64,
    /// Maximum value
    pub(crate) max: f64,
    /// Mean value
    pub(crate) mean: f64,
    /// Whether values are monotonically increasing
    pub(crate) is_monotonic: bool,
    /// All values (for dump_values)
    pub(crate) values: Vec<(u64, f64)>, // (fetch_index, value)
}
