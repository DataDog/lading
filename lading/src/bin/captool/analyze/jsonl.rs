//! JSONL capture file analysis
//!
//! This module provides analysis operations on JSONL (row-based) capture files.

use std::collections::{BTreeSet, HashMap, hash_map::RandomState};
use std::hash::{BuildHasher, Hasher};

use lading_capture::line::{Line, MetricKind};

use super::{MetricInfo, SeriesStats};

/// Lists all unique metrics from a collection of lines.
///
/// Returns a sorted list of (kind, name) tuples.
#[must_use]
pub(crate) fn list_metrics(lines: &[Line]) -> Vec<MetricInfo> {
    let mut metrics: BTreeSet<MetricInfo> = BTreeSet::new();

    for line in lines {
        let kind = match line.metric_kind {
            MetricKind::Counter => "counter".to_string(),
            MetricKind::Gauge => "gauge".to_string(),
            MetricKind::Histogram => "histogram".to_string(),
        };
        metrics.insert(MetricInfo {
            kind,
            name: line.metric_name.clone(),
        });
    }

    metrics.into_iter().collect()
}

/// Analyzes a specific metric from a collection of lines.
///
/// Returns statistics grouped by label set (context).
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub(crate) fn analyze_metric(
    lines: &[Line],
    metric_name: &str,
) -> HashMap<BTreeSet<String>, SeriesStats> {
    let mut context_map: HashMap<u64, (BTreeSet<String>, Vec<f64>)> = HashMap::new();
    let mut fetch_indices: HashMap<u64, Vec<u64>> = HashMap::new();
    let hash_builder = RandomState::new();

    // Filter to matching metric and group by label set
    for line in lines {
        if line.metric_name != metric_name {
            continue;
        }

        let mut sorted_labels: BTreeSet<String> = BTreeSet::new();
        for (key, value) in &line.labels {
            sorted_labels.insert(format!("{key}:{value}"));
        }

        let mut context_key = hash_builder.build_hasher();
        context_key.write_usize(metric_name.len());
        context_key.write(metric_name.as_bytes());
        for label in &sorted_labels {
            context_key.write_usize(label.len());
            context_key.write(label.as_bytes());
        }
        let key = context_key.finish();

        let entry = context_map.entry(key).or_default();
        entry.0 = sorted_labels;
        entry.1.push(line.value.as_f64());

        fetch_indices.entry(key).or_default().push(line.fetch_index);
    }

    // Compute statistics
    let mut results = HashMap::new();
    for (key, (labels, values)) in context_map {
        if values.is_empty() {
            continue;
        }

        let min = values.iter().copied().fold(f64::INFINITY, f64::min);
        let max = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let sum: f64 = values.iter().sum();
        let mean = sum / values.len() as f64;

        let is_monotonic = values.windows(2).all(|w| w[0] <= w[1]);

        // Combine with fetch_indices
        let fetch_idx = fetch_indices.get(&key).expect("fetch_indices exist");
        let values_with_fetch: Vec<(u64, f64)> = fetch_idx
            .iter()
            .copied()
            .zip(values.iter().copied())
            .collect();

        results.insert(
            labels.clone(),
            SeriesStats {
                labels,
                min,
                max,
                mean,
                is_monotonic,
                values: values_with_fetch,
            },
        );
    }

    results
}
