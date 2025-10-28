//! Validation logic for capture files
//!
//! This module contains the canonical validation logic for lading capture
//! files. All validation - in captool, tests - must use this module to ensure
//! consistency.

use std::collections::{BTreeSet, HashMap, hash_map::RandomState};
use std::hash::{BuildHasher, Hasher};

use crate::json::{Line, MetricKind};

/// Result of validating capture invariants
#[derive(Debug)]
pub struct ValidationResult {
    /// Total number of lines validated
    pub line_count: u128,
    /// Number of unique series (`metric_name` + labels combinations)
    pub unique_series: usize,
    /// Number of unique `fetch_index` values
    pub unique_fetch_indices: usize,
    /// `fetch_index`/time mapping violations
    pub fetch_index_errors: u128,
    /// Per-series violations (time or `fetch_index` not strictly increasing)
    pub per_series_errors: u128,
    /// First error encountered (line number, series id, message)
    pub first_error: Option<(u128, String, String)>,
}

impl ValidationResult {
    /// Returns true if validation passed with no errors
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.fetch_index_errors == 0 && self.per_series_errors == 0
    }
}

/// Validates capture lines satisfy all required invariants.
///
/// Invariants:
///
/// - Each `fetch_index` maps to exactly one time value (globally)
/// - Each series (`metric_name` + labels) has strictly increasing time
/// - Each series (`metric_name` + labels) has strictly increasing `fetch_index`
///
/// This is the canonical validation logic used by captool and tests.
#[must_use]
pub fn validate_lines(lines: &[Line]) -> ValidationResult {
    let mut fetch_index_to_time: HashMap<u64, u128> = HashMap::new();
    let mut series_last_state: HashMap<u64, (u128, u64, String)> = HashMap::new();
    let hash_builder = RandomState::new();
    let mut error_count = 0u128;
    let mut fetch_index_errors = 0u128;
    let mut first_error: Option<(u128, String, String)> = None;

    for (line_num, line) in lines.iter().enumerate() {
        let line_num = line_num as u128 + 1;
        let time = line.time;
        let fetch_index = line.fetch_index;

        // Check global invariant: each fetch_index maps to exactly one time
        if let Some(&existing_time) = fetch_index_to_time.get(&fetch_index) {
            if existing_time != time {
                if fetch_index_errors == 0 {
                    let msg = format!(
                        "fetch_index {fetch_index} appears with multiple times: {existing_time} and {time}"
                    );
                    first_error = Some((line_num, "fetch_index/time mismatch".to_string(), msg));
                }
                fetch_index_errors += 1;
            }
        } else {
            fetch_index_to_time.insert(fetch_index, time);
        }

        // Build a unique key for (metric_name, labels)
        let mut sorted_labels: BTreeSet<String> = BTreeSet::new();
        for (key, value) in &line.labels {
            sorted_labels.insert(format!("{key}:{value}"));
        }

        let mut hasher = hash_builder.build_hasher();
        hasher.write_usize(line.metric_name.len());
        hasher.write(line.metric_name.as_bytes());
        // Include metric kind to distinguish counter from gauge
        let kind_byte = match line.metric_kind {
            MetricKind::Counter => 0u8,
            MetricKind::Gauge => 1u8,
        };
        hasher.write_u8(kind_byte);
        for label in &sorted_labels {
            hasher.write_usize(label.len());
            hasher.write(label.as_bytes());
        }
        let series_key = hasher.finish();

        let kind_str = match line.metric_kind {
            MetricKind::Counter => "counter",
            MetricKind::Gauge => "gauge",
        };
        let series_id = format!(
            "{kind}:{metric}[{labels}]",
            kind = kind_str,
            metric = line.metric_name,
            labels = sorted_labels
                .iter()
                .cloned()
                .collect::<Vec<String>>()
                .join(",")
        );

        // Check per-series invariants
        if let Some((prev_time, prev_fetch_index, _)) = series_last_state.get(&series_key) {
            if time <= *prev_time {
                if error_count == 0 {
                    let msg =
                        format!("time not strictly increasing: prev={prev_time}, curr={time}");
                    first_error = Some((line_num, series_id.clone(), msg));
                }
                error_count += 1;
            }
            if fetch_index <= *prev_fetch_index {
                if error_count == 0 {
                    let msg = format!(
                        "fetch_index not strictly increasing: prev={prev_fetch_index}, curr={fetch_index}"
                    );
                    first_error = Some((line_num, series_id.clone(), msg));
                }
                error_count += 1;
            }
        }

        series_last_state.insert(series_key, (time, fetch_index, series_id));
    }

    ValidationResult {
        line_count: lines.len() as u128,
        unique_series: series_last_state.len(),
        unique_fetch_indices: fetch_index_to_time.len(),
        fetch_index_errors,
        per_series_errors: error_count,
        first_error,
    }
}
