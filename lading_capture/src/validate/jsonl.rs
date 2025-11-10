//! JSONL capture file validation
//!
//! This module provides validation for JSONL (JSON Lines) format capture files.
//! It operates on already-deserialized `Line` objects.

use std::collections::hash_map::RandomState;
use std::collections::{BTreeSet, HashMap};
use std::hash::{BuildHasher, Hasher};

use crate::line::{Line, MetricKind};
use crate::validate::ValidationResult;

/// Validates capture lines satisfy all required invariants.
///
/// This function uses a two-phase validation approach:
///
/// **Phase 1 (Streaming):** Validates global invariants that can fail fast:
/// - Each `fetch_index` maps to exactly one time value (1:1 mapping globally)
///
/// **Phase 2 (After collection):** Validates per-series invariants after sorting by `fetch_index`:
/// - Within each series: `fetch_index` values are strictly increasing (no duplicates)
/// - Within each series: `time` values are strictly increasing
/// - If `min_seconds` specified: time span meets requirements
///
/// **Physical line order does not matter.** Lines can arrive in any order.
/// Validation is based on logical ordering after sorting by `fetch_index` within each series.
///
/// A series is defined by: `metric_name` + `metric_kind` + labels.
///
/// This is the canonical validation logic used by captool and tests.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn validate_lines(lines: &[Line], min_seconds: Option<u64>) -> ValidationResult {
    // Phase 1: Streaming assertions
    let mut fetch_index_to_time: HashMap<u64, u128> = HashMap::new();
    // Collect (fetch_index, time) pairs for each series
    let mut series_data: HashMap<u64, (Vec<(u64, u128)>, String)> = HashMap::new();
    let hash_builder = RandomState::new();
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

        // Collect (fetch_index, time) pair for this series
        series_data
            .entry(series_key)
            .or_insert_with(|| (Vec::new(), series_id))
            .0
            .push((fetch_index, time));
    }

    // Phase 2: Verify per-series invariants after sorting
    let mut per_series_errors = 0u128;
    let num_series = series_data.len();
    for (_series_key, (mut data, series_id)) in series_data {
        // Sort by fetch_index
        data.sort_unstable_by_key(|(fetch_idx, _)| *fetch_idx);

        // Check for duplicate fetch_index values and time monotonicity
        for window in data.windows(2) {
            let (prev_fetch_idx, prev_time) = window[0];
            let (curr_fetch_idx, curr_time) = window[1];

            // Check fetch_index strictly increasing (no duplicates)
            if curr_fetch_idx <= prev_fetch_idx {
                if per_series_errors == 0 && first_error.is_none() {
                    let msg = format!(
                        "fetch_index not strictly increasing: prev={prev_fetch_idx}, curr={curr_fetch_idx}"
                    );
                    first_error = Some((0, series_id.clone(), msg));
                }
                per_series_errors += 1;
            }

            // Check time strictly increasing
            if curr_time <= prev_time {
                if per_series_errors == 0 && first_error.is_none() {
                    let msg =
                        format!("time not strictly increasing: prev={prev_time}, curr={curr_time}");
                    first_error = Some((0, series_id.clone(), msg));
                }
                per_series_errors += 1;
            }
        }
    }

    // Check minimum seconds requirement by computing actual time span from timestamps
    let mut min_seconds_errors = 0u128;
    if let Some(min_secs) = min_seconds
        && let (Some(&min_time), Some(&max_time)) = (
            fetch_index_to_time.values().min(),
            fetch_index_to_time.values().max(),
        )
    {
        // Convert milliseconds to seconds
        #[allow(clippy::cast_possible_truncation)]
        let time_span_seconds = ((max_time - min_time) / 1000) as u64;

        if time_span_seconds < min_secs {
            if first_error.is_none() {
                let msg = format!(
                    "Insufficient time span: expected >= {min_secs}s, got {time_span_seconds}s"
                );
                first_error = Some((0, "min_seconds".to_string(), msg));
            }
            min_seconds_errors = 1;
        }
        // NOTE 5 seconds is arbitrary, "large enough, but not too large".
        if time_span_seconds > (min_secs + 5) {
            if first_error.is_none() {
                let msg = format!(
                    "Invalid experiment duration, expected < {max}s, got {time_span_seconds}s",
                    max = min_secs + 5,
                );
                first_error = Some((0, "min_seconds".to_string(), msg));
            }
            min_seconds_errors = 1;
        }
    }

    ValidationResult {
        line_count: lines.len() as u128,
        unique_series: num_series,
        unique_fetch_indices: fetch_index_to_time.len(),
        fetch_index_errors,
        per_series_errors,
        min_seconds_errors,
        first_error,
    }
}
