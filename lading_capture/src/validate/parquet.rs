//! Parquet capture file validation
//!
//! This module provides efficient validation of parquet capture files using
//! Apache Arrow compute kernels. Unlike the row-based validation in `jsonl`,
//! this leverages columnar operations for better performance and memory efficiency.

use std::collections::hash_map::RandomState;
use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::hash::{BuildHasher, Hasher};
use std::path::Path;

use arrow_array::{Array, StringArray, TimestampMillisecondArray, UInt64Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::validate::ValidationResult;

/// Errors for parquet validation
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// Parquet error
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    /// Arrow error
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    /// Missing column
    #[error("Missing column: {0}")]
    MissingColumn(String),
    /// Invalid column type
    #[error("Invalid column type: {0}")]
    InvalidColumnType(String),
}

/// Validates a parquet capture file using columnar operations.
///
/// It uses a two-phase validation approach:
///
/// Phase 1, streaming: Validates global invariants that can fail fast:
//
/// - Each `fetch_index` is uniquely associated with exactly one timestamp
/// - Timestamps are non-negative
///
/// Phase 2, batch: Validates per-series invariants after sorting by
/// `fetch_index`:
//
/// - Within each series `fetch_index` values are strictly increasing
/// - Within each series `time` values are strictly increasing
///
/// Physical row order does not matter.
///
/// # Errors
///
/// Returns an error if the file cannot be opened or read, or if the parquet
/// schema doesn't match the expected capture format.
#[allow(clippy::cast_sign_loss)]
#[allow(clippy::too_many_lines)]
pub fn validate_parquet<P: AsRef<Path>>(
    path: P,
    min_seconds: Option<u64>,
) -> Result<ValidationResult, Error> {
    let file = File::open(path)?;
    let reader_builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = reader_builder.build()?;

    // Phase 1: Streaming assertions
    //
    // We make the following assertions in a streaming fashion (fail fast):
    //
    // - Each fetch_index maps to exactly one time value (global 1:1 mapping)
    // - Timestamps are non-negative
    //
    // We also collect (fetch_index, time) pairs for each series to validate
    // per-series invariants in Phase 2.
    let mut fetch_index_to_time: HashMap<u64, u128> = HashMap::new();
    let mut series_data: HashMap<u64, (Vec<(u64, u128)>, String)> = HashMap::new();
    let hash_builder = RandomState::new();

    let mut line_count = 0u128;
    let mut fetch_index_errors = 0u128;
    let mut first_error: Option<(u128, String, String)> = None;

    for batch_result in reader {
        let batch = batch_result?;
        let batch_len = batch.num_rows();

        if batch_len == 0 {
            continue;
        }

        let time_array = batch
            .column_by_name("time")
            .ok_or_else(|| Error::MissingColumn("time".to_string()))?
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| {
                Error::InvalidColumnType("'time' column is not TimestampMillisecond".to_string())
            })?;

        let fetch_index_array = batch
            .column_by_name("fetch_index")
            .ok_or_else(|| Error::MissingColumn("fetch_index".to_string()))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                Error::InvalidColumnType("'fetch_index' column is not UInt64".to_string())
            })?;

        let metric_name_array = batch
            .column_by_name("metric_name")
            .ok_or_else(|| Error::MissingColumn("metric_name".to_string()))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                Error::InvalidColumnType("'metric_name' column is not String".to_string())
            })?;

        // Collect l_* columns for label extraction (new schema uses flat columns)
        let schema = batch.schema();
        let l_columns: Vec<(&str, &StringArray)> = schema
            .fields()
            .iter()
            .filter_map(|field| {
                let name = field.name();
                if name.starts_with("l_") {
                    batch
                        .column_by_name(name)
                        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                        .map(|arr| (name.strip_prefix("l_").unwrap_or(name), arr))
                } else {
                    None
                }
            })
            .collect();

        let metric_kind_array = batch
            .column_by_name("metric_kind")
            .ok_or_else(|| Error::MissingColumn("metric_kind".to_string()))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                Error::InvalidColumnType("'metric_kind' column is not String".to_string())
            })?;

        // Validate invariants: fetch_index uniquely maps to time,
        // and per-series time and fetch_index are strictly increasing
        for row in 0..batch_len {
            let line_num = line_count + row as u128 + 1;
            let fetch_index = fetch_index_array.value(row);
            let time_raw = time_array.value(row);
            let metric_name = metric_name_array.value(row);
            let metric_kind_str = metric_kind_array.value(row);

            // Validate timestamp is non-negative
            if time_raw < 0 {
                if fetch_index_errors == 0 && first_error.is_none() {
                    let msg = format!("negative timestamp: {time_raw}");
                    first_error = Some((line_num, "negative timestamp".to_string(), msg));
                }
                fetch_index_errors += 1;
                continue;
            }
            let time = time_raw as u128;

            // Check fetch_index -> time mapping consistency
            if let Some(&existing_time) = fetch_index_to_time.get(&fetch_index) {
                if existing_time != time {
                    if fetch_index_errors == 0 && first_error.is_none() {
                        let msg = format!(
                            "fetch_index {fetch_index} appears with multiple times: {existing_time} and {time}"
                        );
                        first_error =
                            Some((line_num, "fetch_index/time mismatch".to_string(), msg));
                    }
                    fetch_index_errors += 1;
                }
            } else {
                fetch_index_to_time.insert(fetch_index, time);
            }

            // Extract labels from l_* columns
            let mut sorted_labels: BTreeSet<String> = BTreeSet::new();
            for (key, arr) in &l_columns {
                if !arr.is_null(row) {
                    let value = arr.value(row);
                    sorted_labels.insert(format!("{key}:{value}"));
                }
            }

            let mut hasher = hash_builder.build_hasher();
            hasher.write_usize(metric_name.len());
            hasher.write(metric_name.as_bytes());
            hasher.write_usize(metric_kind_str.len());
            hasher.write(metric_kind_str.as_bytes());
            for label in &sorted_labels {
                hasher.write_usize(label.len());
                hasher.write(label.as_bytes());
            }
            let series_key = hasher.finish();

            let series_id = format!(
                "{kind}:{metric}[{labels}]",
                kind = metric_kind_str,
                metric = metric_name,
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

        line_count += batch_len as u128;
    }

    // Phase 2: Per-series assertions (requires sorting)
    //
    // These assertions require data sorted by fetch_index within each series.
    // Rows can arrive in arbitrary physical order in the parquet file, so we
    // must collect all data first, then sort and validate.
    //
    // We verify:
    // - Within each series: fetch_index values are strictly increasing (no duplicates)
    // - Within each series: time values are strictly increasing when sorted by fetch_index
    let mut per_series_errors = 0u128;
    let num_series = series_data.len();
    for (_series_key, (mut data, series_id)) in series_data {
        data.sort_unstable_by_key(|(fetch_idx, _)| *fetch_idx);

        for window in data.windows(2) {
            let (prev_fetch_idx, prev_time) = window[0];
            let (curr_fetch_idx, curr_time) = window[1];

            if curr_fetch_idx <= prev_fetch_idx {
                if per_series_errors == 0 && first_error.is_none() {
                    let msg = format!(
                        "fetch_index not strictly increasing: prev={prev_fetch_idx}, curr={curr_fetch_idx}"
                    );
                    first_error = Some((0, series_id.clone(), msg));
                }
                per_series_errors += 1;
            }

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
        // NOTE 5 seconds is arbitrary: large enough, but not too large.
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

    Ok(ValidationResult {
        line_count,
        unique_series: num_series,
        unique_fetch_indices: fetch_index_to_time.len(),
        fetch_index_errors,
        per_series_errors,
        min_seconds_errors,
        first_error,
    })
}
