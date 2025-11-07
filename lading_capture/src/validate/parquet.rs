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

use arrow_array::{
    Array, MapArray, RecordBatch, StringArray, StructArray, TimestampMillisecondArray, UInt64Array,
};
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
    /// Invalid metric kind
    #[error("Invalid metric kind: {0}")]
    InvalidMetricKind(String),
}

/// Validates a parquet capture file using columnar operations.
///
/// This function streams through parquet row groups without loading the entire
/// file into memory, using Arrow compute kernels for validation checks.
///
/// # Errors
///
/// Returns an error if the file cannot be opened or read, or if the parquet
/// schema doesn't match the expected capture format.
#[allow(clippy::too_many_lines)]
#[allow(clippy::cast_sign_loss)]
pub fn validate_parquet<P: AsRef<Path>>(
    path: P,
    min_seconds: Option<u64>,
) -> Result<ValidationResult, Error> {
    let file = File::open(path)?;
    let reader_builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = reader_builder.build()?;

    let mut fetch_index_to_time: HashMap<u64, u128> = HashMap::new();
    let mut series_last_state: HashMap<u64, (u128, u64, String)> = HashMap::new();
    let hash_builder = RandomState::new();

    let mut line_count = 0u128;
    let mut fetch_index_errors = 0u128;
    let mut per_series_errors = 0u128;
    let mut first_error: Option<(u128, String, String)> = None;

    // Process each record batch (row group)
    for batch_result in reader {
        let batch = batch_result?;
        let batch_len = batch.num_rows();

        if batch_len == 0 {
            continue;
        }

        // Extract columns
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

        let metric_kind_array = batch
            .column_by_name("metric_kind")
            .ok_or_else(|| Error::MissingColumn("metric_kind".to_string()))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                Error::InvalidColumnType("'metric_kind' column is not String".to_string())
            })?;

        let labels_array = batch
            .column_by_name("labels")
            .ok_or_else(|| Error::MissingColumn("labels".to_string()))?
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| Error::InvalidColumnType("'labels' column is not Map".to_string()))?;

        // Validate global invariant: fetch_index -> time mapping
        for row in 0..batch_len {
            let line_num = line_count + row as u128 + 1;
            let fetch_index = fetch_index_array.value(row);
            let time = time_array.value(row) as u128;

            if let Some(&existing_time) = fetch_index_to_time.get(&fetch_index) {
                if existing_time != time {
                    if fetch_index_errors == 0 {
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
        }

        // Validate per-series invariants: time and fetch_index strictly
        // increasing
        let series_errors = validate_per_series_invariants(
            &batch,
            time_array,
            fetch_index_array,
            metric_name_array,
            metric_kind_array,
            labels_array,
            &mut series_last_state,
            &hash_builder,
            line_count,
            &mut first_error,
        )?;

        per_series_errors += series_errors;
        line_count += batch_len as u128;
    }

    // Check minimum seconds requirement, if specified
    let mut min_seconds_errors = 0u128;
    if let Some(min_secs) = min_seconds {
        #[allow(clippy::cast_possible_truncation)]
        let unique_seconds = fetch_index_to_time.len() as u64;
        if unique_seconds < min_secs {
            if first_error.is_none() {
                let msg = format!(
                    "Insufficient timestamps: expected >= {min_secs} unique timestamps for {min_secs}s experiment, got {unique_seconds}"
                );
                first_error = Some((0, "min_seconds".to_string(), msg));
            }
            min_seconds_errors = 1;
        }
        // NOTE 5 seconds is arbitrary, "large enough, but not too large".
        if unique_seconds > (min_secs + 5) {
            if first_error.is_none() {
                let msg = format!(
                    "Invalid experiment duration, expected < {max} unique timestamps for {min_secs}s experiment, got {unique_seconds}",
                    max = min_secs + 5,
                );
                first_error = Some((0, "min_seconds".to_string(), msg));
            }
            min_seconds_errors = 1;
        }
    }

    Ok(ValidationResult {
        line_count,
        unique_series: series_last_state.len(),
        unique_fetch_indices: fetch_index_to_time.len(),
        fetch_index_errors,
        per_series_errors,
        min_seconds_errors,
        first_error,
    })
}

/// Validates per-series invariants within a record batch.
///
/// Checks that time and `fetch_index` are strictly increasing within each
/// series, maintaining state across batches.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
#[allow(clippy::cast_sign_loss)]
fn validate_per_series_invariants(
    _batch: &RecordBatch,
    time_array: &TimestampMillisecondArray,
    fetch_index_array: &UInt64Array,
    metric_name_array: &StringArray,
    metric_kind_array: &StringArray,
    labels_array: &MapArray,
    series_last_state: &mut HashMap<u64, (u128, u64, String)>,
    hash_builder: &RandomState,
    line_offset: u128,
    first_error: &mut Option<(u128, String, String)>,
) -> Result<u128, Error> {
    let mut error_count = 0u128;

    // Process each row and validate per-series invariants. IF this ends up
    // being a performance drag, sort the series and then partition().
    for row in 0..time_array.len() {
        let line_num = line_offset + row as u128 + 1;
        let time = time_array.value(row) as u128;
        let fetch_index = fetch_index_array.value(row);
        let metric_name = metric_name_array.value(row);
        let metric_kind_str = metric_kind_array.value(row);

        let mut hasher = hash_builder.build_hasher();
        hasher.write_usize(metric_name.len());
        hasher.write(metric_name.as_bytes());

        let kind_byte = match metric_kind_str {
            "counter" => 0u8,
            "gauge" => 1u8,
            _ => {
                return Err(Error::InvalidMetricKind(metric_kind_str.to_string()));
            }
        };
        hasher.write_u8(kind_byte);

        let mut sorted_labels: BTreeSet<String> = BTreeSet::new();
        let labels_slice: StructArray = labels_array.value(row);

        let key_array = labels_slice
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                Error::InvalidColumnType("Labels keys are not StringArray".to_string())
            })?;
        let value_array = labels_slice
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                Error::InvalidColumnType("Labels values are not StringArray".to_string())
            })?;

        for i in 0..key_array.len() {
            let key = key_array.value(i);
            let value = value_array.value(i);
            sorted_labels.insert(format!("{key}:{value}"));
        }

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

        if let Some((prev_time, prev_fetch_index, _)) = series_last_state.get(&series_key) {
            if time <= *prev_time {
                if error_count == 0 && first_error.is_none() {
                    let msg =
                        format!("time not strictly increasing: prev={prev_time}, curr={time}");
                    *first_error = Some((line_num, series_id.clone(), msg));
                }
                error_count += 1;
            }
            if fetch_index <= *prev_fetch_index {
                if error_count == 0 && first_error.is_none() {
                    let msg = format!(
                        "fetch_index not strictly increasing: prev={prev_fetch_index}, curr={fetch_index}"
                    );
                    *first_error = Some((line_num, series_id.clone(), msg));
                }
                error_count += 1;
            }
        }

        series_last_state.insert(series_key, (time, fetch_index, series_id));
    }

    Ok(error_count)
}
