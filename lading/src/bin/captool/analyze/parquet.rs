//! Columnar analysis for parquet capture files
//!
//! This module provides analysis operations on parquet capture files using
//! Apache Arrow, staying in columnar format for efficiency.

use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::path::Path;

use arrow_array::{Array, Float64Array, MapArray, StringArray, UInt64Array};
use lading_capture_schema::columns;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::{MetricInfo, SeriesStats};

/// Errors for parquet analysis
#[derive(thiserror::Error, Debug)]
pub(crate) enum Error {
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

/// Lists all unique metrics in a parquet capture file.
///
/// Returns a sorted list of (kind, name) tuples.
///
/// # Errors
///
/// Returns an error if the file cannot be read or has invalid schema.
pub(crate) fn list_metrics<P: AsRef<Path>>(path: P) -> Result<Vec<MetricInfo>, Error> {
    let file = File::open(path)?;
    let reader_builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = reader_builder.build()?;

    let mut metrics: BTreeSet<MetricInfo> = BTreeSet::new();

    for batch_result in reader {
        let batch = batch_result?;
        if batch.num_rows() == 0 {
            continue;
        }

        let metric_name_array = batch
            .column_by_name(columns::METRIC_NAME)
            .ok_or_else(|| Error::MissingColumn(columns::METRIC_NAME.to_string()))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                Error::InvalidColumnType(format!("'{}' is not String", columns::METRIC_NAME))
            })?;

        let metric_kind_array = batch
            .column_by_name(columns::METRIC_KIND)
            .ok_or_else(|| Error::MissingColumn(columns::METRIC_KIND.to_string()))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                Error::InvalidColumnType(format!("'{}' is not String", columns::METRIC_KIND))
            })?;

        for row in 0..batch.num_rows() {
            let name = metric_name_array.value(row).to_string();
            let kind = metric_kind_array.value(row).to_string();
            metrics.insert(MetricInfo { kind, name });
        }
    }

    Ok(metrics.into_iter().collect())
}

/// Analyzes a specific metric in a parquet capture file.
///
/// Returns statistics grouped by label set (context).
///
/// # Errors
///
/// Returns an error if the file cannot be read or has invalid schema.
#[allow(clippy::too_many_lines)]
#[allow(clippy::cast_precision_loss)]
pub(crate) fn analyze_metric<P: AsRef<Path>>(
    path: P,
    metric_name: &str,
) -> Result<HashMap<BTreeSet<String>, SeriesStats>, Error> {
    let file = File::open(path)?;
    let reader_builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = reader_builder.build()?;

    let mut series_map: HashMap<BTreeSet<String>, Vec<(u64, f64)>> = HashMap::new();

    for batch_result in reader {
        let batch = batch_result?;
        if batch.num_rows() == 0 {
            continue;
        }

        let name_array = batch
            .column_by_name(columns::METRIC_NAME)
            .ok_or_else(|| Error::MissingColumn(columns::METRIC_NAME.to_string()))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                Error::InvalidColumnType(format!("'{}' is not String", columns::METRIC_NAME))
            })?;

        let fetch_index_array = batch
            .column_by_name(columns::FETCH_INDEX)
            .ok_or_else(|| Error::MissingColumn(columns::FETCH_INDEX.to_string()))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                Error::InvalidColumnType(format!("'{}' is not UInt64", columns::FETCH_INDEX))
            })?;

        let value_int_array = batch
            .column_by_name(columns::VALUE_INT)
            .ok_or_else(|| Error::MissingColumn(columns::VALUE_INT.to_string()))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                Error::InvalidColumnType(format!("'{}' is not UInt64", columns::VALUE_INT))
            })?;

        let value_float_array = batch
            .column_by_name(columns::VALUE_FLOAT)
            .ok_or_else(|| Error::MissingColumn(columns::VALUE_FLOAT.to_string()))?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                Error::InvalidColumnType(format!("'{}' is not Float64", columns::VALUE_FLOAT))
            })?;

        let labels_array = batch
            .column_by_name(columns::LABELS)
            .ok_or_else(|| Error::MissingColumn(columns::LABELS.to_string()))?
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| Error::InvalidColumnType(format!("'{}' is not Map", columns::LABELS)))?;

        // Filter to only rows matching the metric name
        for row in 0..batch.num_rows() {
            if name_array.value(row) != metric_name {
                continue;
            }

            let fetch_index = fetch_index_array.value(row);

            // Extract value (int or float)
            let value = if value_int_array.is_valid(row) {
                value_int_array.value(row) as f64
            } else if value_float_array.is_valid(row) {
                value_float_array.value(row)
            } else {
                continue; // Skip if both null
            };

            // Extract labels
            let mut sorted_labels = BTreeSet::new();
            let labels_slice = labels_array.value(row);
            let key_array = labels_slice
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::InvalidColumnType("Label keys not String".to_string()))?;
            let value_array = labels_slice
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::InvalidColumnType("Label values not String".to_string()))?;

            for i in 0..key_array.len() {
                let key = key_array.value(i);
                let val = value_array.value(i);
                sorted_labels.insert(format!("{key}:{val}"));
            }

            series_map
                .entry(sorted_labels)
                .or_default()
                .push((fetch_index, value));
        }
    }

    // Compute statistics for each series
    let mut results = HashMap::new();
    for (labels, mut values) in series_map {
        if values.is_empty() {
            continue;
        }

        // Sort by fetch_index for monotonicity check
        values.sort_by_key(|(fetch_idx, _)| *fetch_idx);

        let value_floats: Vec<f64> = values.iter().map(|(_, v)| *v).collect();

        let min = value_floats.iter().copied().fold(f64::INFINITY, f64::min);
        let max = value_floats
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);
        let sum: f64 = value_floats.iter().sum();
        let mean = sum / value_floats.len() as f64;

        let is_monotonic = value_floats.windows(2).all(|w| w[0] <= w[1]);

        results.insert(
            labels.clone(),
            SeriesStats {
                labels,
                min,
                max,
                mean,
                is_monotonic,
                values,
            },
        );
    }

    Ok(results)
}
