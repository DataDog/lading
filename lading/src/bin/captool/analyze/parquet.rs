//! Columnar analysis for parquet capture files
//!
//! This module provides analysis operations on parquet capture files using
//! Apache Arrow, staying in columnar format for efficiency.

use std::collections::BTreeSet;
use std::fs::File;
use std::path::Path;

use arrow_array::{Array, Float64Array, MapArray, StringArray, UInt64Array};
use lading_capture::formats::parquet::{LabelError, ReadError, reader_builder, resolve_labels};
use lading_capture_schema::columns;
use rustc_hash::FxHashMap;

use super::{MetricInfo, SeriesStats};

/// Errors for parquet analysis
#[derive(thiserror::Error, Debug)]
pub(crate) enum Error {
    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// Capture metadata or declared schema error.
    #[error(transparent)]
    Capture(#[from] ReadError),
    /// Label columns do not match the selected schema major version.
    #[error("version mismatch: {0}")]
    VersionMismatch(String),
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

impl From<LabelError> for Error {
    fn from(error: LabelError) -> Self {
        Self::VersionMismatch(error.to_string())
    }
}

/// Lists all unique metrics in a parquet capture file.
///
/// Returns a sorted list of (kind, name) tuples.
///
/// # Errors
///
/// Returns an error if the file cannot be read or has invalid schema.
pub(crate) fn list_metrics<P: AsRef<Path>>(
    path: P,
    schema_major_version: u64,
) -> Result<Vec<MetricInfo>, Error> {
    let file = File::open(path)?;
    let reader = reader_builder(file, schema_major_version)?.build()?;

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
#[expect(clippy::too_many_lines)]
#[expect(clippy::cast_precision_loss)]
pub(crate) fn analyze_metric<P: AsRef<Path>>(
    path: P,
    metric_name: &str,
    schema_major_version: u64,
) -> Result<FxHashMap<BTreeSet<String>, SeriesStats>, Error> {
    let file = File::open(path)?;
    let reader = reader_builder(file, schema_major_version)?.build()?;

    let mut series_map: FxHashMap<BTreeSet<String>, Vec<(u64, f64)>> = FxHashMap::default();

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
            let key_array =
                resolve_labels(labels_slice.column(0), "Label keys", schema_major_version)?;
            let value_array =
                resolve_labels(labels_slice.column(1), "Label values", schema_major_version)?;

            for (key, val) in key_array.iter().zip(value_array.iter()) {
                sorted_labels.insert(format!("{key}:{val}"));
            }

            series_map
                .entry(sorted_labels)
                .or_default()
                .push((fetch_index, value));
        }
    }

    // Compute statistics for each series
    let mut results = FxHashMap::default();
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

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;

    use lading_capture::{
        formats::parquet::Format,
        line::{Line, LineValue, MetricKind},
    };
    use lading_capture_schema::CURRENT_CAPTURE_VERSION;
    use tempfile::NamedTempFile;

    fn fixture(major: u64) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(format!("../lading_capture/tests/fixtures/v{major}.parquet"))
    }

    /// For all frozen majors M: captool reads the M.0 capture only with schema major version M.
    #[test]
    fn frozen_captures_need_their_selected_major() -> Result<(), Box<dyn std::error::Error>> {
        for (major, other) in [(1, 2), (2, 1)] {
            assert_eq!(
                list_metrics(fixture(major), major)?,
                vec![MetricInfo {
                    kind: "counter".to_string(),
                    name: "requests".to_string(),
                }]
            );
            let series = analyze_metric(fixture(major), "requests", major)?;
            assert_eq!(series.len(), 2);
            let prod_key = ["env:prod".to_string(), "service:api".to_string()].into();
            let prod = series.get(&prod_key).ok_or("missing prod series")?;
            assert_eq!(prod.values, vec![(0, 10.0), (2, 30.0)]);
            assert!(matches!(
                list_metrics(fixture(major), other),
                Err(Error::Capture(ReadError::VersionMismatch { .. }))
            ));
            assert!(matches!(
                analyze_metric(fixture(major), "requests", other),
                Err(Error::Capture(ReadError::VersionMismatch { .. }))
            ));
        }
        Ok(())
    }

    /// For all writer output read with `CURRENT_CAPTURE_VERSION.major`: each distinct label set
    /// is one series, and its statistics equal those of its input values.
    #[test]
    #[expect(clippy::float_cmp, reason = "Statistics use exact integer inputs.")]
    fn analyze_metric_reads_writer_output() -> Result<(), Box<dyn std::error::Error>> {
        let run_id = "12345678-9abc-def0-1234-56789abcdef0".parse()?;
        let capture = NamedTempFile::new()?;
        let mut writer = Format::new(capture.reopen()?, 3)?;
        for (env, fetch_index, value) in [
            ("prod", 0u64, 10),
            ("prod", 1, 20),
            ("prod", 2, 30),
            ("staging", 0, 100),
            ("staging", 1, 200),
        ] {
            writer.write_metric(&Line {
                run_id,
                time: 1_000 + u128::from(fetch_index),
                fetch_index,
                metric_name: "requests".to_string(),
                metric_kind: MetricKind::Counter,
                value: LineValue::Int(value),
                labels: [
                    ("env".to_string(), env.to_string()),
                    ("service".to_string(), "api".to_string()),
                ]
                .into_iter()
                .collect(),
                value_histogram: Vec::new(),
            })?;
        }
        writer.close()?;

        assert_eq!(
            list_metrics(capture.path(), CURRENT_CAPTURE_VERSION.major)?,
            vec![MetricInfo {
                kind: "counter".to_string(),
                name: "requests".to_string(),
            }]
        );

        let series = analyze_metric(capture.path(), "requests", CURRENT_CAPTURE_VERSION.major)?;
        assert_eq!(series.len(), 2);

        let prod_key = ["env:prod".to_string(), "service:api".to_string()].into();
        let staging_key = ["env:staging".to_string(), "service:api".to_string()].into();

        let prod_stats = series.get(&prod_key).ok_or("missing prod series")?;
        assert_eq!(prod_stats.min, 10.0);
        assert_eq!(prod_stats.max, 30.0);
        assert_eq!(prod_stats.mean, 20.0);
        assert!(prod_stats.is_monotonic);

        let staging_stats = series.get(&staging_key).ok_or("missing staging series")?;
        assert_eq!(staging_stats.min, 100.0);
        assert_eq!(staging_stats.max, 200.0);
        assert_eq!(staging_stats.mean, 150.0);
        assert!(staging_stats.is_monotonic);
        Ok(())
    }
}
