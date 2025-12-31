//! Output format abstraction for capture files
//!
//! This module provides a trait-based abstraction for capture output
//! formats.

use crate::line;

pub mod jsonl;
pub mod multi;
pub mod parquet;

/// Format operation errors
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// JSONL format errors
    #[error("JSONL format error: {0}")]
    Jsonl(#[from] jsonl::Error),
    /// Multi format errors
    #[error("Multi format error: {0}")]
    Multi(#[from] multi::Error),
    /// Parquet format errors
    #[error("Parquet format error: {0}")]
    Parquet(#[from] parquet::Error),
    /// IO errors during write operations
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    /// Rotation not supported for this format
    #[error("File rotation not supported for this format")]
    RotationNotSupported,
}

/// Trait for output format implementations
///
/// Implementations of this trait handle the serialization and writing of
/// metrics to a specific file format. The capture manager's state machine uses
/// this trait to remain agnostic to the output format.
///
/// Histogram values in `Line::value_histogram` are stored as protobuf-serialized
/// bytes (via `Dogsketch::write_to_bytes`). Both JSONL and Parquet formats store
/// the same protobuf bytes - JSONL base64-encodes them in JSON, Parquet stores
/// them as binary.
pub trait OutputFormat {
    /// Write a single metric line to the output
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or writing fails.
    fn write_metric(&mut self, line: &line::Line) -> Result<(), Error>;

    /// Flush any buffered data to disk
    ///
    /// # Errors
    ///
    /// Returns an error if flushing fails.
    fn flush(&mut self) -> Result<(), Error>;

    /// Close and finalize the output format
    ///
    /// This method must be called to properly finalize the output file. For
    /// formats like Parquet, this writes critical metadata (file footer). For
    /// simpler formats like JSONL, this ensures all buffered data is written.
    ///
    /// Consumes the format as it can no longer be used after closing.
    ///
    /// # Errors
    ///
    /// Returns an error if closing fails.
    fn close(self) -> Result<(), Error>;

    /// Rotate to a new output file
    ///
    /// Flushes and closes the current file (writing footer for Parquet), then
    /// opens a new file at the specified path. This allows continuous metrics
    /// collection while producing multiple readable output files.
    ///
    /// The default implementation returns `RotationNotSupported`. Formats that
    /// support rotation (like Parquet with file-based writers) should override.
    ///
    /// # Arguments
    ///
    /// * `path` - Path for the new output file
    ///
    /// # Errors
    ///
    /// Returns an error if rotation is not supported or if file operations fail.
    fn rotate(self, _path: std::path::PathBuf) -> Result<Self, Error>
    where
        Self: Sized,
    {
        Err(Error::RotationNotSupported)
    }
}

#[cfg(test)]
mod tests {
    use super::jsonl;
    use crate::line::{Line, LineValue, MetricKind};
    use approx::relative_eq;
    use arrow_array::{
        Array, BinaryArray, Float64Array, StringArray, TimestampMillisecondArray, UInt64Array,
    };
    use bytes::Bytes;
    use datadog_protos::metrics::Dogsketch;
    use ddsketch_agent::DDSketch;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use proptest::prelude::*;
    use protobuf::Message;
    use rustc_hash::FxHashMap;
    use std::io::{BufRead, BufReader, Cursor};
    use uuid::Uuid;

    fn sketch_to_protobuf(sketch: &DDSketch) -> Vec<u8> {
        let mut dogsketch = Dogsketch::new();
        sketch.merge_to_dogsketch(&mut dogsketch);
        dogsketch
            .write_to_bytes()
            .expect("protobuf serialization should succeed")
    }

    fn assert_lines_equal(a: &Line, b: &Line) -> Result<(), proptest::test_runner::TestCaseError> {
        prop_assert_eq!(a.run_id, b.run_id);
        prop_assert_eq!(a.time, b.time);
        prop_assert_eq!(a.fetch_index, b.fetch_index);
        prop_assert_eq!(&a.metric_name, &b.metric_name);
        prop_assert_eq!(a.metric_kind, b.metric_kind);

        match (a.value, b.value) {
            (LineValue::Int(x), LineValue::Int(y)) => prop_assert_eq!(x, y),
            (LineValue::Float(x), LineValue::Float(y)) => {
                // For very large or very small floats, JSON serialization can
                // introduce precision loss due to decimal representation. Use
                // relative comparison with a tolerance appropriate for f64 precision.
                // The max_relative of 1e-12 allows for the precision loss inherent
                // in the binary<->decimal conversion while still catching actual bugs.
                prop_assert!(
                    relative_eq!(x, y, max_relative = 1e-12),
                    "floats not equal: {x} vs {y}"
                );
            }
            (x, y) => prop_assert!(false, "value types don't match: {x:?} vs {y:?}"),
        }

        prop_assert_eq!(a.labels.len(), b.labels.len());
        for (k, v) in &a.labels {
            prop_assert_eq!(b.labels.get(k), Some(v));
        }

        // For histograms, verify protobuf bytes and DDSketch properties
        prop_assert_eq!(
            &a.value_histogram,
            &b.value_histogram,
            "Histogram protobuf bytes differ"
        );

        if !a.value_histogram.is_empty() {
            let a_sketch = Dogsketch::parse_from_bytes(&a.value_histogram)
                .ok()
                .and_then(|ds| DDSketch::try_from(ds).ok());
            let b_sketch = Dogsketch::parse_from_bytes(&b.value_histogram)
                .ok()
                .and_then(|ds| DDSketch::try_from(ds).ok());

            if let (Some(a_sketch), Some(b_sketch)) = (a_sketch, b_sketch) {
                prop_assert_eq!(a_sketch.count(), b_sketch.count());

                if a_sketch.count() > 0 {
                    let a_min = a_sketch.min().unwrap_or(f64::NAN);
                    let b_min = b_sketch.min().unwrap_or(f64::NAN);
                    prop_assert!(
                        relative_eq!(a_min, b_min, epsilon = 1e-10)
                            || (a_min.is_nan() && b_min.is_nan())
                    );

                    let a_max = a_sketch.max().unwrap_or(f64::NAN);
                    let b_max = b_sketch.max().unwrap_or(f64::NAN);
                    prop_assert!(
                        relative_eq!(a_max, b_max, epsilon = 1e-10)
                            || (a_max.is_nan() && b_max.is_nan())
                    );
                }
            }
        }

        Ok(())
    }

    proptest! {
        #[test]
        fn jsonl_round_trip_identity(
            lines in prop::collection::vec(
                prop_oneof![
                    // Counter or Gauge metrics (no histogram)
                    (
                        any::<u128>(),
                        any::<u64>(),
                        "[a-z][a-z0-9_]*",
                        prop_oneof![
                            Just(MetricKind::Counter),
                            Just(MetricKind::Gauge),
                        ],
                        prop_oneof![
                            any::<u64>().prop_map(LineValue::Int),
                            any::<f64>().prop_filter("finite", |f| f.is_finite())
                                .prop_map(LineValue::Float),
                        ],
                        prop::collection::hash_map("[a-z][a-z0-9_]*", "[a-z][a-z0-9_]*", 0..5),
                        Just(Vec::new()),
                    ),
                    // Histogram metrics with DDSketch data - comprehensive edge cases
                    (
                        any::<u128>(),
                        any::<u64>(),
                        "[a-z][a-z0-9_]*",
                        Just(MetricKind::Histogram),
                        Just(LineValue::Float(0.0)),
                        prop::collection::hash_map("[a-z][a-z0-9_]*", "[a-z][a-z0-9_]*", 0..5),
                        prop_oneof![
                            // Empty histogram (should not be written per accumulator.rs:508)
                            Just(Vec::new()),
                            // Histograms with unconstrained finite f64 values
                            prop::collection::vec(
                                any::<f64>().prop_filter("finite", |f| f.is_finite()),
                                1..100
                            ).prop_map(|samples| {
                                let mut sketch = DDSketch::default();
                                for sample in samples {
                                    sketch.insert(sample);
                                }
                                sketch_to_protobuf(&sketch)
                            }),
                        ],
                    ),
                ],
                1..10
            )
        ) {
            let run_id = Uuid::new_v4();
            let input_lines: Vec<Line> = lines
                .into_iter()
                .map(|(time, fetch_index, metric_name, metric_kind, value, labels, value_histogram)| Line {
                    run_id,
                    time,
                    fetch_index,
                    metric_name,
                    metric_kind,
                    value,
                    labels: labels.into_iter().collect(),
                    value_histogram,
                })
                .collect();

            let mut buffer = Vec::new();
            {
                let mut writer = jsonl::Format::new(&mut buffer);
                for line in &input_lines {
                    writer.write_metric(line).expect("write");
                }
                writer.close().expect("close");
            }

            let deserialized_lines: Vec<Line> = BufReader::new(Cursor::new(&buffer))
                .lines()
                .map(|line| serde_json::from_str(&line.expect("read line")).expect("parse"))
                .collect();

            prop_assert_eq!(input_lines.len(), deserialized_lines.len());
            for (input, output) in input_lines.iter().zip(deserialized_lines.iter()) {
                assert_lines_equal(input, output)?;
            }
        }
    }

    proptest! {
        #[test]
        fn parquet_round_trip_identity(
            lines in prop::collection::vec(
                prop_oneof![
                    // Counter or Gauge metrics (no histogram)
                    (
                        0u128..=(i64::MAX as u128),
                        any::<u64>(),
                        "[a-z][a-z0-9_]*",
                        prop_oneof![
                            Just(MetricKind::Counter),
                            Just(MetricKind::Gauge),
                        ],
                        prop_oneof![
                            any::<u64>().prop_map(LineValue::Int),
                            any::<f64>().prop_filter("finite", |f| f.is_finite())
                                .prop_map(LineValue::Float),
                        ],
                        prop::collection::hash_map("[a-z][a-z0-9_]*", "[a-z][a-z0-9_]*", 0..5),
                        Just(Vec::new()),
                    ),
                    // Histogram metrics with DDSketch data - comprehensive edge cases
                    (
                        0u128..=(i64::MAX as u128),
                        any::<u64>(),
                        "[a-z][a-z0-9_]*",
                        Just(MetricKind::Histogram),
                        Just(LineValue::Float(0.0)),
                        prop::collection::hash_map("[a-z][a-z0-9_]*", "[a-z][a-z0-9_]*", 0..5),
                        prop_oneof![
                            // Empty histogram
                            Just(Vec::new()),
                            // Single sample (protobuf for Parquet)
                            any::<f64>().prop_filter("finite", |f| f.is_finite())
                                .prop_map(|sample| {
                                    let mut sketch = DDSketch::default();
                                    sketch.insert(sample);
                                    sketch_to_protobuf(&sketch)
                                }),
                            // Small histogram (typical case)
                            prop::collection::vec(
                                any::<f64>().prop_filter("finite", |f| f.is_finite()),
                                2..20
                            ).prop_map(|samples| {
                                let mut sketch = DDSketch::default();
                                for sample in samples {
                                    sketch.insert(sample);
                                }
                                sketch_to_protobuf(&sketch)
                            }),
                            // Large histogram (stress test)
                            prop::collection::vec(
                                any::<f64>().prop_filter("finite", |f| f.is_finite()),
                                100..1000
                            ).prop_map(|samples| {
                                let mut sketch = DDSketch::default();
                                for sample in samples {
                                    sketch.insert(sample);
                                }
                                sketch_to_protobuf(&sketch)
                            }),
                            // All zeros
                            prop::collection::vec(Just(0.0), 1..10)
                                .prop_map(|samples| {
                                    let mut sketch = DDSketch::default();
                                    for sample in samples {
                                        sketch.insert(sample);
                                    }
                                    sketch_to_protobuf(&sketch)
                                }),
                            // All negative
                            prop::collection::vec(
                                -1000.0f64..-1.0f64,
                                1..10
                            ).prop_map(|samples| {
                                let mut sketch = DDSketch::default();
                                for sample in samples {
                                    sketch.insert(sample);
                                }
                                sketch_to_protobuf(&sketch)
                            }),
                            // Extreme values that test DDSketch limits
                            prop::collection::vec(
                                prop_oneof![
                                    Just(f64::MIN_POSITIVE),  // Smallest positive
                                    Just(1e-300),             // Near-zero positive
                                    Just(-1e-300),            // Near-zero negative
                                    Just(1e100),              // Large positive
                                    Just(-1e100),             // Large negative
                                ],
                                1..5
                            ).prop_map(|samples| {
                                let mut sketch = DDSketch::default();
                                for sample in samples {
                                    sketch.insert(sample);
                                }
                                sketch_to_protobuf(&sketch)
                            }),
                            // Mix of very close values (tests bin resolution)
                            prop::collection::vec(
                                Just(1.0),
                                1..10
                            ).prop_map(|samples| {
                                let mut sketch = DDSketch::default();
                                for (i, _) in samples.iter().enumerate() {
                                    // Insert values very close together
                                    sketch.insert(1.0 + (i as f64 * 1e-10));
                                }
                                sketch_to_protobuf(&sketch)
                            }),
                        ],
                    ),
                ],
                1..10
            )
        ) {
            let run_id = Uuid::new_v4();
            let input_lines: Vec<Line> = lines
                .into_iter()
                .map(|(time, fetch_index, metric_name, metric_kind, value, labels, value_histogram)| Line {
                    run_id,
                    time,
                    fetch_index,
                    metric_name,
                    metric_kind,
                    value,
                    labels: labels.into_iter().collect(),
                    value_histogram,
                })
                .collect();

            let mut buffer = Cursor::new(Vec::new());
            {
                let mut writer = super::parquet::Format::new(&mut buffer, 3).expect("create writer");
                for line in &input_lines {
                    writer.write_metric(line).expect("write");
                }
                writer.flush().expect("flush");
                writer.close().expect("close");
            }
            let bytes = buffer.into_inner();

            let deserialized_lines = read_parquet_lines(&bytes).expect("read parquet");

            prop_assert_eq!(input_lines.len(), deserialized_lines.len());
            for (input, output) in input_lines.iter().zip(deserialized_lines.iter()) {
                assert_lines_equal(input, output)?;
            }
        }
    }

    fn read_parquet_lines(bytes: &[u8]) -> Result<Vec<Line>, Box<dyn std::error::Error>> {
        let bytes_buf = Bytes::copy_from_slice(bytes);
        let reader_builder = ParquetRecordBatchReaderBuilder::try_new(bytes_buf)?;
        let reader = reader_builder.build()?;

        let mut lines = Vec::new();

        for batch_result in reader {
            let batch = batch_result?;
            let batch_len = batch.num_rows();

            if batch_len == 0 {
                continue;
            }

            let run_id_array = batch
                .column_by_name("run_id")
                .expect("run_id column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("run_id is StringArray");

            let time_array = batch
                .column_by_name("time")
                .expect("time column")
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("time is TimestampMillisecondArray");

            let fetch_index_array = batch
                .column_by_name("fetch_index")
                .expect("fetch_index column")
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("fetch_index is UInt64Array");

            let metric_name_array = batch
                .column_by_name("metric_name")
                .expect("metric_name column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("metric_name is StringArray");

            let metric_kind_array = batch
                .column_by_name("metric_kind")
                .expect("metric_kind column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("metric_kind is StringArray");

            let value_int_array = batch
                .column_by_name("value_int")
                .expect("value_int column")
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("value_int is UInt64Array");

            let value_float_array = batch
                .column_by_name("value_float")
                .expect("value_float column")
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("value_float is Float64Array");

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

            let value_histogram_array = batch
                .column_by_name("value_histogram")
                .expect("value_histogram column")
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("value_histogram is BinaryArray");

            for row_idx in 0..batch_len {
                let run_id = Uuid::parse_str(run_id_array.value(row_idx)).expect("parse UUID");

                // Parquet stores timestamps as non-negative milliseconds since epoch
                #[allow(clippy::cast_sign_loss)]
                let time = time_array.value(row_idx) as u128;

                let fetch_index = fetch_index_array.value(row_idx);
                let metric_name = metric_name_array.value(row_idx).to_string();

                let metric_kind = match metric_kind_array.value(row_idx) {
                    "counter" => MetricKind::Counter,
                    "gauge" => MetricKind::Gauge,
                    "histogram" => MetricKind::Histogram,
                    kind => panic!("unknown metric kind: {kind}"),
                };

                let value = if value_int_array.is_null(row_idx) {
                    LineValue::Float(value_float_array.value(row_idx))
                } else {
                    LineValue::Int(value_int_array.value(row_idx))
                };

                // Extract labels from l_* columns
                let mut labels = FxHashMap::default();
                for (key, arr) in &l_columns {
                    if !arr.is_null(row_idx) {
                        labels.insert((*key).to_string(), arr.value(row_idx).to_string());
                    }
                }

                let value_histogram = if value_histogram_array.is_null(row_idx) {
                    Vec::new()
                } else {
                    value_histogram_array.value(row_idx).to_vec()
                };

                lines.push(Line {
                    run_id,
                    time,
                    fetch_index,
                    metric_name,
                    metric_kind,
                    value,
                    labels,
                    value_histogram,
                });
            }
        }

        Ok(lines)
    }
}
