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
}

/// Trait for output format implementations
///
/// Implementations of this trait handle the serialization and writing of
/// metrics to a specific file format. The capture manager's state machine uses
/// this trait to remain agnostic to the output format.
///
/// Histogram values in `Line::value_histogram` are stored as JSON-encoded bytes.
/// JSONL displays as human-readable JSON. Parquet stores as compact binary.
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

    /// Serialize a `DDSketch` histogram for this format
    ///
    /// JSONL uses JSON serialization. Parquet uses protobuf for smaller file size.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    fn serialize_sketch(&self, sketch: &ddsketch_agent::DDSketch) -> Result<Vec<u8>, Error>;
}

#[cfg(test)]
mod tests {
    use super::jsonl;
    use crate::line::{Line, LineValue, MetricKind};
    use approx::relative_eq;
    use arrow_array::{
        Array, BinaryArray, Float64Array, MapArray, StringArray, StructArray,
        TimestampMillisecondArray, UInt64Array,
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

    fn sketch_to_json(sketch: &DDSketch) -> Vec<u8> {
        serde_json::to_vec(sketch).expect("JSON serialization should succeed")
    }

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

        // For histograms, verify DDSketch integrity beyond just byte equality
        match (&a.value_histogram, &b.value_histogram) {
            (None, None) => {} // Both empty - OK
            (Some(a_bytes), Some(b_bytes)) => {
                // Verify bytes are identical for round-trip
                prop_assert_eq!(a_bytes, b_bytes, "Histogram bytes differ after round-trip");

                // Try to deserialize (try both JSON and protobuf formats)
                // Parquet uses protobuf, JSONL uses JSON
                let (a_sketch_opt, b_sketch_opt) = if let (Ok(proto_a), Ok(proto_b)) = (
                    Dogsketch::parse_from_bytes(a_bytes),
                    Dogsketch::parse_from_bytes(b_bytes),
                ) {
                    // Protobuf format (Parquet)
                    (
                        DDSketch::try_from(proto_a).ok(),
                        DDSketch::try_from(proto_b).ok(),
                    )
                } else {
                    // JSON format (JSONL)
                    (
                        serde_json::from_slice::<DDSketch>(a_bytes).ok(),
                        serde_json::from_slice::<DDSketch>(b_bytes).ok(),
                    )
                };

                if let (Some(a_sketch), Some(b_sketch)) = (a_sketch_opt, b_sketch_opt) {
                    // Verify sketch properties are preserved
                    prop_assert_eq!(
                        a_sketch.count(),
                        b_sketch.count(),
                        "Sketch sample counts differ: input={} output={}",
                        a_sketch.count(),
                        b_sketch.count()
                    );

                    // Verify min/max if sketch has samples
                    if a_sketch.count() > 0 {
                        let a_min = a_sketch.min().unwrap_or(f64::NAN);
                        let b_min = b_sketch.min().unwrap_or(f64::NAN);
                        let a_max = a_sketch.max().unwrap_or(f64::NAN);
                        let b_max = b_sketch.max().unwrap_or(f64::NAN);

                        prop_assert!(
                            relative_eq!(a_min, b_min, epsilon = 1e-10)
                                || (a_min.is_nan() && b_min.is_nan()),
                            "Sketch min differs: input={a_min} output={b_min}"
                        );
                        prop_assert!(
                            relative_eq!(a_max, b_max, epsilon = 1e-10)
                                || (a_max.is_nan() && b_max.is_nan()),
                            "Sketch max differs: input={a_max} output={b_max}"
                        );

                        // Verify multiple quantiles are preserved (P50, P95, P99)
                        for quantile in [0.5, 0.95, 0.99] {
                            let a_q = a_sketch.quantile(quantile).unwrap_or(f64::NAN);
                            let b_q = b_sketch.quantile(quantile).unwrap_or(f64::NAN);
                            prop_assert!(
                                relative_eq!(a_q, b_q, epsilon = 1e-6)
                                    || (a_q.is_nan() && b_q.is_nan()),
                                "Sketch P{} differs: input={a_q} output={b_q}",
                                quantile * 100.0
                            );
                        }

                        // Verify quantile ordering invariant: P0 <= P50 <= P100
                        let p0 = a_sketch.min().unwrap_or(f64::NAN);
                        let p50 = a_sketch.quantile(0.5).unwrap_or(f64::NAN);
                        let p100 = a_sketch.max().unwrap_or(f64::NAN);
                        if p0.is_finite() && p50.is_finite() && p100.is_finite() {
                            prop_assert!(
                                p0 <= p50 && p50 <= p100,
                                "Quantile ordering violated: min={p0} <= median={p50} <= max={p100}"
                            );
                        }
                    }
                }
            }
            (Some(_), None) => {
                prop_assert!(false, "Input had histogram data but output is empty");
            }
            (None, Some(_)) => {
                prop_assert!(false, "Input was empty but output has histogram data");
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
                        Just(None),
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
                            Just(None),
                            // Single sample
                            any::<f64>().prop_filter("finite", |f| f.is_finite())
                                .prop_map(|sample| {
                                    let mut sketch = DDSketch::default();
                                    sketch.insert(sample);
                                    Some(sketch_to_json(&sketch))
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
                                Some(sketch_to_json(&sketch))
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
                                Some(sketch_to_json(&sketch))
                            }),
                            // All zeros
                            prop::collection::vec(Just(0.0), 1..10)
                                .prop_map(|samples| {
                                    let mut sketch = DDSketch::default();
                                    for sample in samples {
                                        sketch.insert(sample);
                                    }
                                    Some(sketch_to_json(&sketch))
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
                                Some(sketch_to_json(&sketch))
                            }),
                            // Extreme values that test DDSketch limits
                            prop::collection::vec(
                                prop_oneof![
                                    Just(f64::MIN_POSITIVE),  // Smallest positive
                                    Just(f64::MAX),           // Largest positive
                                    Just(f64::MIN),           // Largest negative
                                    Just(1e-300),             // Near-zero positive
                                    Just(-1e-300),            // Near-zero negative
                                    Just(1e308),              // Near f64::MAX
                                    Just(-1e308),             // Near f64::MIN
                                ],
                                1..5
                            ).prop_map(|samples| {
                                let mut sketch = DDSketch::default();
                                for sample in samples {
                                    sketch.insert(sample);
                                }
                                Some(sketch_to_json(&sketch))
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
                                Some(sketch_to_json(&sketch))
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
                        Just(None),
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
                            Just(None),
                            // Single sample (protobuf for Parquet)
                            any::<f64>().prop_filter("finite", |f| f.is_finite())
                                .prop_map(|sample| {
                                    let mut sketch = DDSketch::default();
                                    sketch.insert(sample);
                                    Some(sketch_to_protobuf(&sketch))
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
                                Some(sketch_to_protobuf(&sketch))
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
                                Some(sketch_to_protobuf(&sketch))
                            }),
                            // All zeros
                            prop::collection::vec(Just(0.0), 1..10)
                                .prop_map(|samples| {
                                    let mut sketch = DDSketch::default();
                                    for sample in samples {
                                        sketch.insert(sample);
                                    }
                                    Some(sketch_to_protobuf(&sketch))
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
                                Some(sketch_to_protobuf(&sketch))
                            }),
                            // Extreme values that test DDSketch limits
                            prop::collection::vec(
                                prop_oneof![
                                    Just(f64::MIN_POSITIVE),  // Smallest positive
                                    Just(f64::MAX),           // Largest positive
                                    Just(f64::MIN),           // Largest negative
                                    Just(1e-300),             // Near-zero positive
                                    Just(-1e-300),            // Near-zero negative
                                    Just(1e308),              // Near f64::MAX
                                    Just(-1e308),             // Near f64::MIN
                                ],
                                1..5
                            ).prop_map(|samples| {
                                let mut sketch = DDSketch::default();
                                for sample in samples {
                                    sketch.insert(sample);
                                }
                                Some(sketch_to_protobuf(&sketch))
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
                                Some(sketch_to_protobuf(&sketch))
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

            let labels_array = batch
                .column_by_name("labels")
                .expect("labels column")
                .as_any()
                .downcast_ref::<MapArray>()
                .expect("labels is MapArray");

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

                let labels_slice: StructArray = labels_array.value(row_idx);
                let keys = labels_slice
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("label keys are strings");
                let values = labels_slice
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("label values are strings");

                let mut labels = FxHashMap::default();
                for i in 0..keys.len() {
                    labels.insert(keys.value(i).to_string(), values.value(i).to_string());
                }

                let value_histogram = if value_histogram_array.is_null(row_idx) {
                    None
                } else {
                    Some(value_histogram_array.value(row_idx).to_vec())
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
