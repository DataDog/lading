//! Parquet format for Lading capture lines, stored in columnar format for
//! better compression and query performance.

use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, BinaryBuilder, Float64Builder, MapBuilder, StringBuilder,
        StringDictionaryBuilder, TimestampMillisecondBuilder, UInt64Builder,
    },
    datatypes::{DataType, Field, Int32Type, Schema, SchemaRef, TimeUnit},
    record_batch::RecordBatch,
};
use rustc_hash::FxHashMap;
use uuid::Uuid;

use crate::json::{Line, LineValue, MetricKind};

/// Raw metric data for direct Arrow conversion
#[derive(Debug, Clone)]
pub struct RawMetricData {
    /// Unique identifier for this metric collection run
    pub run_id: Uuid,
    /// Timestamp in milliseconds since Unix epoch
    pub time: u128,
    /// Index of the metrics collection fetch/flush cycle
    pub fetch_index: u64,
    /// Name of the metric being recorded
    pub metric_name: String,
    /// Type of metric (counter or gauge)
    pub metric_kind: RawMetricKind,
    /// Numeric value of the metric
    pub value: RawMetricValue,
    /// Key-value pairs of labels associated with this metric
    pub labels: FxHashMap<String, String>,
}

/// The kind of metric being recorded
#[derive(Debug, Clone, Copy)]
pub enum RawMetricKind {
    /// A monotonically increasing value
    Counter,
    /// A point-in-time value that can increase or decrease
    Gauge,
}

/// The numeric value of a metric
#[derive(Debug, Clone, Copy)]
pub enum RawMetricValue {
    /// An unsigned 64-bit integer value
    Int(u64),
    /// A 64-bit floating point value
    Float(f64),
}

impl RawMetricValue {
    /// Convert this value to a 64-bit float for Arrow storage
    #[must_use]
    pub fn as_f64(&self) -> f64 {
        match self {
            #[allow(clippy::cast_precision_loss)]
            RawMetricValue::Int(i) => *i as f64,
            RawMetricValue::Float(f) => *f,
        }
    }
}

/// Schema for Lading capture data in Arrow/Parquet format
#[must_use]
pub fn capture_schema() -> SchemaRef {
    let fields = vec![
        Field::new("run_id", DataType::Binary, false),
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("fetch_index", DataType::UInt64, false),
        Field::new(
            "metric_name",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new(
            "metric_kind",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        // Simplified: all values as Float64 for now
        Field::new("value", DataType::Float64, false),
        // Map for labels
        Field::new(
            "labels",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("keys", DataType::Utf8, false),
                            Field::new("values", DataType::Utf8, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            false,
        ),
    ];

    Arc::new(Schema::new(fields))
}

/// Convert a batch of raw metrics directly to Arrow `RecordBatch`
///
/// # Errors
///
/// Returns an error if Arrow array building or `RecordBatch` creation fails.
pub fn raw_metrics_to_record_batch(
    metrics: &[RawMetricData],
) -> Result<RecordBatch, arrow::error::ArrowError> {
    let schema = capture_schema();
    let num_metrics = metrics.len();

    // Build run_id array (UUID as 16-byte binary) - pre-allocate capacity
    let mut run_id_builder = BinaryBuilder::with_capacity(num_metrics, num_metrics * 16);
    for metric in metrics {
        run_id_builder.append_value(metric.run_id.as_bytes());
    }
    let run_id_array = Arc::new(run_id_builder.finish()) as ArrayRef;

    // Build time array (timestamp in milliseconds) - pre-allocate capacity
    let mut time_builder = TimestampMillisecondBuilder::with_capacity(num_metrics);
    for metric in metrics {
        #[allow(clippy::cast_possible_truncation)]
        let time_i64 = metric.time as i64;
        time_builder.append_value(time_i64);
    }
    let time_array = Arc::new(time_builder.finish()) as ArrayRef;

    // Build fetch_index array - pre-allocate capacity
    let mut fetch_index_builder = UInt64Builder::with_capacity(num_metrics);
    for metric in metrics {
        fetch_index_builder.append_value(metric.fetch_index);
    }
    let fetch_index_array = Arc::new(fetch_index_builder.finish()) as ArrayRef;

    // Build metric_name dictionary array - pre-allocate capacity
    let mut metric_name_builder = StringDictionaryBuilder::<Int32Type>::with_capacity(num_metrics, 256, 1024);
    for metric in metrics {
        metric_name_builder.append(&metric.metric_name)?;
    }
    let metric_name_array = Arc::new(metric_name_builder.finish()) as ArrayRef;

    // Build metric_kind dictionary array - pre-allocate capacity
    let mut metric_kind_builder = StringDictionaryBuilder::<Int32Type>::with_capacity(num_metrics, 8, 64);
    for metric in metrics {
        let kind_str = match metric.metric_kind {
            RawMetricKind::Counter => "counter",
            RawMetricKind::Gauge => "gauge",
        };
        metric_kind_builder.append(kind_str)?;
    }
    let metric_kind_array = Arc::new(metric_kind_builder.finish()) as ArrayRef;

    // For now, let's use a simpler approach - convert all values to Float64
    // This avoids the complexity of union arrays while we get the basic functionality working
    let mut value_builder = Float64Builder::with_capacity(num_metrics);
    for metric in metrics {
        value_builder.append_value(metric.value.as_f64());
    }
    let value_array = Arc::new(value_builder.finish()) as ArrayRef;

    // Build labels map array - pre-allocate capacity (estimate ~4 labels per metric)
    let estimated_label_pairs = num_metrics * 4;
    let mut labels_builder = MapBuilder::new(
        None, 
        StringBuilder::with_capacity(estimated_label_pairs, estimated_label_pairs * 16),
        StringBuilder::with_capacity(estimated_label_pairs, estimated_label_pairs * 32)
    );
    for metric in metrics {
        for (key, value) in &metric.labels {
            labels_builder.keys().append_value(key);
            labels_builder.values().append_value(value);
        }
        labels_builder.append(true)?;
    }
    let labels_array = Arc::new(labels_builder.finish()) as ArrayRef;

    // Create the record batch
    let arrays = vec![
        run_id_array,
        time_array,
        fetch_index_array,
        metric_name_array,
        metric_kind_array,
        value_array,
        labels_array,
    ];

    RecordBatch::try_new(schema, arrays)
}

/// Convert a batch of JSON Lines to Arrow `RecordBatch`
///
/// # Errors
///
/// Returns an error if Arrow array building or `RecordBatch` creation fails.
pub fn lines_to_record_batch(lines: &[Line]) -> Result<RecordBatch, arrow::error::ArrowError> {
    let schema = capture_schema();
    let num_lines = lines.len();

    // Build run_id array (UUID as 16-byte binary) - pre-allocate capacity
    let mut run_id_builder = BinaryBuilder::with_capacity(num_lines, num_lines * 16);
    for line in lines {
        run_id_builder.append_value(line.run_id.as_bytes());
    }
    let run_id_array = Arc::new(run_id_builder.finish()) as ArrayRef;

    // Build time array (timestamp in milliseconds) - pre-allocate capacity
    let mut time_builder = TimestampMillisecondBuilder::with_capacity(num_lines);
    for line in lines {
        #[allow(clippy::cast_possible_truncation)]
        let time_i64 = line.time as i64;
        time_builder.append_value(time_i64);
    }
    let time_array = Arc::new(time_builder.finish()) as ArrayRef;

    // Build fetch_index array - pre-allocate capacity
    let mut fetch_index_builder = UInt64Builder::with_capacity(num_lines);
    for line in lines {
        fetch_index_builder.append_value(line.fetch_index);
    }
    let fetch_index_array = Arc::new(fetch_index_builder.finish()) as ArrayRef;

    // Build metric_name dictionary array - pre-allocate capacity
    let mut metric_name_builder = StringDictionaryBuilder::<Int32Type>::with_capacity(num_lines, 256, 1024);
    for line in lines {
        metric_name_builder.append(&line.metric_name)?;
    }
    let metric_name_array = Arc::new(metric_name_builder.finish()) as ArrayRef;

    // Build metric_kind dictionary array - pre-allocate capacity
    let mut metric_kind_builder = StringDictionaryBuilder::<Int32Type>::with_capacity(num_lines, 8, 64);
    for line in lines {
        let kind_str = match line.metric_kind {
            MetricKind::Counter => "counter",
            MetricKind::Gauge => "gauge",
        };
        metric_kind_builder.append(kind_str)?;
    }
    let metric_kind_array = Arc::new(metric_kind_builder.finish()) as ArrayRef;

    // For now, let's use a simpler approach - convert all values to Float64
    // This avoids the complexity of union arrays while we get the basic functionality working
    let mut value_builder = Float64Builder::with_capacity(num_lines);
    for line in lines {
        value_builder.append_value(line.value.as_f64());
    }
    let value_array = Arc::new(value_builder.finish()) as ArrayRef;

    // Build labels map array - pre-allocate capacity (estimate ~4 labels per line)
    let estimated_label_pairs = num_lines * 4;
    let mut labels_builder = MapBuilder::new(
        None, 
        StringBuilder::with_capacity(estimated_label_pairs, estimated_label_pairs * 16),
        StringBuilder::with_capacity(estimated_label_pairs, estimated_label_pairs * 32)
    );
    for line in lines {
        for (key, value) in &line.labels {
            labels_builder.keys().append_value(key);
            labels_builder.values().append_value(value);
        }
        labels_builder.append(true)?;
    }
    let labels_array = Arc::new(labels_builder.finish()) as ArrayRef;

    // Create the record batch
    let arrays = vec![
        run_id_array,
        time_array,
        fetch_index_array,
        metric_name_array,
        metric_kind_array,
        value_array,
        labels_array,
    ];

    RecordBatch::try_new(schema, arrays)
}

/// Convert a `RecordBatch` back to JSON Lines
///
/// # Errors
///
/// Returns an error if the batch structure doesn't match expected schema or
/// if any data conversion fails during the process.
#[allow(clippy::too_many_lines)]
pub fn record_batch_to_lines(batch: &RecordBatch) -> Result<Vec<Line>, arrow::error::ArrowError> {
    let mut lines = Vec::new();

    // Extract arrays from the batch
    let run_id_array = batch.column(0);
    let time_array = batch.column(1);
    let fetch_index_array = batch.column(2);
    let metric_name_array = batch.column(3);
    let metric_kind_array = batch.column(4);
    let value_array = batch.column(5);
    let labels_array = batch.column(6);

    for row in 0..batch.num_rows() {
        // Extract run_id
        let run_id_bytes = run_id_array
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .ok_or(arrow::error::ArrowError::CastError(
                "Failed to cast run_id to BinaryArray".to_string(),
            ))?
            .value(row);
        let run_id = Uuid::from_slice(run_id_bytes)
            .map_err(|e| arrow::error::ArrowError::CastError(format!("Invalid UUID bytes: {e}")))?;

        // Extract time
        let time_val = time_array
            .as_any()
            .downcast_ref::<arrow::array::TimestampMillisecondArray>()
            .ok_or(arrow::error::ArrowError::CastError(
                "Failed to cast time to TimestampMillisecondArray".to_string(),
            ))?
            .value(row);
        #[allow(clippy::cast_sign_loss)]
        let time = time_val.max(0) as u128;

        // Extract fetch_index
        let fetch_index = fetch_index_array
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .ok_or(arrow::error::ArrowError::CastError(
                "Failed to cast fetch_index to UInt64Array".to_string(),
            ))?
            .value(row);

        // Extract metric_name from dictionary
        let metric_name_dict = metric_name_array
            .as_any()
            .downcast_ref::<arrow::array::DictionaryArray<Int32Type>>()
            .ok_or(arrow::error::ArrowError::CastError(
                "Failed to cast metric_name to DictionaryArray".to_string(),
            ))?;
        let metric_name = metric_name_dict
            .values()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or(arrow::error::ArrowError::CastError(
                "Failed to cast metric_name values to StringArray".to_string(),
            ))?
            .value(
                metric_name_dict
                    .key(row)
                    .ok_or(arrow::error::ArrowError::CastError(
                        "Invalid metric_name key".to_string(),
                    ))?,
            )
            .to_string();

        // Extract metric_kind from dictionary
        let metric_kind_dict = metric_kind_array
            .as_any()
            .downcast_ref::<arrow::array::DictionaryArray<Int32Type>>()
            .ok_or(arrow::error::ArrowError::CastError(
                "Failed to cast metric_kind to DictionaryArray".to_string(),
            ))?;
        let metric_kind_str = metric_kind_dict
            .values()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or(arrow::error::ArrowError::CastError(
                "Failed to cast metric_kind values to StringArray".to_string(),
            ))?
            .value(
                metric_kind_dict
                    .key(row)
                    .ok_or(arrow::error::ArrowError::CastError(
                        "Invalid metric_kind key".to_string(),
                    ))?,
            );
        let metric_kind = match metric_kind_str {
            "counter" => MetricKind::Counter,
            "gauge" => MetricKind::Gauge,
            _ => {
                return Err(arrow::error::ArrowError::CastError(format!(
                    "Unknown metric kind: {metric_kind_str}"
                )));
            }
        };

        // Extract value (simplified - all stored as Float64)
        let value_float_array = value_array
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .ok_or(arrow::error::ArrowError::CastError(
                "Failed to cast value to Float64Array".to_string(),
            ))?;
        let value = LineValue::Float(value_float_array.value(row));

        // Extract labels from map
        let labels_map = labels_array
            .as_any()
            .downcast_ref::<arrow::array::MapArray>()
            .ok_or(arrow::error::ArrowError::CastError(
                "Failed to cast labels to MapArray".to_string(),
            ))?;
        let mut labels = FxHashMap::default();

        #[allow(clippy::cast_sign_loss)]
        let start = labels_map.value_offsets()[row].max(0) as usize;
        #[allow(clippy::cast_sign_loss)]
        let end = labels_map.value_offsets()[row + 1].max(0) as usize;

        let keys_array = labels_map
            .keys()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or(arrow::error::ArrowError::CastError(
                "Failed to cast map keys to StringArray".to_string(),
            ))?;
        let label_values_array = labels_map
            .values()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or(arrow::error::ArrowError::CastError(
                "Failed to cast map values to StringArray".to_string(),
            ))?;

        for i in start..end {
            let key = keys_array.value(i).to_string();
            let value = label_values_array.value(i).to_string();
            labels.insert(key, value);
        }

        lines.push(Line {
            run_id,
            time,
            fetch_index,
            metric_name,
            metric_kind,
            value,
            labels,
        });
    }

    Ok(lines)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json::{Line, LineValue, MetricKind};

    #[test]
    fn test_schema_creation() {
        let schema = capture_schema();
        assert_eq!(schema.fields().len(), 7);
        assert_eq!(schema.field(0).name(), "run_id");
        assert_eq!(schema.field(1).name(), "time");
        assert_eq!(schema.field(2).name(), "fetch_index");
        assert_eq!(schema.field(3).name(), "metric_name");
        assert_eq!(schema.field(4).name(), "metric_kind");
        assert_eq!(schema.field(5).name(), "value");
        assert_eq!(schema.field(6).name(), "labels");
    }

    #[test]
    fn test_round_trip_conversion() {
        let mut labels = FxHashMap::default();
        labels.insert("host".to_string(), "localhost".to_string());
        labels.insert("service".to_string(), "test".to_string());

        let lines = vec![
            Line {
                run_id: Uuid::new_v4(),
                time: 1000,
                fetch_index: 1,
                metric_name: "test.counter".to_string(),
                metric_kind: MetricKind::Counter,
                value: LineValue::Int(42),
                labels: labels.clone(),
            },
            Line {
                run_id: Uuid::new_v4(),
                time: 2000,
                fetch_index: 2,
                metric_name: "test.gauge".to_string(),
                metric_kind: MetricKind::Gauge,
                value: LineValue::Float(3.14),
                labels,
            },
        ];

        let batch = lines_to_record_batch(&lines).expect("Failed to create record batch");
        let converted_lines = record_batch_to_lines(&batch).expect("Failed to convert back");

        assert_eq!(lines.len(), converted_lines.len());

        for (original, converted) in lines.iter().zip(converted_lines.iter()) {
            assert_eq!(original.run_id, converted.run_id);
            assert_eq!(original.time, converted.time);
            assert_eq!(original.fetch_index, converted.fetch_index);
            assert_eq!(original.metric_name, converted.metric_name);
            assert_eq!(original.metric_kind, converted.metric_kind);

            // Note: All values are stored as Float64 in simplified schema
            let expected_float = original.value.as_f64();
            match &converted.value {
                LineValue::Float(actual) => assert!((expected_float - actual).abs() < f64::EPSILON),
                _ => panic!("Expected Float value in converted data"),
            }

            assert_eq!(original.labels, converted.labels);
        }
    }

    #[test]
    fn test_empty_labels() {
        let lines = vec![Line {
            run_id: Uuid::new_v4(),
            time: 1000,
            fetch_index: 1,
            metric_name: "test.counter".to_string(),
            metric_kind: MetricKind::Counter,
            value: LineValue::Int(42),
            labels: FxHashMap::default(),
        }];

        let batch = lines_to_record_batch(&lines).expect("Failed to create record batch");
        let converted_lines = record_batch_to_lines(&batch).expect("Failed to convert back");

        assert_eq!(lines.len(), converted_lines.len());
        assert!(converted_lines[0].labels.is_empty());
    }

    #[test]
    fn test_large_batch() {
        let mut lines = Vec::new();
        let mut labels = FxHashMap::default();
        labels.insert("host".to_string(), "server1".to_string());

        for i in 0..1000 {
            lines.push(Line {
                run_id: Uuid::new_v4(),
                time: i as u128,
                fetch_index: i,
                metric_name: format!("metric.{}", i % 10),
                metric_kind: if i % 2 == 0 {
                    MetricKind::Counter
                } else {
                    MetricKind::Gauge
                },
                value: LineValue::Float(i as f64 * 3.14),
                labels: labels.clone(),
            });
        }

        let batch = lines_to_record_batch(&lines).expect("Failed to create record batch");
        let converted_lines = record_batch_to_lines(&batch).expect("Failed to convert back");

        assert_eq!(lines.len(), converted_lines.len());
        assert_eq!(lines.len(), 1000);
    }
}
