//! Shared Arrow schema for Lading capture parquet files.
//!
//! This crate exposes the exact column layout used by `lading-capture` so
//! downstream consumers can read capture files without relying on ad-hoc
//! schema reconstruction.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};

/// Column names used by capture parquet files.
pub mod columns {
    /// Capture run identifier.
    pub const RUN_ID: &str = "run_id";
    /// Millisecond timestamp for the metric.
    pub const TIME: &str = "time";
    /// Monotonic fetch index within a capture.
    pub const FETCH_INDEX: &str = "fetch_index";
    /// Metric name.
    pub const METRIC_NAME: &str = "metric_name";
    /// Metric kind as a string label.
    pub const METRIC_KIND: &str = "metric_kind";
    /// Integer metric value.
    pub const VALUE_INT: &str = "value_int";
    /// Floating-point metric value.
    pub const VALUE_FLOAT: &str = "value_float";
    /// Labels map column.
    pub const LABELS: &str = "labels";
    /// Internal struct name for label entries.
    pub const LABEL_ENTRIES: &str = "entries";
    /// Label key field.
    pub const LABEL_KEY: &str = "key";
    /// Label value field.
    pub const LABEL_VALUE: &str = "value";
    /// Serialized `DDSketch` histogram payload.
    pub const VALUE_HISTOGRAM: &str = "value_histogram";
}

/// Builds the Arrow [`Schema`] used by `lading-capture` parquet files.
#[must_use]
pub fn capture_schema() -> Schema {
    Schema::new(vec![
        Field::new(columns::RUN_ID, DataType::Utf8, false),
        Field::new(
            columns::TIME,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(columns::FETCH_INDEX, DataType::UInt64, false),
        Field::new(columns::METRIC_NAME, DataType::Utf8, false),
        Field::new(columns::METRIC_KIND, DataType::Utf8, false),
        Field::new(columns::VALUE_INT, DataType::UInt64, true),
        Field::new(columns::VALUE_FLOAT, DataType::Float64, true),
        labels_field(),
        Field::new(columns::VALUE_HISTOGRAM, DataType::Binary, true),
    ])
}

fn labels_field() -> Field {
    Field::new(
        columns::LABELS,
        DataType::Map(labels_entry_field(), false),
        false,
    )
}

fn labels_entry_field() -> Arc<Field> {
    Arc::new(Field::new(
        columns::LABEL_ENTRIES,
        DataType::Struct(Fields::from(vec![
            Field::new(columns::LABEL_KEY, label_dictionary_type(), false),
            Field::new(columns::LABEL_VALUE, label_dictionary_type(), false),
        ])),
        false,
    ))
}

/// Arrow logical type for label keys and values.
///
/// Label strings repeat heavily across rows and dominate capture memory.
/// `Dictionary(Int32, Utf8)` lets readers hold each distinct string once,
/// cutting resident memory (not just on-disk size, which parquet already
/// dictionary-encoded).
#[must_use]
pub fn label_dictionary_type() -> DataType {
    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
}
