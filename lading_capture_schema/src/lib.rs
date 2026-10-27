//! Shared Arrow schema for Lading capture parquet files.
//!
//! This crate exposes the exact column layout used by `lading-capture` so
//! downstream consumers can read capture files without relying on ad-hoc
//! schema reconstruction. See this crate's README for the capture contract and
//! Rust loading helpers.

mod validation;
mod version;

pub use validation::{SchemaError, validate_schema};
pub use version::{
    CAPTURE_VERSION_KEY, CURRENT_CAPTURE_VERSION, CaptureVersion, MetadataError,
    parse_capture_version,
};

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

/// Builds the Arrow [`Schema`] used by the current `lading-capture` parquet writer.
#[must_use]
pub fn capture_schema() -> Schema {
    schema_with_labels(label_dictionary_type())
}

/// Builds the Arrow [`Schema`] that capture files of one major version declare.
///
/// Return `None` when this crate does not define the major version.
#[must_use]
pub fn capture_schema_for_major(major: u64) -> Option<Schema> {
    LabelEncoding::for_major(major).map(|encoding| schema_with_labels(encoding.data_type()))
}

/// Arrow representation of label keys and values in one major version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LabelEncoding {
    /// V1 stores label keys and values as `Utf8`.
    Utf8,
    /// V2 stores label keys and values as `Dictionary(Int32, Utf8)`.
    Dictionary,
}

impl LabelEncoding {
    /// Return the label encoding of one major version.
    ///
    /// Return `None` when this crate does not define the major version.
    #[must_use]
    pub const fn for_major(major: u64) -> Option<Self> {
        match major {
            1 => Some(Self::Utf8),
            2 => Some(Self::Dictionary),
            _ => None,
        }
    }

    /// Return the Arrow logical type of this encoding.
    #[must_use]
    pub fn data_type(self) -> DataType {
        match self {
            Self::Utf8 => DataType::Utf8,
            Self::Dictionary => label_dictionary_type(),
        }
    }
}

fn schema_with_labels(label_type: DataType) -> Schema {
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
        labels_field(label_type),
        Field::new(columns::VALUE_HISTOGRAM, DataType::Binary, true),
    ])
}

fn labels_field(label_type: DataType) -> Field {
    Field::new(
        columns::LABELS,
        DataType::Map(labels_entry_field(label_type), false),
        false,
    )
}

fn labels_entry_field(label_type: DataType) -> Arc<Field> {
    Arc::new(Field::new(
        columns::LABEL_ENTRIES,
        DataType::Struct(Fields::from(vec![
            Field::new(columns::LABEL_KEY, label_type.clone(), false),
            Field::new(columns::LABEL_VALUE, label_type, false),
        ])),
        false,
    ))
}

/// Return the Arrow logical type for label keys and values in the current writer.
///
/// Dictionary arrays let Arrow readers share repeated label strings in memory instead of decoding one string for each label entry.
#[must_use]
pub fn label_dictionary_type() -> DataType {
    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::{
        prelude::{Just, any},
        prop_assert, prop_assume, prop_oneof, proptest,
    };

    /// `capture_schema() = capture_schema_for_major(CURRENT_CAPTURE_VERSION.major)`.
    #[test]
    fn current_schema_is_current_major_schema() {
        assert_eq!(
            capture_schema_for_major(CURRENT_CAPTURE_VERSION.major),
            Some(capture_schema())
        );
    }

    /// For all fields f != `labels`: `V1[f] = V2[f]`, and `V1[labels] != V2[labels]`.
    #[test]
    fn majors_differ_only_in_label_types() -> Result<(), &'static str> {
        let v1 = capture_schema_for_major(1).ok_or("missing V1 schema")?;
        let v2 = capture_schema_for_major(2).ok_or("missing V2 schema")?;
        assert_eq!(v1.fields().len(), v2.fields().len());
        for (left, right) in v1.fields().iter().zip(v2.fields().iter()) {
            assert_eq!(left.name(), right.name());
            assert_eq!(left.is_nullable(), right.is_nullable());
            if left.name() != columns::LABELS {
                assert_eq!(left, right);
            }
        }
        assert_ne!(v1, v2);
        Ok(())
    }

    proptest! {
        /// For all major not in {1, 2}: `capture_schema_for_major(major) = None` and
        /// `LabelEncoding::for_major(major) = None`.
        #[test]
        fn undefined_majors_have_no_schema(
            major in prop_oneof![Just(0u64), Just(3), Just(u64::MAX), any::<u64>()],
        ) {
            prop_assume!(!matches!(major, 1 | 2));
            prop_assert!(capture_schema_for_major(major).is_none());
            prop_assert!(LabelEncoding::for_major(major).is_none());
        }
    }
}
