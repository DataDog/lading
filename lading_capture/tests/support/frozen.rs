//! Keep frozen V1 and V2 decoding expectations independent of producer constants, using current workspace libraries.

use std::{collections::BTreeMap, io};

use arrow_array::{
    Array, ArrayRef, BinaryArray, DictionaryArray, Float64Array, MapArray, RecordBatch,
    StringArray, TimestampMillisecondArray, UInt64Array, cast::AsArray, types::Int32Type,
};
use arrow_schema::Schema;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use uuid::Uuid;

/// Records compare logical values without imposing row or label-pair order.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Record {
    pub run_id: [u8; 16],
    pub time: i64,
    pub fetch_index: u64,
    pub metric_name: String,
    pub metric_kind: String,
    pub value_int: Option<u64>,
    pub value_float_bits: Option<u64>,
    pub labels: BTreeMap<String, String>,
    pub value_histogram: Option<Vec<u8>>,
}

/// Frozen schema major versions, each with a separate baseline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Major {
    /// Label keys and values use `Utf8`.
    V1,
    /// Label keys and values use `Dictionary(Int32, Utf8)`.
    V2,
}

impl Major {
    /// Return the major component of this frozen version.
    pub(crate) fn number(self) -> u64 {
        match self {
            Self::V1 => 1,
            Self::V2 => 2,
        }
    }

    /// Return the frozen capture for this major.
    pub(crate) fn capture(self) -> &'static [u8] {
        match self {
            Self::V1 => include_bytes!("../fixtures/v1.parquet"),
            Self::V2 => include_bytes!("../fixtures/v2.parquet"),
        }
    }

    /// Load the frozen Arrow schema instead of deriving expectations from the current writer.
    ///
    /// # Errors
    ///
    /// Return an error if the snapshot cannot decode as an Arrow schema.
    pub(crate) fn schema(self) -> io::Result<Schema> {
        let json = match self {
            Self::V1 => include_str!("../fixtures/v1.arrow.json"),
            Self::V2 => include_str!("../fixtures/v2.arrow.json"),
        };
        serde_json::from_str(json).map_err(io::Error::other)
    }

    /// Read one label column with this major's fixed representation.
    fn labels(self, column: &ArrayRef, role: &str) -> io::Result<Vec<String>> {
        match self {
            Self::V1 => {
                let strings = column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| invalid(format!("label {role}s are not Utf8")))?;
                strings
                    .iter()
                    .map(|value| {
                        value
                            .map(str::to_string)
                            .ok_or_else(|| invalid(format!("null label {role}")))
                    })
                    .collect()
            }
            Self::V2 => {
                let dictionary = column
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .ok_or_else(|| invalid(format!("label {role}s are not Dictionary(Int32)")))?;
                let strings = dictionary
                    .values()
                    .as_string_opt::<i32>()
                    .ok_or_else(|| invalid(format!("label {role} dictionary is not Utf8")))?;
                dictionary
                    .keys()
                    .iter()
                    .map(|key| {
                        let key = key.ok_or_else(|| invalid(format!("null label {role}")))?;
                        let index = usize::try_from(key)
                            .map_err(|_| invalid(format!("negative label {role} key")))?;
                        if index >= strings.len() {
                            return Err(invalid(format!("label {role} key out of range")));
                        }
                        if strings.is_null(index) {
                            return Err(invalid(format!("null label {role} dictionary value")));
                        }
                        Ok(strings.value(index).to_string())
                    })
                    .collect()
            }
        }
    }
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn column<'a, T: Array + 'static>(batch: &'a RecordBatch, name: &str) -> io::Result<&'a T> {
    let array = batch
        .column_by_name(name)
        .ok_or_else(|| invalid(format!("missing {name}")))?;
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| invalid(format!("unexpected type for {name}")))
}

/// Read named columns and fixed map roles with current Parquet reader options.
///
/// # Errors
///
/// Return an error when a file cannot decode with the fixed expectations of `major`.
pub(crate) fn read(data: &[u8], major: Major) -> io::Result<Vec<Record>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(data))
        .map_err(io::Error::other)?
        .with_batch_size(2)
        .build()
        .map_err(io::Error::other)?;
    let mut records = Vec::new();
    for batch in reader {
        let batch = batch.map_err(io::Error::other)?;
        let run_id = column::<StringArray>(&batch, "run_id")?;
        let time = column::<TimestampMillisecondArray>(&batch, "time")?;
        let fetch_index = column::<UInt64Array>(&batch, "fetch_index")?;
        let metric_name = column::<StringArray>(&batch, "metric_name")?;
        let metric_kind = column::<StringArray>(&batch, "metric_kind")?;
        let value_int = column::<UInt64Array>(&batch, "value_int")?;
        let value_float = column::<Float64Array>(&batch, "value_float")?;
        let labels = column::<MapArray>(&batch, "labels")?;
        let histogram = column::<BinaryArray>(&batch, "value_histogram")?;
        for row in 0..batch.num_rows() {
            let kind = metric_kind.value(row);
            if !matches!(kind, "counter" | "gauge" | "histogram") {
                return Err(invalid(format!("unknown metric kind {kind}")));
            }
            let entries = labels.value(row);
            let keys = major.labels(entries.column(0), "key")?;
            let values = major.labels(entries.column(1), "value")?;
            if keys.len() != values.len() {
                return Err(invalid("label key and value counts differ"));
            }
            let labels = keys.into_iter().zip(values).collect();
            records.push(Record {
                run_id: *Uuid::parse_str(run_id.value(row))
                    .map_err(io::Error::other)?
                    .as_bytes(),
                time: time.value(row),
                fetch_index: fetch_index.value(row),
                metric_name: metric_name.value(row).to_string(),
                metric_kind: kind.to_string(),
                value_int: value_int.is_valid(row).then(|| value_int.value(row)),
                value_float_bits: value_float
                    .is_valid(row)
                    .then(|| value_float.value(row).to_bits()),
                labels,
                value_histogram: histogram
                    .is_valid(row)
                    .then(|| histogram.value(row).to_vec()),
            });
        }
    }
    records.sort();
    Ok(records)
}
