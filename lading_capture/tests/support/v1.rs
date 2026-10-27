//! Keep V1 decoding expectations independent of producer constants, using current workspace libraries.

use std::{collections::BTreeMap, io};

use arrow_array::{
    Array, BinaryArray, Float64Array, MapArray, RecordBatch, StringArray,
    TimestampMillisecondArray, UInt64Array,
};
use arrow_schema::Schema;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use uuid::Uuid;

/// V1 records compare logical values without imposing row or label-pair order.
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

/// Load the frozen Arrow schema instead of deriving expectations from the current writer.
///
/// # Errors
///
/// Return an error if the snapshot cannot decode as an Arrow schema.
pub(crate) fn schema() -> io::Result<Schema> {
    serde_json::from_str(include_str!("../fixtures/v1.arrow.json")).map_err(io::Error::other)
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

/// Read named V1 columns and fixed map roles with current Parquet reader options.
///
/// # Errors
///
/// Return an error when a file cannot decode with the fixed V1 expectations.
pub(crate) fn read(data: &[u8]) -> io::Result<Vec<Record>> {
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
            let keys = entries
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| invalid("label keys are not Utf8"))?;
            let values = entries
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| invalid("label values are not Utf8"))?;
            let labels = (0..keys.len())
                .map(|index| {
                    (
                        keys.value(index).to_string(),
                        values.value(index).to_string(),
                    )
                })
                .collect();
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
