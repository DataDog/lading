//! Frozen 1.x data-reading behavior, independent of writer schemas and constants.
//!
//! Changes to this decoder or its dependency pins need explicit compatibility
//! review. Do not adapt it to make a same-major producer change pass.

use std::io;

use crate::model::Record;

use arrow_array::{
    Array, BinaryArray, Float64Array, MapArray, RecordBatch, StringArray,
    TimestampMillisecondArray, UInt64Array,
};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use uuid::Uuid;

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

/// Read named capture columns and fixed map roles with default Parquet reader options.
///
/// Footer-version admission policy is deliberately separate from this data test.
/// Small batches exercise labels across batch boundaries.
///
/// # Errors
///
/// Return an error when the fixed reader cannot decode the file or its known columns.
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
            // Map roles stay positional even when top-level columns move.
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
    // Canonical record order keeps physical row and batch ordering outside the compatibility contract.
    records.sort();
    Ok(records)
}
