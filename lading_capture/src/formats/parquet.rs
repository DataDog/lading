//! Parquet format
//!
//! This format buffers metrics in memory and periodically writes them as
//! Parquet row groups, providing better compression and query performance than
//! JSONL.

use std::{
    io::{Seek, Write},
    sync::Arc,
};

use arrow_array::{
    ArrayRef, BinaryArray, Float64Array, MapArray, RecordBatch, StringArray, StructArray,
    TimestampMillisecondArray, UInt64Array,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{ArrowError, DataType, Field, Fields, Schema};
use lading_capture_schema::{capture_schema, columns};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::{WriterProperties, WriterVersion},
    schema::types::ColumnPath,
};

use crate::line;

/// Parquet format errors
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// IO errors during write operations
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    /// Arrow errors
    #[error("Arrow error: {0}")]
    Arrow(#[from] ArrowError),
    /// Parquet errors
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
}

/// Pre-allocated column buffers for building Arrow arrays
///
/// Holds reusable allocations for all columns to avoid repeated allocation
/// during flush operations. Buffers are cleared after each write, preserving
/// capacity for the next batch.
#[derive(Debug)]
struct ColumnBuffers {
    run_ids: Vec<String>,
    times: Vec<i64>,
    fetch_indices: Vec<u64>,
    metric_names: Vec<String>,
    metric_kinds: Vec<&'static str>,
    values_int: Vec<Option<u64>>,
    values_float: Vec<Option<f64>>,
    label_keys: Vec<String>,
    label_values: Vec<String>,
    label_offsets: Vec<i32>,
    values_histogram: Vec<Vec<u8>>,
}

/// Acts as sentinel when storing non-histogram metrics
/// Won't be sent over the wire - we prune empty histograms in `buffers_to_record_batch`
const EMPTY_HISTOGRAM: Vec<u8> = Vec::<_>::new();

impl ColumnBuffers {
    /// Create new column buffers with default capacity
    fn new() -> Self {
        // Pre-allocate with reasonable capacity to minimize reallocations
        const INITIAL_CAPACITY: usize = 1024;
        Self {
            run_ids: Vec::with_capacity(INITIAL_CAPACITY),
            times: Vec::with_capacity(INITIAL_CAPACITY),
            fetch_indices: Vec::with_capacity(INITIAL_CAPACITY),
            metric_names: Vec::with_capacity(INITIAL_CAPACITY),
            metric_kinds: Vec::with_capacity(INITIAL_CAPACITY),
            values_int: Vec::with_capacity(INITIAL_CAPACITY),
            values_float: Vec::with_capacity(INITIAL_CAPACITY),
            label_keys: Vec::with_capacity(INITIAL_CAPACITY * 2),
            label_values: Vec::with_capacity(INITIAL_CAPACITY * 2),
            label_offsets: Vec::with_capacity(INITIAL_CAPACITY + 1),
            values_histogram: Vec::with_capacity(INITIAL_CAPACITY),
        }
    }

    /// Clear all buffers while preserving capacity
    fn clear(&mut self) {
        self.run_ids.clear();
        self.times.clear();
        self.fetch_indices.clear();
        self.metric_names.clear();
        self.metric_kinds.clear();
        self.values_int.clear();
        self.values_float.clear();
        self.label_keys.clear();
        self.label_values.clear();
        self.label_offsets.clear();
        self.values_histogram.clear();
    }

    /// Add a metric line to the buffers
    fn push(&mut self, line: &line::Line) {
        self.run_ids.push(line.run_id.to_string());
        #[allow(clippy::cast_possible_truncation)]
        self.times.push(line.time as i64);
        self.fetch_indices.push(line.fetch_index);
        self.metric_names.push(line.metric_name.clone());
        self.metric_kinds.push(match line.metric_kind {
            line::MetricKind::Counter => "counter",
            line::MetricKind::Gauge => "gauge",
            line::MetricKind::Histogram => "histogram",
        });

        match line.value {
            line::LineValue::Int(v) => {
                self.values_int.push(Some(v));
                self.values_float.push(None);
                self.values_histogram.push(EMPTY_HISTOGRAM);
            }
            line::LineValue::Float(v) => {
                self.values_int.push(None);
                self.values_float.push(Some(v));
                self.values_histogram.push(EMPTY_HISTOGRAM);
            }
            line::LineValue::ExternalHistogram => {
                self.values_int.push(None);
                self.values_float.push(None);
                self.values_histogram.push(line.value_histogram.clone());
            }
        }

        // Add labels for this row
        for (k, v) in &line.labels {
            self.label_keys.push(k.clone());
            self.label_values.push(v.clone());
        }
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        self.label_offsets.push(self.label_keys.len() as i32);
    }

    /// Check if buffers are empty
    fn is_empty(&self) -> bool {
        self.run_ids.is_empty()
    }

    #[cfg(test)]
    /// Number of metrics currently buffered
    fn len(&self) -> usize {
        self.run_ids.len()
    }
}

/// Parquet format writer with in-memory buffering
///
/// Buffers metrics in memory. Calling `flush()` writes accumulated metrics as a
/// Parquet row group.
#[derive(Debug)]
pub struct Format<W: Write + Seek + Send> {
    /// Reusable column buffers for building Arrow arrays
    buffers: ColumnBuffers,
    /// Parquet writer
    writer: ArrowWriter<W>,
    /// Pre-computed Arrow schema
    schema: Arc<Schema>,
}

impl<W: Write + Seek + Send> Format<W> {
    /// Create a new Parquet format writer
    ///
    /// # Arguments
    ///
    /// * `writer` - Writer implementing Write + Seek for Parquet output
    /// * `compression_level` - Zstd compression level (1-22)
    ///
    /// # Errors
    ///
    /// Returns error if Arrow writer creation fails
    pub fn new(writer: W, compression_level: i32) -> Result<Self, Error> {
        let schema = Arc::new(capture_schema());

        // Use Parquet v2 format for better encodings and compression:
        //
        // - DELTA_BINARY_PACKED encoding for integers (timestamps, fetch_index)
        // - DELTA_LENGTH_BYTE_ARRAY encoding for strings (run_id, metric_name)
        // - More efficient page-level statistics for query optimization
        //
        // Dictionary encoding for low-cardinality columns:
        //
        // - metric_kind: only 2 values ("counter", "gauge")
        // - run_id: one UUID per run
        let props = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(compression_level)?))
            .set_column_dictionary_enabled(ColumnPath::from(columns::METRIC_KIND), true)
            .set_column_dictionary_enabled(ColumnPath::from(columns::RUN_ID), true)
            .build();

        let arrow_writer = ArrowWriter::try_new(writer, schema.clone(), Some(props))?;

        Ok(Self {
            buffers: ColumnBuffers::new(),
            writer: arrow_writer,
            schema,
        })
    }

    /// Convert buffered data to Arrow `RecordBatch`
    ///
    /// # Errors
    ///
    /// Returns error if `RecordBatch` construction fails
    fn buffers_to_record_batch(&self) -> Result<RecordBatch, Error> {
        if self.buffers.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        // Prepare label offsets with initial 0
        let mut label_offsets = Vec::with_capacity(self.buffers.label_offsets.len() + 1);
        label_offsets.push(0i32);
        label_offsets.extend_from_slice(&self.buffers.label_offsets);

        // Build the labels map array using pre-allocated buffers
        let keys_array = Arc::new(StringArray::from(self.buffers.label_keys.clone()));
        let values_array = Arc::new(StringArray::from(self.buffers.label_values.clone()));
        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new(columns::LABEL_KEY, DataType::Utf8, false)),
                keys_array as ArrayRef,
            ),
            (
                Arc::new(Field::new(columns::LABEL_VALUE, DataType::Utf8, false)),
                values_array as ArrayRef,
            ),
        ]);

        let field = Arc::new(Field::new(
            columns::LABEL_ENTRIES,
            DataType::Struct(Fields::from(vec![
                Field::new(columns::LABEL_KEY, DataType::Utf8, false),
                Field::new(columns::LABEL_VALUE, DataType::Utf8, false),
            ])),
            false,
        ));

        let offsets = OffsetBuffer::new(label_offsets.into());
        let labels_map = MapArray::new(field, offsets, struct_array, None, false);

        // Build arrays directly from pre-allocated buffers
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(self.buffers.run_ids.clone())),
            Arc::new(TimestampMillisecondArray::from(self.buffers.times.clone())),
            Arc::new(UInt64Array::from(self.buffers.fetch_indices.clone())),
            Arc::new(StringArray::from(self.buffers.metric_names.clone())),
            Arc::new(StringArray::from(self.buffers.metric_kinds.clone())),
            Arc::new(UInt64Array::from(self.buffers.values_int.clone())),
            Arc::new(Float64Array::from(self.buffers.values_float.clone())),
            Arc::new(labels_map),
            Arc::new(BinaryArray::from_opt_vec(
                self.buffers
                    .values_histogram
                    .iter()
                    .map(|v| {
                        if v.is_empty() {
                            None
                        } else {
                            Some(v.as_slice())
                        }
                    })
                    .collect(),
            )),
        ];

        Ok(RecordBatch::try_new(self.schema.clone(), arrays)?)
    }

    /// Write buffered metrics as a Parquet row group
    ///
    /// # Errors
    ///
    /// Returns error if Parquet write fails
    fn write_parquet(&mut self) -> Result<(), Error> {
        if self.buffers.is_empty() {
            return Ok(());
        }

        let batch = self.buffers_to_record_batch()?;
        self.writer.write(&batch)?;
        self.buffers.clear();
        Ok(())
    }
}

impl<W: Write + Seek + Send> Format<W> {
    /// Write a single metric line to the buffer
    ///
    /// # Errors
    ///
    /// This method does not currently return errors, but the signature
    /// maintains consistency with other format implementations
    pub fn write_metric(&mut self, line: &line::Line) -> Result<(), Error> {
        self.buffers.push(line);
        Ok(())
    }

    /// Flush buffered metrics to disk as a Parquet row group
    ///
    /// # Errors
    ///
    /// Returns an error if Parquet writing fails
    pub fn flush(&mut self) -> Result<(), Error> {
        self.write_parquet()?;
        Ok(())
    }

    /// Close and finalize the Parquet file
    ///
    /// This method MUST be called to properly finalize the Parquet file. It
    /// writes any remaining buffered data and the Parquet file footer which
    /// contains critical metadata. Without calling `close()`, the file will be
    /// incomplete and unreadable.
    ///
    /// Consumes the format as it can no longer be used after closing.
    ///
    /// # Errors
    ///
    /// Returns an error if writing or closing fails
    pub fn close(mut self) -> Result<(), Error> {
        // Write any remaining buffered data as a final row group
        self.write_parquet()?;
        // Close the ArrowWriter which consumes it
        self.writer.close()?;
        Ok(())
    }
}

impl<W: Write + Seek + Send> crate::formats::OutputFormat for Format<W> {
    fn write_metric(&mut self, line: &line::Line) -> Result<(), crate::formats::Error> {
        self.write_metric(line).map_err(Into::into)
    }

    fn flush(&mut self) -> Result<(), crate::formats::Error> {
        self.flush().map_err(Into::into)
    }

    fn close(self) -> Result<(), crate::formats::Error> {
        self.close().map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::line::{Line, LineValue, MetricKind};
    use datadog_protos::metrics::Dogsketch;
    use ddsketch_agent::DDSketch;
    use protobuf::Message;
    use rustc_hash::FxHashMap;
    use std::io::Cursor;
    use uuid::Uuid;

    #[test]
    fn buffers_metrics_before_flush() {
        let buffer = Cursor::new(Vec::new());
        let mut format = Format::new(buffer, 3).expect("create format");

        let line = Line {
            run_id: Uuid::new_v4(),
            time: 1000,
            fetch_index: 0,
            metric_name: "test_metric".into(),
            metric_kind: MetricKind::Counter,
            value: LineValue::Int(42),
            labels: FxHashMap::default(),
            value_histogram: Vec::new(),
        };

        format.write_metric(&line).expect("write should succeed");
        assert_eq!(format.buffers.len(), 1);

        // Flush should write the buffer
        format.flush().expect("flush should succeed");
        assert_eq!(format.buffers.len(), 0);
    }

    #[test]
    fn writes_parquet_on_flush() {
        let buffer = Cursor::new(Vec::new());
        let mut format = Format::new(buffer, 3).expect("create format");

        // Write multiple metrics
        for i in 0..5 {
            let line = Line {
                run_id: Uuid::new_v4(),
                time: 1000 + (i as u128),
                fetch_index: i as u64,
                metric_name: format!("metric_{i}"),
                metric_kind: MetricKind::Gauge,
                value: LineValue::Float(i as f64),
                labels: FxHashMap::default(),
                value_histogram: Vec::new(),
            };
            format.write_metric(&line).expect("write should succeed");
        }

        // Flush to trigger write
        format.flush().expect("flush should succeed");

        assert_eq!(format.buffers.len(), 0, "buffers should be empty");
    }

    #[test]
    fn writes_histogram() {
        let mut buffer = Cursor::new(Vec::new());

        {
            let mut format = Format::new(&mut buffer, 10).expect("create format");

            let mut sketch = DDSketch::default();
            sketch.insert(10.0);
            sketch.insert(20.0);
            let mut dogsketch = Dogsketch::new();
            sketch.merge_to_dogsketch(&mut dogsketch);

            let line = Line {
                run_id: Uuid::new_v4(),
                time: 1000,
                fetch_index: 0,
                metric_name: "histogram_metric".into(),
                metric_kind: MetricKind::Histogram,
                value: LineValue::Float(0.0),
                labels: FxHashMap::default(),
                value_histogram: dogsketch.write_to_bytes().expect("protobuf"),
            };

            format.write_metric(&line).expect("write should succeed");
            format.flush().expect("flush should succeed");
        }

        assert!(!buffer.get_ref().is_empty(), "should have written data");
    }
}
