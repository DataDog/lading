//! Parquet format
//!
//! This format buffers metrics in memory and periodically writes them as
//! Parquet row groups, providing better compression and query performance than
//! JSONL.
//!
//! Labels are stored as top-level columns with `l_` prefix (e.g., `l_container_id`,
//! `l_namespace`). This enables Parquet predicate pushdown for efficient filtering
//! by any label key, unlike the previous `MapArray` approach which required post-read
//! filtering.
//!
//! Schema is determined dynamically at first flush based on label keys discovered
//! in the buffered data. All label columns are nullable Utf8 strings.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{BufWriter, Seek, Write},
    sync::Arc,
};

use arrow_array::{
    ArrayRef, BinaryArray, Float64Array, RecordBatch, StringArray, TimestampMillisecondArray,
    UInt64Array,
};
use arrow_schema::{ArrowError, DataType, Field, Schema, TimeUnit};
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
///
/// Labels are stored per-row as a map, with all unique keys tracked separately
/// to enable dynamic schema generation with `l_<key>` columns.
#[derive(Debug)]
struct ColumnBuffers {
    run_ids: Vec<String>,
    times: Vec<i64>,
    fetch_indices: Vec<u64>,
    metric_names: Vec<String>,
    metric_kinds: Vec<&'static str>,
    values_int: Vec<Option<u64>>,
    values_float: Vec<Option<f64>>,
    /// Per-row label maps. Each entry contains the labels for one metric row.
    /// Using `BTreeMap` for consistent ordering when iterating.
    row_labels: Vec<BTreeMap<String, String>>,
    /// All unique label keys seen in this batch. Used to generate schema.
    /// `BTreeSet` ensures consistent column ordering across runs.
    unique_label_keys: BTreeSet<String>,
    values_histogram: Vec<Vec<u8>>,
}

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
            row_labels: Vec::with_capacity(INITIAL_CAPACITY),
            unique_label_keys: BTreeSet::new(),
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
        self.row_labels.clear();
        // Note: unique_label_keys is NOT cleared - it accumulates across flushes
        // to maintain consistent schema within a file
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
            }
            line::LineValue::Float(v) => {
                self.values_int.push(None);
                self.values_float.push(Some(v));
            }
        }

        // Store labels for this row and track unique keys
        let mut row_map = BTreeMap::new();
        for (k, v) in &line.labels {
            self.unique_label_keys.insert(k.clone());
            row_map.insert(k.clone(), v.clone());
        }
        self.row_labels.push(row_map);

        self.values_histogram.push(line.value_histogram.clone());
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
///
/// The schema is determined dynamically at first flush based on label keys
/// discovered in the buffered data. Label columns use the `l_` prefix
/// (e.g., `l_container_id`).
#[derive(Debug)]
pub struct Format<W: Write + Seek + Send> {
    /// Reusable column buffers for building Arrow arrays
    buffers: ColumnBuffers,
    /// Parquet writer - created lazily on first flush when schema is known
    writer: Option<ArrowWriter<W>>,
    /// The underlying writer, stored until `ArrowWriter` is created
    raw_writer: Option<W>,
    /// Arrow schema - created on first flush based on discovered label keys
    schema: Option<Arc<Schema>>,
    /// Ordered list of label keys in schema (for consistent column ordering)
    schema_label_keys: Vec<String>,
    /// Compression level for Zstd (stored for rotation)
    compression_level: i32,
}

/// Label column prefix for flattened labels
const LABEL_COLUMN_PREFIX: &str = "l_";

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
    /// Returns error if compression level is invalid
    pub fn new(writer: W, compression_level: i32) -> Result<Self, Error> {
        // Validate compression level early
        let _ = ZstdLevel::try_new(compression_level)?;

        Ok(Self {
            buffers: ColumnBuffers::new(),
            writer: None,
            raw_writer: Some(writer),
            schema: None,
            schema_label_keys: Vec::new(),
            compression_level,
        })
    }

    /// Generate schema based on discovered label keys
    ///
    /// Creates base columns plus `l_<key>` columns for each unique label key.
    /// Label columns are nullable Utf8 strings, sorted alphabetically for
    /// consistent ordering.
    fn generate_schema(label_keys: &BTreeSet<String>) -> (Arc<Schema>, Vec<String>) {
        let mut fields = vec![
            Field::new("run_id", DataType::Utf8, false),
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("fetch_index", DataType::UInt64, false),
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("metric_kind", DataType::Utf8, false),
            Field::new("value_int", DataType::UInt64, true),
            Field::new("value_float", DataType::Float64, true),
        ];

        // Add l_<key> columns for each label key (sorted by BTreeSet)
        let ordered_keys: Vec<String> = label_keys.iter().cloned().collect();
        for key in &ordered_keys {
            fields.push(Field::new(
                format!("{LABEL_COLUMN_PREFIX}{key}"),
                DataType::Utf8,
                true, // nullable - not all rows have all labels
            ));
        }

        fields.push(Field::new("value_histogram", DataType::Binary, true));

        (Arc::new(Schema::new(fields)), ordered_keys)
    }

    /// Create writer properties with dictionary encoding for appropriate columns
    fn create_writer_properties(
        compression_level: i32,
        label_keys: &[String],
    ) -> Result<WriterProperties, Error> {
        // Use Parquet v2 format for better encodings and compression:
        //
        // - DELTA_BINARY_PACKED encoding for integers (timestamps, fetch_index)
        // - DELTA_LENGTH_BYTE_ARRAY encoding for strings (run_id, metric_name)
        // - More efficient page-level statistics for query optimization
        //
        // Dictionary encoding for low-cardinality columns:
        //
        // - metric_kind: only 3 values ("counter", "gauge", "histogram")
        // - run_id: one UUID per run
        // - label columns: often low cardinality (container_id, namespace, etc.)
        let mut builder = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(compression_level)?))
            .set_column_dictionary_enabled(ColumnPath::from("metric_kind"), true)
            .set_column_dictionary_enabled(ColumnPath::from("run_id"), true);

        // Enable dictionary encoding for all label columns
        for key in label_keys {
            builder = builder.set_column_dictionary_enabled(
                ColumnPath::from(format!("{LABEL_COLUMN_PREFIX}{key}")),
                true,
            );
        }

        Ok(builder.build())
    }

    /// Initialize the writer with schema based on discovered label keys
    ///
    /// Called on first flush when we know what label keys exist.
    fn initialize_writer(&mut self) -> Result<(), Error> {
        let raw_writer = self
            .raw_writer
            .take()
            .expect("raw_writer should be present before initialization");

        let (schema, ordered_keys) = Self::generate_schema(&self.buffers.unique_label_keys);
        let props = Self::create_writer_properties(self.compression_level, &ordered_keys)?;

        let arrow_writer = ArrowWriter::try_new(raw_writer, schema.clone(), Some(props))?;

        self.schema = Some(schema);
        self.schema_label_keys = ordered_keys;
        self.writer = Some(arrow_writer);

        Ok(())
    }

    /// Convert buffered data to Arrow `RecordBatch`
    ///
    /// # Errors
    ///
    /// Returns error if `RecordBatch` construction fails
    fn buffers_to_record_batch(&self) -> Result<RecordBatch, Error> {
        let schema = self
            .schema
            .as_ref()
            .expect("schema should be initialized before creating record batch");

        if self.buffers.is_empty() {
            return Ok(RecordBatch::new_empty(schema.clone()));
        }

        let num_rows = self.buffers.run_ids.len();

        // Build base column arrays
        let mut arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(self.buffers.run_ids.clone())),
            Arc::new(TimestampMillisecondArray::from(self.buffers.times.clone())),
            Arc::new(UInt64Array::from(self.buffers.fetch_indices.clone())),
            Arc::new(StringArray::from(self.buffers.metric_names.clone())),
            Arc::new(StringArray::from(self.buffers.metric_kinds.clone())),
            Arc::new(UInt64Array::from(self.buffers.values_int.clone())),
            Arc::new(Float64Array::from(self.buffers.values_float.clone())),
        ];

        // Build l_<key> columns for each label key in schema order
        for key in &self.schema_label_keys {
            let values: Vec<Option<&str>> = self
                .buffers
                .row_labels
                .iter()
                .map(|row_map| row_map.get(key).map(String::as_str))
                .collect();
            arrays.push(Arc::new(StringArray::from(values)));
        }

        // Add histogram column last
        arrays.push(Arc::new(
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
                .collect::<BinaryArray>(),
        ));

        debug_assert_eq!(
            arrays.len(),
            schema.fields().len(),
            "array count ({}) must match schema field count ({})",
            arrays.len(),
            schema.fields().len()
        );
        debug_assert!(
            arrays.iter().all(|a| a.len() == num_rows),
            "all arrays must have {num_rows} rows",
        );

        Ok(RecordBatch::try_new(schema.clone(), arrays)?)
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

        // Initialize writer on first flush when we know the label keys
        if self.writer.is_none() {
            self.initialize_writer()?;
        }

        let batch = self.buffers_to_record_batch()?;
        self.writer
            .as_mut()
            .expect("writer should be initialized")
            .write(&batch)?;
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

        // Close the ArrowWriter if it was created
        if let Some(writer) = self.writer {
            writer.close()?;
        }
        // If writer was never created (no data written), nothing to close
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

impl Format<std::io::BufWriter<std::fs::File>> {
    /// Rotate to a new output file
    ///
    /// Closes the current Parquet file (writing footer) and opens a new file
    /// at the specified path with the same compression settings.
    ///
    /// # Errors
    ///
    /// Returns an error if closing the current file or creating the new file fails.
    pub fn rotate_to(self, path: &std::path::Path) -> Result<Self, Error> {
        // Store compression level before closing
        let compression_level = self.compression_level;

        // Close current file (writes footer)
        self.close()?;

        // Create new file and writer
        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        let format = Self::new(writer, compression_level)?;

        Ok(format)
    }

    /// Get the compression level for this format
    #[must_use]
    pub fn compression_level(&self) -> i32 {
        self.compression_level
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

    #[test]
    fn writes_label_columns() {
        use arrow_array::{Array, RecordBatchReader};
        use bytes::Bytes;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let mut buffer = Cursor::new(Vec::new());

        {
            let mut format = Format::new(&mut buffer, 3).expect("create format");

            // Write metric with labels
            let mut labels = FxHashMap::default();
            labels.insert("container_id".to_string(), "abc123".to_string());
            labels.insert("namespace".to_string(), "default".to_string());
            labels.insert("qos_class".to_string(), "Guaranteed".to_string());

            let line = Line {
                run_id: Uuid::new_v4(),
                time: 1000,
                fetch_index: 0,
                metric_name: "test_metric".into(),
                metric_kind: MetricKind::Gauge,
                value: LineValue::Float(42.0),
                labels,
                value_histogram: Vec::new(),
            };

            format.write_metric(&line).expect("write should succeed");

            // Write another metric with different labels
            let mut labels2 = FxHashMap::default();
            labels2.insert("container_id".to_string(), "def456".to_string());
            labels2.insert("namespace".to_string(), "kube-system".to_string());
            // Note: no qos_class label

            let line2 = Line {
                run_id: Uuid::new_v4(),
                time: 2000,
                fetch_index: 1,
                metric_name: "test_metric".into(),
                metric_kind: MetricKind::Gauge,
                value: LineValue::Float(100.0),
                labels: labels2,
                value_histogram: Vec::new(),
            };

            format.write_metric(&line2).expect("write should succeed");
            format.close().expect("close should succeed");
        }

        // Read back and verify schema has l_* columns
        let data = Bytes::from(buffer.into_inner());
        let reader = ParquetRecordBatchReaderBuilder::try_new(data)
            .expect("create reader")
            .build()
            .expect("build reader");

        let schema = reader.schema();

        // Check that l_* columns exist (sorted alphabetically)
        assert!(
            schema.field_with_name("l_container_id").is_ok(),
            "should have l_container_id column"
        );
        assert!(
            schema.field_with_name("l_namespace").is_ok(),
            "should have l_namespace column"
        );
        assert!(
            schema.field_with_name("l_qos_class").is_ok(),
            "should have l_qos_class column"
        );

        // Check no labels MapArray column
        assert!(
            schema.field_with_name("labels").is_err(),
            "should NOT have labels column (replaced by l_* columns)"
        );

        // Read data and verify values
        let batches: Vec<_> = reader.into_iter().collect();
        assert_eq!(batches.len(), 1, "should have one batch");
        let batch = batches[0].as_ref().expect("batch should be ok");

        assert_eq!(batch.num_rows(), 2, "should have 2 rows");

        // Check l_container_id values
        let container_col = batch
            .column_by_name("l_container_id")
            .expect("l_container_id column");
        let container_arr = container_col
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array");
        assert_eq!(container_arr.value(0), "abc123");
        assert_eq!(container_arr.value(1), "def456");

        // Check l_qos_class values (second row should be null)
        let qos_col = batch
            .column_by_name("l_qos_class")
            .expect("l_qos_class column");
        let qos_arr = qos_col
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array");
        assert_eq!(qos_arr.value(0), "Guaranteed");
        assert!(qos_arr.is_null(1), "second row should have null qos_class");
    }
}
