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
    Array, ArrayAccessor, ArrayRef, BinaryArray, DictionaryArray, Float64Array, MapArray,
    RecordBatch, StringArray, StructArray, TimestampMillisecondArray, TypedDictionaryArray,
    UInt64Array, builder::StringDictionaryBuilder, types::Int32Type,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{ArrowError, Field, Schema};
use lading_capture_schema::{
    CAPTURE_VERSION_KEY, CURRENT_CAPTURE_VERSION, CaptureVersion, LabelEncoding, MetadataError,
    SchemaError, capture_schema, capture_schema_for_major, columns, parse_capture_version,
    validate_schema,
};
use parquet::{
    arrow::{
        ArrowWriter,
        arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder},
    },
    basic::{Compression, ZstdLevel},
    file::{
        metadata::{KeyValue, ParquetMetaDataReader},
        properties::{WriterProperties, WriterVersion},
        reader::ChunkReader,
    },
    schema::types::ColumnPath,
};

use crate::line;

/// Errors in Lading's metadata-aware capture reader.
#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    /// The file footer or Arrow schema cannot be read.
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    /// A present capture version has invalid metadata.
    #[error(transparent)]
    Metadata(#[from] MetadataError),
    /// Lading does not define the selected schema major version.
    #[error("unsupported schema major version {schema_major_version}")]
    UnsupportedMajor {
        /// Major version that the caller selected.
        schema_major_version: u64,
    },
    /// The declared capture version has a different major than the selected one.
    #[error("capture version {version} does not match schema major version {schema_major_version}")]
    VersionMismatch {
        /// Valid declared version.
        version: CaptureVersion,
        /// Major version that the caller selected.
        schema_major_version: u64,
    },
    /// A versioned file disagrees with its known field contract.
    #[error(transparent)]
    Schema(#[from] SchemaError),
}

/// Load metadata once and construct Lading's capture batch-reader builder.
///
/// The caller selects one schema major version. A declared version must have this major,
/// and its Arrow schema must satisfy that major's field contract. Minor versions of the same
/// major are compatible. Files without a version key follow the ordinary Parquet reading path.
///
/// # Errors
///
/// Return an error for an undefined schema major version, unreadable metadata, invalid
/// declared versions, a different declared major, or a mismatched declared schema.
pub fn reader_builder<T: ChunkReader + 'static>(
    reader: T,
    schema_major_version: u64,
) -> Result<ParquetRecordBatchReaderBuilder<T>, ReadError> {
    let expected =
        capture_schema_for_major(schema_major_version).ok_or(ReadError::UnsupportedMajor {
            schema_major_version,
        })?;
    let metadata = ParquetMetaDataReader::new().parse_and_finish(&reader)?;
    let version = parse_capture_version(
        metadata
            .file_metadata()
            .key_value_metadata()
            .into_iter()
            .flatten()
            .map(|entry| (entry.key.as_str(), entry.value.as_deref())),
    )?;
    if let Some(version) = version
        && version.major != schema_major_version
    {
        return Err(ReadError::VersionMismatch {
            version,
            schema_major_version,
        });
    }
    let metadata = ArrowReaderMetadata::try_new(Arc::new(metadata), ArrowReaderOptions::default())?;
    if version.is_some() {
        validate_schema(metadata.schema().as_ref(), &expected)?;
    }
    Ok(ParquetRecordBatchReaderBuilder::new_with_metadata(
        reader, metadata,
    ))
}

/// Label key or value strings from one schema major version, read without copies.
#[derive(Debug, Clone, Copy)]
pub enum LabelStrings<'a> {
    /// V1 stores label strings as `Utf8`.
    Utf8(&'a StringArray),
    /// V2 stores label strings as `Dictionary(Int32, Utf8)`.
    Dictionary(TypedDictionaryArray<'a, Int32Type, StringArray>),
}

impl<'a> LabelStrings<'a> {
    fn len(&self) -> usize {
        match self {
            Self::Utf8(array) => array.len(),
            Self::Dictionary(array) => array.len(),
        }
    }

    /// Iterate over the label strings in entry order.
    pub fn iter(self) -> impl Iterator<Item = &'a str> {
        // Indexes stay below `len`, so Arrow value access stays in bounds.
        (0..self.len()).map(move |index| match self {
            Self::Utf8(array) => array.value(index),
            Self::Dictionary(array) => array.value(index),
        })
    }
}

/// Read label strings with one label encoding.
///
/// Return `None` if `column` does not use `encoding`.
#[must_use]
pub fn resolve_labels(column: &dyn Array, encoding: LabelEncoding) -> Option<LabelStrings<'_>> {
    match encoding {
        LabelEncoding::Utf8 => column
            .as_any()
            .downcast_ref::<StringArray>()
            .map(LabelStrings::Utf8),
        LabelEncoding::Dictionary => column
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .and_then(DictionaryArray::downcast_dict::<StringArray>)
            .map(LabelStrings::Dictionary),
    }
}

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
        #[expect(clippy::cast_possible_truncation)]
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

        // Add labels for this row
        for (k, v) in &line.labels {
            self.label_keys.push(k.clone());
            self.label_values.push(v.clone());
        }
        #[expect(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        self.label_offsets.push(self.label_keys.len() as i32);

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
            .set_key_value_metadata(Some(vec![KeyValue::new(
                CAPTURE_VERSION_KEY.to_string(),
                Some(CURRENT_CAPTURE_VERSION.to_string()),
            )]))
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

        let mut keys_builder = StringDictionaryBuilder::<Int32Type>::new();
        for key in &self.buffers.label_keys {
            keys_builder.append(key)?;
        }
        let keys_array = Arc::new(keys_builder.finish());
        let mut values_builder = StringDictionaryBuilder::<Int32Type>::new();
        for value in &self.buffers.label_values {
            values_builder.append(value)?;
        }
        let values_array = Arc::new(values_builder.finish());
        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new(
                    columns::LABEL_KEY,
                    LabelEncoding::Dictionary.data_type(),
                    false,
                )),
                keys_array as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    columns::LABEL_VALUE,
                    LabelEncoding::Dictionary.data_type(),
                    false,
                )),
                values_array as ArrayRef,
            ),
        ]);

        let field = Arc::new(Field::new(
            columns::LABEL_ENTRIES,
            struct_array.data_type().clone(),
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

    mod resolve_labels {
        use super::super::resolve_labels;
        use arrow_array::{
            Array, DictionaryArray, Int32Array, LargeStringArray, StringArray,
            types::{Int8Type, Int32Type},
        };
        use lading_capture_schema::LabelEncoding;
        use proptest::{collection, prelude::Just, prop_assert_eq, proptest, strategy::Strategy};
        use std::sync::Arc;

        fn read(column: &dyn Array, encoding: LabelEncoding) -> Option<Vec<String>> {
            Some(
                resolve_labels(column, encoding)?
                    .iter()
                    .map(str::to_string)
                    .collect(),
            )
        }

        proptest! {
            /// For all string vectors v and offsets o <= len(v): `resolve_labels` on the slice
            /// v[o..] yields v[o..] for `Utf8` and for `Dictionary`.
            #[test]
            fn both_encodings_yield_entry_order(
                (values, offset) in collection::vec("[a-z]{0,4}", 0..16)
                    .prop_flat_map(|values| {
                        let len = values.len();
                        (Just(values), 0..=len)
                    }),
            ) {
                let len = values.len() - offset;
                let expected = values[offset..].to_vec();
                let utf8 = StringArray::from(values.clone()).slice(offset, len);
                let dictionary: DictionaryArray<Int32Type> =
                    values.iter().map(String::as_str).collect();
                let dictionary = dictionary.slice(offset, len);
                prop_assert_eq!(read(&utf8, LabelEncoding::Utf8), Some(expected.clone()));
                prop_assert_eq!(read(&dictionary, LabelEncoding::Dictionary), Some(expected));
            }
        }

        /// For each (column, E) in the table, where column does not use encoding E:
        /// `resolve_labels(column, E) = None`.
        #[test]
        fn other_representations_fail() -> Result<(), arrow_schema::ArrowError> {
            let utf8 = StringArray::from(vec!["env"]);
            let dictionary: DictionaryArray<Int32Type> = vec!["env"].into_iter().collect();
            let small_keys: DictionaryArray<Int8Type> = vec!["env"].into_iter().collect();
            let large_values = DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![0]),
                Arc::new(LargeStringArray::from(vec!["env"])),
            )?;
            let cases: [(&dyn Array, LabelEncoding); 4] = [
                (&utf8, LabelEncoding::Dictionary),
                (&dictionary, LabelEncoding::Utf8),
                (&small_keys, LabelEncoding::Dictionary),
                (&large_values, LabelEncoding::Dictionary),
            ];
            for (column, encoding) in cases {
                assert!(resolve_labels(column, encoding).is_none());
            }
            Ok(())
        }
    }

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
        for i in 0_u32..5 {
            let line = Line {
                run_id: Uuid::new_v4(),
                time: 1000 + u128::from(i),
                fetch_index: u64::from(i),
                metric_name: format!("metric_{i}"),
                metric_kind: MetricKind::Gauge,
                value: LineValue::Float(f64::from(i)),
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
