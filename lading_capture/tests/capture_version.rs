//! Verify version metadata and compatibility independently of current writer assumptions.

#[path = "support/reference.rs"]
mod reference;

use std::{
    error::Error,
    io::Cursor,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use arrow_array::{
    Array, ArrayRef, BooleanArray, MapArray, RecordBatch, StructArray,
    builder::StringDictionaryBuilder, types::Int32Type,
};
use arrow_schema::{DataType, Field, Fields, Schema};
use bytes::Bytes;
use datadog_protos::metrics::Dogsketch;
use ddsketch_agent::DDSketch;
use lading_capture::{
    formats::parquet::{Format, ReadError, reader_builder},
    line::{Line, LineValue, MetricKind},
    validate::parquet::validate_parquet,
};
use lading_capture_schema::{
    CAPTURE_VERSION_KEY, CURRENT_CAPTURE_VERSION, Version, capture_schema, parse_capture_version,
};
use parquet::{
    arrow::{
        ARROW_SCHEMA_META_KEY, ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder,
        arrow_writer::ArrowWriterOptions,
    },
    file::{
        metadata::KeyValue,
        properties::WriterProperties,
        reader::{ChunkReader, Length},
    },
};
use proptest::{collection, prelude::any, prop_assert_eq, proptest, test_runner::TestCaseError};
use protobuf::Message;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
const PRE_V1: &[u8] = include_bytes!("fixtures/pre_v1.parquet");
const V1: &[u8] = include_bytes!("fixtures/v1.parquet");

fn lines(values: &[u64]) -> Vec<Line> {
    (0u64..)
        .zip(values)
        .map(|(tick, value)| Line {
            run_id: Uuid::nil(),
            time: 1000 + u128::from(tick) * 1000,
            fetch_index: tick,
            metric_name: "requests".to_string(),
            metric_kind: MetricKind::Counter,
            value: LineValue::Int(*value),
            labels: if tick % 3 == 1 {
                [("env".to_string(), "staging".to_string())]
                    .into_iter()
                    .collect()
            } else {
                [
                    ("env".to_string(), "prod".to_string()),
                    ("service".to_string(), "api".to_string()),
                ]
                .into_iter()
                .collect()
            },
            value_histogram: Vec::new(),
        })
        .collect()
}

fn write_capture(lines: &[Line]) -> TestResult<Bytes> {
    let mut output = Cursor::new(Vec::new());
    let mut writer = Format::new(&mut output, 3)?;
    for line in lines {
        writer.write_metric(line)?;
    }
    writer.close()?;
    Ok(Bytes::from(output.into_inner()))
}

fn version(data: Bytes) -> TestResult<Option<Version>> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(data)?;
    Ok(parse_capture_version(
        builder
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .into_iter()
            .flatten()
            .map(|entry| (entry.key.as_str(), entry.value.as_deref())),
    )?)
}

fn declared(value: Option<&str>) -> Vec<KeyValue> {
    vec![KeyValue::new(
        CAPTURE_VERSION_KEY.to_string(),
        value.map(str::to_string),
    )]
}

fn write_batches(
    schema: Arc<Schema>,
    batches: &[RecordBatch],
    metadata: Vec<KeyValue>,
    store_arrow_schema: bool,
) -> TestResult<Bytes> {
    let properties = WriterProperties::builder()
        .set_key_value_metadata(Some(metadata))
        .build();
    let options = ArrowWriterOptions::new()
        .with_properties(properties)
        .with_skip_arrow_metadata(!store_arrow_schema);
    let mut output = Vec::new();
    let mut writer = ArrowWriter::try_new_with_options(&mut output, schema, options)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.close()?;
    Ok(Bytes::from(output))
}

fn input_batch() -> TestResult<RecordBatch> {
    let mut reader =
        ParquetRecordBatchReaderBuilder::try_new(Bytes::from_static(PRE_V1))?.build()?;
    let batch = reader.next().ok_or("missing fixture batch")??;
    if reader.next().transpose()?.is_some() {
        return Err("expected exactly one fixture batch".into());
    }
    Ok(batch)
}

#[test]
fn new_metadata_preserves_frozen_reader_output() -> TestResult {
    let data = write_capture(&lines(&[10, 20, 30]))?;
    assert_eq!(version(data.clone())?, Some(CURRENT_CAPTURE_VERSION));
    assert_eq!(reference::read(&data)?, reference::read(V1)?);
    let file = tempfile::NamedTempFile::new()?;
    std::fs::write(file.path(), &data)?;
    let validation = validate_parquet(file.path(), None)?;
    assert!(validation.is_valid());
    assert_eq!(validation.line_count, 3);
    assert_eq!(validation.unique_series, 2);
    Ok(())
}

#[test]
fn zero_rows_still_have_version_and_schema() -> TestResult {
    let data = write_capture(&[])?;
    assert_eq!(version(data.clone())?, Some(CURRENT_CAPTURE_VERSION));
    let builder = reader_builder(data.clone())?;
    assert_eq!(builder.metadata().file_metadata().num_rows(), 0);
    assert_eq!(builder.schema().fields(), capture_schema().fields());
    assert!(reference::read(&data)?.is_empty());
    Ok(())
}

/// Both frozen baselines remain readable without changing their metadata or records.
#[test]
fn frozen_fixtures_work_with_current_reader() -> TestResult {
    let expected_records = reference::read(PRE_V1)?;
    for (filename, data, declared_version) in [
        ("pre_v1.parquet", PRE_V1, None),
        ("v1.parquet", V1, Some(Version::new(1, 0, 0))),
    ] {
        assert_eq!(
            version(Bytes::from_static(data))?,
            declared_version,
            "{filename}"
        );
        let batches = reader_builder(Bytes::from_static(data))?
            .with_batch_size(1)
            .build()?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(batches.len(), 3, "{filename}");
        assert!(
            batches.iter().all(|batch| batch.num_rows() == 1),
            "{filename}"
        );
        assert_eq!(reference::read(data)?, expected_records, "{filename}");
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(filename);
        let validation = validate_parquet(path, None)?;
        assert!(validation.is_valid(), "{filename}");
        assert_eq!(validation.line_count, 3, "{filename}");
        assert_eq!(validation.unique_series, 2, "{filename}");
    }
    Ok(())
}

proptest! {
    /// For all scalar inputs: the unchanged 1.x reader preserves the writer's integer values.
    #[test]
    fn unchanged_reader_preserves_values(values in collection::vec(any::<u64>(), 1..32)) {
        let data = write_capture(&lines(&values)).map_err(|error| TestCaseError::fail(error.to_string()))?;
        let records = reference::read(&data)?;
        prop_assert_eq!(records.len(), values.len());
        for (record, expected) in records.iter().zip(values) {
            prop_assert_eq!(record.value_int, Some(expected));
            prop_assert_eq!(record.value_float_bits, None);
        }
    }
}

/// Any column permutation and ignorable top-level field preserve 1.x decoding.
#[test]
fn column_order_and_extras_are_not_version_boundaries() -> TestResult {
    let expected = reference::read(PRE_V1)?;
    let batch = input_batch()?;
    proptest!(|(order in collection::vec(any::<u64>(), 9), insertion in 0usize..10)| {
        let result = reordered_capture(&batch, order, insertion)
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        let decoded = reference::read(&result)?;
        prop_assert_eq!(decoded.as_slice(), expected.as_slice());
        let reader = reader_builder(result)?.with_batch_size(1).build()?;
        let batches = reader.collect::<Result<Vec<_>, _>>()?;
        prop_assert_eq!(batches.len(), 3);
    });
    Ok(())
}

fn reordered_capture(batch: &RecordBatch, order: Vec<u64>, insertion: usize) -> TestResult<Bytes> {
    let mut fields: Vec<_> = order
        .into_iter()
        .zip(batch.schema().fields().iter().cloned())
        .collect();
    fields.sort_by_key(|(key, _)| *key);
    let mut fields: Vec<_> = fields.into_iter().map(|(_, field)| field).collect();
    fields.insert(
        insertion,
        Arc::new(Field::new("diagnostic_note", DataType::Boolean, true)),
    );
    let schema = Arc::new(Schema::new(fields));
    let arrays: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| {
            if field.name() == "diagnostic_note" {
                Ok(Arc::new(BooleanArray::from(vec![Some(true); batch.num_rows()])) as ArrayRef)
            } else {
                batch
                    .column_by_name(field.name())
                    .cloned()
                    .ok_or("missing fixture column")
            }
        })
        .collect::<Result<_, _>>()?;
    let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)?;
    write_batches(schema, &[batch], declared(Some("1.1.0")), true)
}

/// For all dictionary settings: decoded rows equal the baseline across three row groups.
#[test]
fn page_dictionary_encoding_keeps_unchanged_readers() -> TestResult {
    let batch = input_batch()?;
    for dictionary in [false, true] {
        let properties = WriterProperties::builder()
            .set_dictionary_enabled(dictionary)
            .set_max_row_group_size(1)
            .set_key_value_metadata(Some(declared(Some("1.0.0"))))
            .build();
        let mut output = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut output, batch.schema(), Some(properties))?;
        writer.write(&batch)?;
        writer.close()?;
        let data = Bytes::from(output);
        assert_eq!(reader_builder(data.clone())?.metadata().num_row_groups(), 3);
        assert_eq!(reference::read(&data)?, reference::read(PRE_V1)?);
    }
    Ok(())
}

#[test]
fn physical_row_order_does_not_change_the_contract() -> TestResult {
    let batch = input_batch()?;
    let reversed: Vec<_> = (0..batch.num_rows())
        .rev()
        .map(|row| batch.slice(row, 1))
        .collect();
    let data = write_batches(batch.schema(), &reversed, declared(Some("1.0.0")), true)?;
    assert_eq!(reference::read(&data)?, reference::read(PRE_V1)?);
    Ok(())
}

#[test]
fn scalar_kinds_and_histograms_keep_existing_representation() -> TestResult {
    let cases = [
        (MetricKind::Counter, LineValue::Int(10)),
        (MetricKind::Counter, LineValue::Float(1.5)),
        (MetricKind::Gauge, LineValue::Int(30)),
        (MetricKind::Gauge, LineValue::Float(-2.5)),
        (MetricKind::Histogram, LineValue::Float(0.0)),
    ];
    let mut records = lines(&[10, 20, 30, 40, 50]);
    assert_eq!(records.len(), cases.len());
    for (record, (kind, value)) in records.iter_mut().zip(cases) {
        record.metric_kind = kind;
        record.value = value;
    }
    let mut sketch = DDSketch::default();
    sketch.insert(11.0);
    sketch.insert(13.0);
    let mut payload = Dogsketch::new();
    sketch.merge_to_dogsketch(&mut payload);
    let payload = payload.write_to_bytes()?;
    records
        .last_mut()
        .ok_or("missing histogram case")?
        .value_histogram
        .clone_from(&payload);
    let data = write_capture(&records)?;
    let decoded = reference::read(&data)?;
    for (input, output) in records.iter().zip(&decoded) {
        match input.value {
            LineValue::Int(value) => {
                assert_eq!(output.value_int, Some(value));
                assert!(output.value_float_bits.is_none());
            }
            LineValue::Float(value) => {
                assert_eq!(output.value_float_bits, Some(value.to_bits()));
                assert!(output.value_int.is_none());
            }
        }
    }
    assert_eq!(
        decoded
            .last()
            .and_then(|record| record.value_histogram.as_ref()),
        Some(&payload)
    );
    Ok(())
}

#[test]
fn nonfinite_scalars_do_not_gain_new_validation() -> TestResult {
    let values = [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.0];
    let mut records = lines(&[0, 0, 0, 0]);
    for (record, value) in records.iter_mut().zip(values) {
        record.metric_kind = MetricKind::Gauge;
        record.value = LineValue::Float(value);
    }
    let data = write_capture(&records)?;
    let decoded = reference::read(&data)?;
    assert_eq!(decoded.len(), values.len());
    for (record, value) in decoded.iter().zip(values) {
        assert_eq!(record.value_float_bits, Some(value.to_bits()));
    }
    Ok(())
}

#[test]
fn malformed_metadata_never_falls_back_to_legacy() -> TestResult {
    for metadata in [
        declared(None),
        declared(Some("")),
        declared(Some("1.0")),
        declared(Some("1.0.1")),
        vec![KeyValue::new(CAPTURE_VERSION_KEY.to_string(), Some("1.0.0".to_string())); 2],
    ] {
        let data = write_batches(Arc::new(capture_schema()), &[], metadata, true)?;
        assert!(matches!(reader_builder(data), Err(ReadError::Metadata(_))));
    }
    Ok(())
}

#[test]
fn unsupported_major_is_checked_before_arrow_schema() -> TestResult {
    let metadata = vec![
        KeyValue::new(CAPTURE_VERSION_KEY.to_string(), Some("99.0.0".to_string())),
        KeyValue::new(
            ARROW_SCHEMA_META_KEY.to_string(),
            Some("invalid Arrow schema".to_string()),
        ),
    ];
    let data = write_batches(Arc::new(capture_schema()), &[], metadata, false)?;
    assert!(
        matches!(reader_builder(data), Err(ReadError::UnsupportedVersion { version, .. }) if version.major == 99)
    );
    Ok(())
}

#[test]
fn versioned_schema_checks_do_not_tighten_legacy() -> TestResult {
    let batch = input_batch()?;
    let fields: Vec<_> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| {
            if field.name() == "time" {
                Arc::new(field.as_ref().clone().with_nullable(true))
            } else {
                Arc::clone(field)
            }
        })
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(Arc::clone(&schema), batch.columns().to_vec())?;
    let legacy = write_batches(
        Arc::clone(&schema),
        std::slice::from_ref(&batch),
        vec![],
        true,
    )?;
    assert!(
        reader_builder(legacy)?
            .build()?
            .next()
            .transpose()?
            .is_some()
    );
    let tagged = write_batches(schema, &[batch], declared(Some("1.0.0")), true)?;
    assert!(matches!(reader_builder(tagged), Err(ReadError::Schema(_))));
    Ok(())
}

fn replace_labels(batch: &RecordBatch, labels: MapArray) -> TestResult<RecordBatch> {
    let labels: ArrayRef = Arc::new(labels);
    let (fields, columns): (Vec<_>, Vec<_>) = batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, array)| {
            if field.name() == "labels" {
                (
                    Arc::new(
                        field
                            .as_ref()
                            .clone()
                            .with_data_type(labels.data_type().clone()),
                    ),
                    Arc::clone(&labels),
                )
            } else {
                (Arc::clone(field), Arc::clone(array))
            }
        })
        .unzip();
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

/// Reversing map roles must change decoded labels and fail the declared schema check.
#[test]
fn reversed_map_roles_are_not_compatible() -> TestResult {
    let batch = input_batch()?;
    let labels = batch
        .column_by_name("labels")
        .ok_or("missing labels")?
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or("labels are not a map")?;
    let old = labels.entries();
    let fields = Fields::from(vec![
        Arc::clone(&old.fields()[1]),
        Arc::clone(&old.fields()[0]),
    ]);
    let entries = StructArray::try_new(
        fields,
        vec![Arc::clone(old.column(1)), Arc::clone(old.column(0))],
        None,
    )?;
    let labels = MapArray::try_new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        labels.offsets().clone(),
        entries,
        None,
        false,
    )?;
    let batch = replace_labels(&batch, labels)?;
    let data = write_batches(batch.schema(), &[batch], declared(Some("1.0.0")), true)?;
    let decoded = reference::read(&data)?;
    assert_ne!(decoded, reference::read(PRE_V1)?);
    let first = decoded.first().ok_or("missing decoded row")?;
    assert_eq!(first.labels.get("prod").map(String::as_str), Some("env"));
    assert!(matches!(reader_builder(data), Err(ReadError::Schema(_))));
    Ok(())
}

#[test]
fn dictionary_representation_is_not_a_same_major_change() -> TestResult {
    let batch = input_batch()?;
    let labels = batch
        .column_by_name("labels")
        .ok_or("missing labels")?
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or("labels are not a map")?;
    let mut keys = StringDictionaryBuilder::<Int32Type>::new();
    let mut values = StringDictionaryBuilder::<Int32Type>::new();
    let key_values = labels
        .keys()
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .ok_or("invalid keys")?;
    let value_values = labels
        .values()
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .ok_or("invalid values")?;
    for value in key_values {
        keys.append(value.ok_or("null key")?)?;
    }
    for value in value_values {
        values.append(value.ok_or("null value")?)?;
    }
    let keys: ArrayRef = Arc::new(keys.finish());
    let values: ArrayRef = Arc::new(values.finish());
    let fields = Fields::from(vec![
        Field::new("key", keys.data_type().clone(), false),
        Field::new("value", values.data_type().clone(), false),
    ]);
    let entries = StructArray::try_new(fields, vec![keys, values], None)?;
    let entry = Arc::new(Field::new("entries", entries.data_type().clone(), false));
    let labels = MapArray::try_new(entry, labels.offsets().clone(), entries, None, false)?;
    let batch = replace_labels(&batch, labels)?;
    let data = write_batches(batch.schema(), &[batch], declared(Some("1.1.0")), true)?;
    assert!(reference::read(&data).is_err());
    assert!(matches!(reader_builder(data), Err(ReadError::Schema(_))));
    Ok(())
}

#[derive(Clone, Debug)]
struct CountingReader {
    data: Bytes,
    calls: Arc<AtomicUsize>,
}

impl Length for CountingReader {
    fn len(&self) -> u64 {
        Length::len(&self.data)
    }
}

impl ChunkReader for CountingReader {
    type T = <Bytes as ChunkReader>::T;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        ChunkReader::get_read(&self.data, start)
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        ChunkReader::get_bytes(&self.data, start, length)
    }
}

#[test]
fn metadata_preflight_does_not_duplicate_io() -> TestResult {
    let reader = CountingReader {
        data: write_capture(&lines(&[10, 20, 30]))?,
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let _baseline = ParquetRecordBatchReaderBuilder::try_new(reader.clone())?;
    let baseline = reader.calls.swap(0, Ordering::Relaxed);
    let builder = reader_builder(reader.clone())?;
    assert_eq!(reader.calls.load(Ordering::Relaxed), baseline);
    let rows = builder
        .with_batch_size(1)
        .build()?
        .map(|batch| batch.map(|batch| batch.num_rows()))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(rows, vec![1, 1, 1]);
    Ok(())
}
