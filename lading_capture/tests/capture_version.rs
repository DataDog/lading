//! Check versioned captures against frozen V1 and V2 schema and record expectations.

#[path = "support/frozen.rs"]
mod frozen;

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
use frozen::Major;
use lading_capture::{
    formats::parquet::{Format, ReadError, reader_builder},
    line::{Line, LineValue, MetricKind},
    validate::parquet::validate_parquet,
};
use lading_capture_schema::{
    CAPTURE_VERSION_KEY, CURRENT_CAPTURE_VERSION, CaptureVersion, capture_schema,
    parse_capture_version,
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
use proptest::{
    collection,
    prelude::{Just, any},
    prop_assert, prop_assert_eq, prop_oneof, proptest,
    test_runner::TestCaseError,
};
use protobuf::Message;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
const MAJORS: [Major; 2] = [Major::V1, Major::V2];
/// The frozen baseline of the current writer's major.
const CURRENT: Major = Major::V2;

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
    let data = Bytes::from(output.into_inner());
    assert_contract(data.clone(), CURRENT)?;
    Ok(data)
}

fn version(data: Bytes) -> TestResult<Option<CaptureVersion>> {
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

fn assert_contract(data: Bytes, major: Major) -> TestResult {
    let declared = version(data.clone())?.ok_or("missing capture version")?;
    assert_eq!(
        declared.major,
        major.number(),
        "add a frozen contract for capture version {declared}"
    );
    let actual = ParquetRecordBatchReaderBuilder::try_new(data)?;
    lading_capture_schema::validate_schema(actual.schema(), &major.schema()?)?;
    let baseline = ParquetRecordBatchReaderBuilder::try_new(Bytes::from_static(major.capture()))?;
    for expected in baseline.parquet_schema().root_schema().get_fields() {
        let observed = actual
            .parquet_schema()
            .root_schema()
            .get_fields()
            .iter()
            .find(|field| field.name() == expected.name())
            .ok_or("missing native Parquet field")?;
        assert_eq!(observed, expected);
    }
    Ok(())
}

fn input_batch(major: Major) -> TestResult<RecordBatch> {
    let mut reader =
        ParquetRecordBatchReaderBuilder::try_new(Bytes::from_static(major.capture()))?.build()?;
    let batch = reader.next().ok_or("missing fixture batch")??;
    if reader.next().transpose()?.is_some() {
        return Err("expected exactly one fixture batch".into());
    }
    Ok(batch)
}

/// `CURRENT_CAPTURE_VERSION.major = CURRENT.number()`, so the current writer major has a
/// frozen baseline.
#[test]
fn current_writer_has_a_frozen_baseline() {
    assert_eq!(CURRENT_CAPTURE_VERSION.major, CURRENT.number());
}

#[test]
fn writer_output_preserves_current_records() -> TestResult {
    let data = write_capture(&lines(&[10, 20, 30]))?;
    assert_eq!(version(data.clone())?, Some(CURRENT_CAPTURE_VERSION));
    assert_eq!(
        frozen::read(&data, CURRENT)?,
        frozen::read(CURRENT.capture(), CURRENT)?
    );
    let file = tempfile::NamedTempFile::new()?;
    std::fs::write(file.path(), &data)?;
    let validation = validate_parquet(file.path(), None, CURRENT_CAPTURE_VERSION.major)?;
    assert!(validation.is_valid());
    assert_eq!(validation.line_count, 3);
    assert_eq!(validation.unique_series, 2);
    Ok(())
}

#[test]
fn zero_rows_still_have_version_and_schema() -> TestResult {
    let data = write_capture(&[])?;
    assert_eq!(version(data.clone())?, Some(CURRENT_CAPTURE_VERSION));
    let builder = reader_builder(data.clone(), CURRENT_CAPTURE_VERSION.major)?;
    assert_eq!(builder.metadata().file_metadata().num_rows(), 0);
    lading_capture_schema::validate_schema(builder.schema(), &CURRENT.schema()?)?;
    assert!(frozen::read(&data, CURRENT)?.is_empty());
    Ok(())
}

fn expected_records() -> Vec<frozen::Record> {
    [
        (0u64, 1000, 10, vec![("env", "prod"), ("service", "api")]),
        (1, 2000, 20, vec![("env", "staging")]),
        (2, 3000, 30, vec![("env", "prod"), ("service", "api")]),
    ]
    .into_iter()
    .map(|(tick, time, value, labels)| frozen::Record {
        run_id: [0; 16],
        time,
        fetch_index: tick,
        metric_name: "requests".to_string(),
        metric_kind: "counter".to_string(),
        value_int: Some(value),
        value_float_bits: None,
        labels: labels
            .into_iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect(),
        value_histogram: None,
    })
    .collect()
}

fn fixture_path(major: Major) -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
        "tests/fixtures/v{major}.parquet",
        major = major.number()
    ))
}

/// For all frozen majors M: the M.0 capture keeps its declared schema and logical records
/// when Lading reads it with schema major version M.
#[test]
fn frozen_captures_work_with_their_selected_major() -> TestResult {
    for major in MAJORS {
        let data = Bytes::from_static(major.capture());
        assert_eq!(
            version(data.clone())?,
            Some(CaptureVersion::new(major.number(), 0))
        );
        assert_contract(data.clone(), major)?;
        let batches = reader_builder(data, major.number())?
            .with_batch_size(1)
            .build()?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(batches.len(), 3);
        assert!(batches.iter().all(|batch| batch.num_rows() == 1));
        assert_eq!(frozen::read(major.capture(), major)?, expected_records());
        let validation = validate_parquet(fixture_path(major), None, major.number())?;
        assert!(validation.is_valid());
        assert_eq!(validation.line_count, 3);
        assert_eq!(validation.unique_series, 2);
    }
    Ok(())
}

/// For all frozen majors M and selected majors S != M: Lading rejects the M.0 capture.
#[test]
fn selected_major_rejects_other_declared_majors() {
    for (declared, selected) in [(Major::V1, Major::V2), (Major::V2, Major::V1)] {
        let data = Bytes::from_static(declared.capture());
        assert!(matches!(
            reader_builder(data, selected.number()),
            Err(ReadError::VersionMismatch { version, schema_major_version })
                if version.major == declared.number() && schema_major_version == selected.number()
        ));
        assert!(matches!(
            validate_parquet(fixture_path(declared), None, selected.number()),
            Err(lading_capture::validate::parquet::Error::Capture(
                ReadError::VersionMismatch { .. }
            ))
        ));
    }
}

proptest! {
    /// For all major not in {1, 2}: reader_builder rejects the selection without reading the file.
    #[test]
    fn undefined_selected_major_fails_before_io(
        major in prop_oneof![Just(0u64), Just(3), Just(u64::MAX), any::<u64>()],
    ) {
        proptest::prop_assume!(!matches!(major, 1 | 2));
        let reader = CountingReader {
            data: Bytes::from_static(CURRENT.capture()),
            calls: Arc::new(AtomicUsize::new(0)),
        };
        let result = reader_builder(reader.clone(), major);
        let rejected = matches!(
            result,
            Err(ReadError::UnsupportedMajor { schema_major_version }) if schema_major_version == major
        );
        prop_assert!(rejected);
        prop_assert_eq!(reader.calls.load(Ordering::Relaxed), 0);
    }
}

proptest! {
    /// For all scalar inputs: current-major decoding preserves the writer's integer values.
    #[test]
    fn current_decoder_preserves_values(values in collection::vec(any::<u64>(), 1..32)) {
        let data = write_capture(&lines(&values)).map_err(|error| TestCaseError::fail(error.to_string()))?;
        let records = frozen::read(&data, CURRENT)?;
        prop_assert_eq!(records.len(), values.len());
        for (record, expected) in records.iter().zip(values) {
            prop_assert_eq!(record.value_int, Some(expected));
            prop_assert_eq!(record.value_float_bits, None);
        }
    }
}

/// For all frozen majors M: any column permutation and ignorable top-level field preserve M.x decoding.
#[test]
fn column_order_and_extras_are_not_version_boundaries() -> TestResult {
    for major in MAJORS {
        let expected = frozen::read(major.capture(), major)?;
        let batch = input_batch(major)?;
        proptest!(|(order in collection::vec(any::<u64>(), 9), insertion in 0usize..10)| {
            let result = reordered_capture(&batch, order, insertion, major)
                .map_err(|error| TestCaseError::fail(error.to_string()))?;
            assert_contract(result.clone(), major)
                .map_err(|error| TestCaseError::fail(error.to_string()))?;
            let decoded = frozen::read(&result, major)?;
            prop_assert_eq!(decoded.as_slice(), expected.as_slice());
            let reader = reader_builder(result, major.number())?.with_batch_size(1).build()?;
            let batches = reader.collect::<Result<Vec<_>, _>>()?;
            prop_assert_eq!(batches.len(), 3);
        });
    }
    Ok(())
}

fn tag(major: Major, minor: u64) -> Vec<KeyValue> {
    declared(Some(
        &CaptureVersion::new(major.number(), minor).to_string(),
    ))
}

fn reordered_capture(
    batch: &RecordBatch,
    order: Vec<u64>,
    insertion: usize,
    major: Major,
) -> TestResult<Bytes> {
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
    write_batches(schema, &[batch], tag(major, 1), true)
}

/// For all frozen majors and page dictionary settings: decoded rows equal the baseline
/// across three row groups.
#[test]
fn page_dictionary_encoding_preserves_frozen_contracts() -> TestResult {
    for major in MAJORS {
        let batch = input_batch(major)?;
        for dictionary in [false, true] {
            let properties = WriterProperties::builder()
                .set_dictionary_enabled(dictionary)
                .set_max_row_group_row_count(Some(1))
                .set_key_value_metadata(Some(tag(major, 0)))
                .build();
            let mut output = Vec::new();
            let mut writer = ArrowWriter::try_new(&mut output, batch.schema(), Some(properties))?;
            writer.write(&batch)?;
            writer.close()?;
            let data = Bytes::from(output);
            assert_contract(data.clone(), major)?;
            let builder = reader_builder(data.clone(), major.number())?;
            assert_eq!(builder.metadata().num_row_groups(), 3);
            assert_eq!(
                frozen::read(&data, major)?,
                frozen::read(major.capture(), major)?
            );
        }
    }
    Ok(())
}

#[test]
fn physical_row_order_does_not_change_the_contract() -> TestResult {
    for major in MAJORS {
        let batch = input_batch(major)?;
        let reversed: Vec<_> = (0..batch.num_rows())
            .rev()
            .map(|row| batch.slice(row, 1))
            .collect();
        let data = write_batches(batch.schema(), &reversed, tag(major, 0), true)?;
        assert_contract(data.clone(), major)?;
        assert_eq!(
            frozen::read(&data, major)?,
            frozen::read(major.capture(), major)?
        );
    }
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
    let decoded = frozen::read(&data, CURRENT)?;
    assert_eq!(decoded.len(), records.len());
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
    let decoded = frozen::read(&data, CURRENT)?;
    assert_eq!(decoded.len(), values.len());
    for (record, value) in decoded.iter().zip(values) {
        assert_eq!(record.value_float_bits, Some(value.to_bits()));
    }
    Ok(())
}

#[test]
fn malformed_version_metadata_is_rejected() -> TestResult {
    for metadata in [
        declared(None),
        declared(Some("")),
        declared(Some("1.0.0")),
        declared(Some("1.0.1")),
        vec![KeyValue::new(CAPTURE_VERSION_KEY.to_string(), Some("1.0".to_string())); 2],
    ] {
        let data = write_batches(Arc::new(capture_schema()), &[], metadata, true)?;
        assert!(matches!(
            reader_builder(data, CURRENT_CAPTURE_VERSION.major),
            Err(ReadError::Metadata(_))
        ));
    }
    Ok(())
}

/// For all frozen majors M: an untagged file with M's layout reads with any defined major,
/// and the selected major chooses label decoding during validation.
#[test]
fn missing_version_uses_standard_parquet_reader() -> TestResult {
    for (layout, other) in [(Major::V1, Major::V2), (Major::V2, Major::V1)] {
        let batch = input_batch(layout)?;
        let data = write_batches(batch.schema(), &[batch], vec![], true)?;
        assert_eq!(version(data.clone())?, None);
        for selected in MAJORS {
            let batches = reader_builder(data.clone(), selected.number())?
                .with_batch_size(1)
                .build()?
                .collect::<Result<Vec<_>, _>>()?;
            assert_eq!(batches.len(), 3);
        }
        assert_eq!(
            frozen::read(&data, layout)?,
            frozen::read(layout.capture(), layout)?
        );
        let file = tempfile::NamedTempFile::new()?;
        std::fs::write(file.path(), &data)?;
        assert!(validate_parquet(file.path(), None, layout.number())?.is_valid());
        assert!(matches!(
            validate_parquet(file.path(), None, other.number()),
            Err(lading_capture::validate::parquet::Error::VersionMismatch(message))
                if message.starts_with(&format!("schema major version {other} uses ", other = other.number()))
        ));
    }
    Ok(())
}

#[test]
fn other_declared_major_is_checked_before_arrow_schema() -> TestResult {
    let metadata = vec![
        KeyValue::new(CAPTURE_VERSION_KEY.to_string(), Some("99.0".to_string())),
        KeyValue::new(
            ARROW_SCHEMA_META_KEY.to_string(),
            Some("invalid Arrow schema".to_string()),
        ),
    ];
    let data = write_batches(Arc::new(capture_schema()), &[], metadata, false)?;
    assert!(matches!(
        reader_builder(data, CURRENT_CAPTURE_VERSION.major),
        Err(ReadError::VersionMismatch { version, .. }) if version.major == 99
    ));
    Ok(())
}

#[test]
fn nullable_mandatory_fields_fail_frozen_schema_checks() -> TestResult {
    for major in MAJORS {
        let batch = input_batch(major)?;
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
        let tagged = write_batches(schema, &[batch], tag(major, 0), true)?;
        assert!(matches!(
            reader_builder(tagged, major.number()),
            Err(ReadError::Schema(_))
        ));
    }
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

/// For all frozen majors: reversing map roles changes decoded labels and fails the declared
/// schema check.
#[test]
fn reversed_map_roles_are_not_compatible() -> TestResult {
    for major in MAJORS {
        let batch = input_batch(major)?;
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
        let data = write_batches(batch.schema(), &[batch], tag(major, 0), true)?;
        let decoded = frozen::read(&data, major)?;
        assert_ne!(decoded, frozen::read(major.capture(), major)?);
        let first = decoded.first().ok_or("missing decoded row")?;
        assert_eq!(first.labels.get("prod").map(String::as_str), Some("env"));
        assert!(matches!(
            reader_builder(data, major.number()),
            Err(ReadError::Schema(_))
        ));
    }
    Ok(())
}

/// Dictionary labels keep the native Parquet schema but fail the V1 Arrow contract, so they
/// need a new major. The same batch tagged 2.0 satisfies the V2 contract and records.
#[test]
fn dictionary_labels_need_a_new_major() -> TestResult {
    let batch = input_batch(Major::V1)?;
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
    let data = write_batches(
        batch.schema(),
        std::slice::from_ref(&batch),
        tag(Major::V1, 1),
        true,
    )?;
    let actual = ParquetRecordBatchReaderBuilder::try_new(data.clone())?;
    let baseline =
        ParquetRecordBatchReaderBuilder::try_new(Bytes::from_static(Major::V1.capture()))?;
    assert_eq!(
        actual.parquet_schema().root_schema(),
        baseline.parquet_schema().root_schema()
    );
    assert!(lading_capture_schema::validate_schema(actual.schema(), &Major::V1.schema()?).is_err());
    assert!(frozen::read(&data, Major::V1).is_err());
    assert!(matches!(
        reader_builder(data, Major::V1.number()),
        Err(ReadError::Schema(_))
    ));

    let data = write_batches(batch.schema(), &[batch], tag(Major::V2, 0), true)?;
    assert_contract(data.clone(), Major::V2)?;
    reader_builder(data.clone(), Major::V2.number())?;
    assert_eq!(frozen::read(&data, Major::V2)?, expected_records());
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
    let builder = reader_builder(reader.clone(), CURRENT_CAPTURE_VERSION.major)?;
    assert_eq!(reader.calls.load(Ordering::Relaxed), baseline);
    let rows = builder
        .with_batch_size(1)
        .build()?
        .map(|batch| batch.map(|batch| batch.num_rows()))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(rows, vec![1, 1, 1]);
    Ok(())
}
