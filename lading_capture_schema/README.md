# Lading capture schema

This crate provides schema and version definitions for Lading-generated Parquet captures. The format sections define the capture contract, and the Rust helpers section describes the available APIs.

Capture versions are independent of Lading releases, Rust crate versions, and Parquet writer properties.

## Schema versions

| Capture version | Definition | Frozen baseline |
| --- | --- | --- |
| `1.0` | V1 schema | [Capture](../lading_capture/tests/fixtures/v1.parquet) and [Arrow schema](../lading_capture/tests/fixtures/v1.arrow.json) |

## Version metadata

The writer records exactly one raw footer entry:

```text
capture.version = "1.0"
```

The entry belongs to `FileMetaData.key_value_metadata`. A finalized zero-row capture contains the schema and version entry.

Capture versions use `major.minor` syntax with unsigned 64-bit components. Components contain ASCII decimal digits. Only zero itself starts with `0`.

The shared parser returns the declared version when the key exists and `None` when it is absent. Invalid values and duplicate version keys produce metadata errors.

## Version semantics

- Major versions identify changes that need supported readers to change parsing code, configuration, or interpretation.
- Minor versions identify compatible additions that preserve existing fields and their meaning.
- The version stays stable when implementation, encoding, or ordinary data changes preserve the contract.

Top-level column names are case-sensitive identifiers. Readers select columns by name. Compatible additions may place ignorable columns anywhere while preserving mandatory fields.

## Schema

The table describes V1's Arrow representation. The writer stores the standard `ARROW:schema` hint alongside Parquet's native schema. See [Parquet file structure](https://parquet.apache.org/docs/file-format/), [logical types](https://parquet.apache.org/docs/file-format/types/logicaltypes/), and [Arrow/Parquet schema handling](https://docs.rs/parquet/57.3.0/parquet/arrow/) for these layers.

Every V1 file declares all columns below, including zero-row files. Nullable means that a row can contain a null cell. The column remains part of the file schema even when all its cells are null.

| Column | Arrow type | Nullable cells |
| --- | --- | --- |
| `run_id` | `Utf8` | No |
| `time` | `Timestamp(Millisecond, None)` | No |
| `fetch_index` | `UInt64` | No |
| `metric_name` | `Utf8` | No |
| `metric_kind` | `Utf8` | No |
| `value_int` | `UInt64` | Yes |
| `value_float` | `Float64` | Yes |
| `labels` | Map of `Utf8` key/value pairs | No |
| `value_histogram` | `Binary` | Yes |

Column names are unique. Schema checks match mandatory fields by name, type, and nullability.

The labels map uses a non-null `entries` struct with non-null `key` and `value` fields. The `key` field is first, and the `value` field is second. The Arrow map declaration uses `keys_sorted=false`.

Label keys are unique within a row. Label pairs are unordered, and an empty map is valid.

## Field interpretation

### Run and metric identity

`run_id` is a UUID string identifying the capture run. `metric_name` identifies the metric, and `metric_kind` is `counter`, `gauge`, or `histogram`.

### Time and logical ticks

`time` contains non-negative signed 64-bit epoch-millisecond values. Lading derives each value from its capture start time plus the logical tick multiplied by `1000`.

`fetch_index` is the logical one-second tick. Readers use `fetch_index` or `time` to order observations within a series. Tick values can repeat across series, and a series can have gaps.

### Scalar values

Exactly one scalar column has a value, and the other is null. Readers select the populated scalar column to get its value and storage type.

| Scalar storage | `value_int` cell | `value_float` cell |
| --- | --- | --- |
| Integer | Integer value | Null |
| Floating-point | Null | Floating-point value |

Both storage forms are valid for counters and gauges. `Float64` supports IEEE 754 values, including NaN, infinities, and signed zero.

### Histograms

A populated `value_histogram` cell contains serialized `Dogsketch` protobuf bytes. The capture manager writes a `0.0` floating-point placeholder in the scalar slot for histogram rows. Read histogram measurements from the sketch payload.

A null `value_histogram` cell represents an empty payload. Readers represent this absence as an empty byte vector.

## Reading captures

1. Read raw footer entries before converting metadata to a map, so duplicate version keys remain visible.
2. Use declared version metadata to select decoding behavior according to your consumer's support policy.
3. Check the schema and decode the columns and values you need.

Consumers choose their reader library, storage client, batching, projections, and failure handling. Implementations use the format definitions above to interpret capture data.

## Rust helpers

Use `lading_capture_schema` for shared version parsing, field names, and schema declaration checks.

| API | Purpose |
| --- | --- |
| `CaptureVersion` | Represent, parse, and format the major and minor components |
| `parse_capture_version(entries)` | Parse raw footer pairs and return the declared version, or `None` when absent |
| `capture_schema()`, `columns::*` | Get the current writer's Arrow schema and field names |
| `validate_schema(actual, expected)` | Check mandatory declarations while allowing extra top-level columns |

`CURRENT_CAPTURE_VERSION` identifies the current writer's format. Consumers maintain their own supported-version policy and validate measurement values according to their use case.

`lading_capture::formats::parquet::reader_builder(reader)` provides Lading's batch-reader setup. It checks declared majors before Arrow reconstruction, validates their schemas, and reuses footer metadata for decoding. Files without a version key follow the ordinary Parquet reading path.

## Verification

Tests compare output against the frozen native Parquet schema, Arrow schema snapshot, and expected logical records. Schema comparisons allow top-level reordering and compatible extra columns while preserving mandatory declarations and nested map roles.

The tests use current workspace dependencies. V1 baselines stay fixed, and each new major format gets its own definition and baseline.
