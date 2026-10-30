# Lading capture schema

This crate provides schema and version definitions for Lading-generated Parquet captures.

Capture versions are independent of Lading releases, Rust crate versions, and Parquet writer properties.

## Schema versions

| Capture version | Definition | Frozen baseline                                                                                                           |
| --------------- | ---------- | ------------------------------------------------------------------------------------------------------------------------- |
| `1.0`           | V1 schema  | [Capture](../lading_capture/tests/fixtures/v1.parquet) and [Arrow schema](../lading_capture/tests/fixtures/v1.arrow.json) |
| `2.0`           | V2 schema  | [Capture](../lading_capture/tests/fixtures/v2.parquet) and [Arrow schema](../lading_capture/tests/fixtures/v2.arrow.json) |

## Version metadata

The writer records exactly one raw footer entry:

```text
capture.version = "2.0"
```

## Version semantics

Versions use `major.minor` syntax with unsigned 64-bit components.

- Major versions identify changes that need supported readers to change parsing code, configuration, or interpretation.
- Minor versions identify compatible additions that preserve existing fields and their meaning.
- The version stays stable when implementation, encoding, or ordinary data changes preserve the contract.

Major versions should be incremented when the schema is backwards incompatible.

## Schema

The table describes the Arrow representation of each major version. The writer stores the standard `ARROW:schema` hint alongside Parquet's native schema. See [Parquet file structure](https://parquet.apache.org/docs/file-format/), [logical types](https://parquet.apache.org/docs/file-format/types/logicaltypes/), and [Arrow/Parquet schema handling](https://docs.rs/parquet/60.0.0/parquet/arrow/) for these layers.

Every versioned file declares all columns below.

- Nullable means that a row can contain a null cell.
- The column remains part of the file schema even when all its cells are null.
- Column names are unique.

| Column            | V1 Arrow type                  | V2 Arrow type                                     | Nullable cells |
| ----------------- | ------------------------------ | ------------------------------------------------- | -------------- |
| `run_id`          | `Utf8`                         | `Utf8`                                            | No             |
| `time`            | `Timestamp(Millisecond, None)` | `Timestamp(Millisecond, None)`                    | No             |
| `fetch_index`     | `UInt64`                       | `UInt64`                                          | No             |
| `metric_name`     | `Utf8`                         | `Utf8`                                            | No             |
| `metric_kind`     | `Utf8`                         | `Utf8`                                            | No             |
| `value_int`       | `UInt64`                       | `UInt64`                                          | Yes            |
| `value_float`     | `Float64`                      | `Float64`                                         | Yes            |
| `labels`          | Map of `Utf8` key/value pairs  | Map of `Dictionary(Int32, Utf8)` key/value pairs  | No             |
| `value_histogram` | `Binary`                       | `Binary`                                          | Yes            |

## Field interpretation

### Run and metric identity

`run_id` is a UUID string identifying the capture run. `metric_name` identifies the metric, and `metric_kind` is `counter`, `gauge`, or `histogram`.

### Time and logical ticks

`time` contains non-negative signed 64-bit epoch-millisecond values. Lading derives each value from its capture start time plus the logical tick multiplied by `1000`.

`fetch_index` is the logical one-second tick. Readers use `fetch_index` or `time` to order observations within a series. Tick values can repeat across series, and a series can have gaps.

### Scalar values

Exactly one scalar column has a value, and the other is null. Readers select the populated scalar column to get its value and storage type.

| Scalar storage | `value_int` cell | `value_float` cell   |
| -------------- | ---------------- | -------------------- |
| Integer        | Integer value    | Null                 |
| Floating-point | Null             | Floating-point value |

Both storage forms are valid for counters and gauges. `Float64` supports IEEE 754 values, including NaN, infinities, and signed zero.

### Histograms

A populated `value_histogram` cell contains serialized `Dogsketch` protobuf bytes. The capture manager writes a `0.0` floating-point placeholder in the scalar slot for histogram rows. Read histogram measurements from the sketch payload.

A null `value_histogram` cell represents an empty payload.

## Reading captures

1. Read raw footer entries to read schema version.
2. Use declared version to select decoding behavior.
3. Decode the columns and values you need.

Lading's `captool` and `validate_parquet` read one schema major version. `captool --schema-major-version <MAJOR>` selects it, and the default is the current writer's major. `validate_parquet` takes it as its `schema_major_version` argument. These tools reject a file that declares a different major. For a file without `capture.version`, the selected major also selects the label decoding.

## Rust helpers

Use `lading_capture_schema` for shared version parsing, field names, and schema declaration checks.

| API                                 | Purpose                                                                       |
| ----------------------------------- | ----------------------------------------------------------------------------- |
| `CaptureVersion`                    | Represent, parse, and format the major and minor components                   |
| `parse_capture_version(entries)`    | Parse raw footer pairs and return the declared version, or `None` when absent |
| `capture_schema()`, `columns::*`    | Get the current writer's Arrow schema and field names                         |
| `capture_schema_for_major(major)`   | Get the Arrow schema of one major version, or `None` when it is not defined   |
| `LabelEncoding::for_major(major)`   | Get the label key and value encoding of one major version, or `None`          |
| `validate_schema(actual, expected)` | Check mandatory declarations while allowing extra top-level columns           |

`CURRENT_CAPTURE_VERSION` identifies the current writer's format.

## Verification

Tests compare output against the frozen native Parquet schema, Arrow schema snapshot, and expected logical records. Schema comparisons allow top-level reordering and compatible extra columns while preserving mandatory declarations and nested map roles.

The V1 and V2 baselines stay fixed, and each new major format gets its own definition and baseline.
