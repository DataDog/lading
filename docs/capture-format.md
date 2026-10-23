# Parquet capture format

This document defines capture format `1.0.0` for Lading-generated Parquet files. The capture version is independent of the Lading release, Rust crate versions, and Parquet writer properties.

## Version metadata

Every new valid capture contains one raw Parquet footer key/value entry:

```text
capture.version = "1.0.0"
```

The entry belongs to `FileMetaData.key_value_metadata`. It is not a data column, row-group field, or value inside `ARROW:schema`. A finalized zero-row capture still contains the schema and this entry.

Versions use canonical SemVer syntax. Patch must be zero, and prerelease/build suffixes are not allowed. For example, `1.0.0` and `99.12.0` are valid capture-version strings, while `1.0`, `01.0.0`, `1.0.1`, and `1.0.0+build` are not.

Version components are unsigned 64-bit integers. Valid unknown versions remain parseable.

An absent key means unversioned. A present key without a value, an invalid value, or duplicate `capture.version` entries means invalid metadata. Identical duplicate values are still invalid.

## Compatibility

- Increment major when output needs supported readers to change parsing code, configuration, or interpretation.
- Increment minor for compatible contract additions that unchanged readers can process correctly.
- Leave the version unchanged when producer implementation or ordinary data changes do not change the contract.
- Do not publish patch-level format revisions.

Reordering columns does not change the version. Column identity is case-sensitive and name-based, not positional.

Later `1.x` versions retain the mandatory fields' names, types, nullability, and meaning. They may add ignorable top-level columns in any position only when unchanged-reader compatibility tests pass.

Physical Parquet schema equality does not prove compatibility. For example, changing default Arrow output from `Utf8` labels to `Dictionary(Int32, Utf8)` breaks readers that expect `StringArray`. A reader-options workaround needs a consumer change and does not establish backwards compatibility.

Consumers own their support and admission policy. A valid unsupported major is not malformed metadata or proof of corruption.

## Schema

The table describes the default Arrow representation restored by the Parquet reader. Physical Parquet encodings can differ without changing these named data fields. The current writer also stores the standard `ARROW:schema` hint.

Column names are unique. Column order and unrelated schema/field metadata do not define compatibility.

| Column | Arrow type | Nullable values |
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

Nullable cells do not make a mandatory column optional.

The labels map uses a non-null `entries` struct with non-null `key` and `value` fields. The `key` field is first, and the `value` field is second. The Arrow map declaration uses `keys_sorted=false`.

Label keys are unique within a row, and their order has no meaning. An empty map is valid.

## Field interpretation

### Run and metric identity

`run_id` is a UUID string identifying the capture run. `metric_name` is the metric name. `metric_kind` is `counter`, `gauge`, or `histogram`.

Metric names and label strings are data. New names or values do not change the format when they follow this contract.

### Time and logical ticks

`time` contains epoch-millisecond values. Lading derives each value from its capture start time plus the row's logical tick multiplied by `1000`. It is not the wall-clock instant at which the row reaches disk.

Valid capture timestamps are non-negative signed 64-bit millisecond values.

The Arrow timestamp has no timezone annotation. Interpret the stored values using the epoch-millisecond meaning above, not a local timezone conversion.

`fetch_index` is that logical one-second tick. It is not a row number, record-batch index, or count of Parquet flushes.

Physical row order does not define time order. Use `fetch_index` or `time` within a series rather than file or batch position. Tick values can repeat across different series, and a series can have gaps.

### Scalar values

Exactly one scalar column has a value: `value_int` for integers or `value_float` for floating-point values. The other scalar column is null.

Counter and gauge rows can use either scalar storage type. Do not infer storage type from `metric_kind`.

`Float64` retains its floating-point representation. This format does not add a finite-only constraint to scalar values or change existing value decoding.

### Histograms

When present, `value_histogram` contains serialized `Dogsketch` protobuf bytes. The capture manager writes a `0.0` floating-point placeholder in the scalar slot for histogram rows. That placeholder is not the histogram measurement.

Empty histogram payload bytes become null in the `Binary` column. Existing readers represent this absence as an empty byte vector. Do not reinterpret that absence as a scalar zero measurement.

## Unversioned captures

Unversioned captures keep the existing reading path without stricter new schema checks. Missing metadata never silently becomes version `1.0.0` or the latest version. A malformed present version must not use the unversioned fallback.

## Verification

Same-major producer changes must pass an unchanged-reader compatibility test. Keep that reader's dependencies, field expectations, and settings independent of the writer under test. Passing after adapting the reader is migration evidence, not backwards-compatibility evidence.

Frozen fixtures check current readers against unversioned and V1 baselines. Tests do not replace semantic review.
