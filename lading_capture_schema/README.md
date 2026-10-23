# Loading Parquet captures

The [capture format contract](../docs/capture-format.md) defines the file layout and interpretation rules. It is authoritative for Rust and non-Rust consumers.

## Recommended Rust helpers

Use `lading_capture_schema` to share contract definitions instead of repeating version parsing, field names, and schema checks. This lightweight crate does not read files or need the capture manager.

- `parse_capture_version(entries)` parses raw footer metadata without choosing which versions your reader supports.
- `capture_schema()` and `columns::*` provide the known schema and top-level field names.
- `validate_schema(actual, expected)` checks mandatory field declarations and allows extra top-level columns. It does not validate measurement values.

Parse raw footer entries before converting them to a map, so duplicate version keys remain visible. Then select the versions and decoding behavior your consumer supports.

`CURRENT_CAPTURE_VERSION` identifies current writer output, not your reader's supported-version list.

## Reader choices

Use your own Parquet reader and I/O client to load metadata and stream the columns you need. Reuse footer metadata when possible, and select top-level columns by name.

If you already use `lading_capture`, its `formats::parquet::reader_builder(reader)` helper provides Lading's default reader policy. It reuses footer metadata, rejects unsupported majors, checks versioned schemas, and returns a Parquet batch-reader builder.

These helpers are optional. Any implementation that follows the format contract can read captures without using our crates. Consumers choose supported versions, storage clients, batching, projections, and failure handling.
