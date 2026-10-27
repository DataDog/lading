# Frozen capture contracts

`v1.parquet` captures the `1.0` writer's schema and three synthetic counter records with values `10`, `20`, and `30`. Its raw footer declares `capture.version = "1.0"`.

`v2.parquet` captures the `2.0` writer's schema and the same three records. Its raw footer declares `capture.version = "2.0"`.

`v1.arrow.json` and `v2.arrow.json` record the Arrow field declarations extracted from those accepted captures using Arrow's schema serialization.

Treat all baseline artifacts as fixed. Each new major format gets a separate baseline and explicit test coverage.

See the [format contract](../../../lading_capture_schema/README.md) for the declared versions and field rules.

Tests compare named native fields, frozen Arrow declarations, and expected logical-record values.
