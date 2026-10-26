# Frozen V1 contract

`v1.parquet` captures the initial `1.0.0` writer's schema and three synthetic counter records with values `10`, `20`, and `30`. Its raw footer declares `capture.version = "1.0.0"`.

`v1.arrow.json` records the Arrow field declarations extracted from that accepted capture using Arrow's schema serialization.

Tests compare named native fields, frozen Arrow declarations, and expected logical-record values. The in-process helper uses current workspace dependencies and keeps its V1 expectations independent of producer constants.

Treat both baseline artifacts as fixed. Each new major format gets a separate baseline and explicit test coverage.

See the [format contract](../../../lading_capture_schema/README.md) for the declared versions and field rules.
