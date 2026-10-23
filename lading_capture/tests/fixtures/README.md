# Frozen capture fixtures

These synthetic fixtures freeze the same nine-column data layout before and after capture version stamping. Neither file comes from a production archive.

| File | Raw `capture.version` | Source |
| --- | --- | --- |
| `pre_v1.parquet` | Absent | The unversioned `main` writer layout, captured before version stamping |
| `v1.parquet` | `1.0.0` | The initial V1 Lading writer |

Both files contain three counter rows, a nil run UUID, timestamps `1000`, `2000`, and `3000`, and values `10`, `20`, and `30`. Repeated `env=prod`/`service=api` labels surround an `env=staging` row.

Both use `Utf8` label keys and values, Parquet v2 writer properties, ZSTD level 3, and the standard embedded Arrow schema. The current reader and the frozen reference reader must read both files and preserve their records.

Keep both files frozen. Do not regenerate either file during tests or writer dependency updates.
