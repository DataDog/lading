import pyarrow as pa
import pyarrow.parquet as pq
from lading_py.capture.line import Line

SCHEMA = pa.schema([
    ("run_id", pa.string()),
    ("time", pa.int64()),
    ("fetch_index", pa.int64()),
    ("metric_name", pa.string()),
    ("metric_kind", pa.string()),
    ("value", pa.float64()),
    ("labels", pa.map_(pa.string(), pa.string())),
    ("value_histogram", pa.binary()),
])


class ParquetWriter:
    def __init__(self, path: str):
        self._path = path
        self._writer: pq.ParquetWriter | None = None

    def flush(self, lines: list[Line]) -> None:
        if not lines:
            return
        table = pa.table(
            {
                "run_id": [l.run_id for l in lines],
                "time": pa.array([l.time for l in lines], type=pa.int64()),
                "fetch_index": pa.array([l.fetch_index for l in lines], type=pa.int64()),
                "metric_name": [l.metric_name for l in lines],
                "metric_kind": [l.metric_kind for l in lines],
                "value": pa.array([l.value for l in lines], type=pa.float64()),
                "labels": pa.array(
                    [list(l.labels.items()) for l in lines],
                    type=pa.map_(pa.string(), pa.string()),
                ),
                "value_histogram": pa.array([l.value_histogram for l in lines], type=pa.binary()),
            },
            schema=SCHEMA,
        )
        if self._writer is None:
            self._writer = pq.ParquetWriter(self._path, SCHEMA)
        self._writer.write_table(table)

    def finalize(self) -> None:
        if self._writer:
            self._writer.close()
            self._writer = None
