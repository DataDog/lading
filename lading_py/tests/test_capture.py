"""Tests for capture output: accumulator, JSONL writer, Parquet writer."""
import json
import os
import tempfile
import time
import pytest
import pyarrow.parquet as pq

from lading_py.capture.line import Line, MetricKind
from lading_py.capture.jsonl_writer import JsonlWriter
from lading_py.capture.parquet_writer import ParquetWriter
from lading_py.capture.accumulator import Accumulator, _parse_key
from lading_py.telemetry.registry import Registry, _key


# ---------------------------------------------------------------------------
# Line serialization
# ---------------------------------------------------------------------------

class TestLine:
    def _make_line(self, **kwargs) -> Line:
        defaults = dict(
            run_id="test-run-id",
            time=1_700_000_000_000,
            fetch_index=0,
            metric_name="bytes_written",
            metric_kind=MetricKind.Counter,
            value=1024.0,
            labels={"generator": "dogstatsd"},
        )
        defaults.update(kwargs)
        return Line(**defaults)

    def test_to_dict_fields(self):
        line = self._make_line()
        d = line.to_dict()
        assert d["run_id"] == "test-run-id"
        assert d["time"] == 1_700_000_000_000
        assert d["fetch_index"] == 0
        assert d["metric_name"] == "bytes_written"
        assert d["metric_kind"] == MetricKind.Counter
        assert d["value"] == 1024.0
        assert d["labels"] == {"generator": "dogstatsd"}

    def test_to_dict_no_histogram_key_when_empty(self):
        line = self._make_line()
        d = line.to_dict()
        assert "value_histogram" not in d

    def test_to_dict_base64_histogram(self):
        import base64
        line = self._make_line(value_histogram=b"\x01\x02\x03")
        d = line.to_dict()
        assert d["value_histogram"] == base64.b64encode(b"\x01\x02\x03").decode()


# ---------------------------------------------------------------------------
# JSONL writer
# ---------------------------------------------------------------------------

class TestJsonlWriter:
    def _make_lines(self, n: int) -> list[Line]:
        return [
            Line(
                run_id="r",
                time=1_000_000 + i,
                fetch_index=i,
                metric_name=f"metric.{i}",
                metric_kind=MetricKind.Gauge,
                value=float(i),
                labels={"idx": str(i)},
            )
            for i in range(n)
        ]

    def test_writes_and_reads_back(self, tmp_path):
        path = str(tmp_path / "out.jsonl")
        writer = JsonlWriter(path)
        lines = self._make_lines(5)
        writer.flush(lines)
        with open(path) as f:
            rows = [json.loads(l) for l in f if l.strip()]
        assert len(rows) == 5
        assert rows[0]["metric_name"] == "metric.0"
        assert rows[4]["value"] == 4.0

    def test_multiple_flushes_append(self, tmp_path):
        path = str(tmp_path / "out.jsonl")
        writer = JsonlWriter(path)
        writer.flush(self._make_lines(3))
        writer.flush(self._make_lines(2))
        with open(path) as f:
            rows = [l for l in f if l.strip()]
        assert len(rows) == 5

    def test_empty_flush_noop(self, tmp_path):
        path = str(tmp_path / "out.jsonl")
        writer = JsonlWriter(path)
        writer.flush([])
        assert os.path.getsize(path) == 0

    def test_valid_json_per_line(self, tmp_path):
        path = str(tmp_path / "out.jsonl")
        writer = JsonlWriter(path)
        writer.flush(self._make_lines(10))
        with open(path) as f:
            for line in f:
                if line.strip():
                    json.loads(line)  # should not raise

    def test_overwrites_on_new_writer(self, tmp_path):
        path = str(tmp_path / "out.jsonl")
        JsonlWriter(path).flush(self._make_lines(5))
        JsonlWriter(path).flush(self._make_lines(2))
        with open(path) as f:
            rows = [l for l in f if l.strip()]
        assert len(rows) == 2

    def test_label_roundtrip(self, tmp_path):
        path = str(tmp_path / "out.jsonl")
        writer = JsonlWriter(path)
        line = Line("r", 0, 0, "m", MetricKind.Gauge, 1.0, {"a": "x", "b": "y"})
        writer.flush([line])
        with open(path) as f:
            row = json.loads(f.read())
        assert row["labels"] == {"a": "x", "b": "y"}


# ---------------------------------------------------------------------------
# Parquet writer
# ---------------------------------------------------------------------------

class TestParquetWriter:
    def _make_lines(self, n: int) -> list[Line]:
        return [
            Line(
                run_id="run1",
                time=1_000 + i,
                fetch_index=i,
                metric_name="test.metric",
                metric_kind=MetricKind.Counter,
                value=float(i * 10),
                labels={"env": "test"},
            )
            for i in range(n)
        ]

    def test_writes_parquet_file(self, tmp_path):
        path = str(tmp_path / "out.parquet")
        writer = ParquetWriter(path)
        writer.flush(self._make_lines(5))
        writer.finalize()
        assert os.path.exists(path)
        table = pq.read_table(path)
        assert table.num_rows == 5

    def test_schema_columns_present(self, tmp_path):
        path = str(tmp_path / "out.parquet")
        writer = ParquetWriter(path)
        writer.flush(self._make_lines(1))
        writer.finalize()
        table = pq.read_table(path)
        for col in ("run_id", "time", "fetch_index", "metric_name", "metric_kind", "value"):
            assert col in table.schema.names

    def test_values_correct(self, tmp_path):
        path = str(tmp_path / "out.parquet")
        writer = ParquetWriter(path)
        writer.flush(self._make_lines(3))
        writer.finalize()
        table = pq.read_table(path)
        assert table["value"].to_pylist() == [0.0, 10.0, 20.0]
        assert all(r == "run1" for r in table["run_id"].to_pylist())

    def test_multiple_flushes_appended(self, tmp_path):
        path = str(tmp_path / "out.parquet")
        writer = ParquetWriter(path)
        writer.flush(self._make_lines(3))
        writer.flush(self._make_lines(2))
        writer.finalize()
        table = pq.read_table(path)
        assert table.num_rows == 5

    def test_empty_flush_noop(self, tmp_path):
        path = str(tmp_path / "out.parquet")
        writer = ParquetWriter(path)
        writer.flush([])
        writer.finalize()
        assert not os.path.exists(path)


# ---------------------------------------------------------------------------
# Accumulator
# ---------------------------------------------------------------------------

class TestParseKey:
    def test_simple(self):
        name, labels = _parse_key(("metric", (("a", "1"),)))
        assert name == "metric"
        assert labels == {"a": "1"}

    def test_empty_labels(self):
        name, labels = _parse_key(("m", ()))
        assert name == "m"
        assert labels == {}


class TestAccumulatorFlush:
    def _acc(self, registry: Registry, writers: list) -> Accumulator:
        return Accumulator("run-1", registry, writers, flush_seconds=3600)

    def test_counter_delta_first_flush(self):
        registry = Registry()
        registry.increment("bytes", 100)
        lines = []
        acc = self._acc(registry, [_ListWriter(lines)])
        acc._flush()
        counter_lines = [l for l in lines if l.metric_kind == MetricKind.Counter]
        assert any(l.metric_name == "bytes" and l.value == 100.0 for l in counter_lines)

    def test_counter_delta_second_flush(self):
        registry = Registry()
        registry.increment("bytes", 100)
        lines = []
        acc = self._acc(registry, [_ListWriter(lines)])
        acc._flush()
        lines.clear()
        registry.increment("bytes", 50)
        acc._flush()
        counter_lines = [l for l in lines if l.metric_name == "bytes"]
        assert any(l.value == 50.0 for l in counter_lines)

    def test_counter_delta_zero_if_no_new_increments(self):
        registry = Registry()
        registry.increment("x", 10)
        lines = []
        acc = self._acc(registry, [_ListWriter(lines)])
        acc._flush()
        lines.clear()
        acc._flush()
        counter_lines = [l for l in lines if l.metric_name == "x"]
        assert any(l.value == 0.0 for l in counter_lines)

    def test_gauge_passthrough(self):
        registry = Registry()
        registry.set_gauge("cpu", 55.5)
        lines = []
        acc = self._acc(registry, [_ListWriter(lines)])
        acc._flush()
        gauge_lines = [l for l in lines if l.metric_kind == MetricKind.Gauge]
        assert any(l.metric_name == "cpu" and l.value == 55.5 for l in gauge_lines)

    def test_histogram_mean(self):
        registry = Registry()
        for v in [10.0, 20.0, 30.0]:
            registry.record_histogram("latency", v)
        lines = []
        acc = self._acc(registry, [_ListWriter(lines)])
        acc._flush()
        hist_lines = [l for l in lines if l.metric_kind == MetricKind.Histogram]
        assert any(l.metric_name == "latency" and l.value == 20.0 for l in hist_lines)

    def test_histogram_drained_after_flush(self):
        registry = Registry()
        registry.record_histogram("h", 5.0)
        lines = []
        acc = self._acc(registry, [_ListWriter(lines)])
        acc._flush()
        lines.clear()
        acc._flush()
        hist_lines = [l for l in lines if l.metric_kind == MetricKind.Histogram]
        assert hist_lines == []

    def test_fetch_index_increments(self):
        registry = Registry()
        registry.set_gauge("g", 1.0)
        lines = []
        acc = self._acc(registry, [_ListWriter(lines)])
        acc._flush()
        idx0 = lines[0].fetch_index
        lines.clear()
        registry.set_gauge("g", 2.0)
        acc._flush()
        idx1 = lines[0].fetch_index
        assert idx1 == idx0 + 1

    def test_labels_preserved(self):
        registry = Registry()
        registry.set_gauge("g", 1.0, {"env": "prod"})
        lines = []
        acc = self._acc(registry, [_ListWriter(lines)])
        acc._flush()
        assert any(l.labels == {"env": "prod"} for l in lines)

    def test_run_id_in_lines(self):
        registry = Registry()
        registry.set_gauge("g", 1.0)
        lines = []
        acc = Accumulator("my-run-id", registry, [_ListWriter(lines)])
        acc._flush()
        assert all(l.run_id == "my-run-id" for l in lines)

    def test_multiple_writers_both_receive_lines(self):
        registry = Registry()
        registry.set_gauge("g", 1.0)
        lines1, lines2 = [], []
        acc = self._acc(registry, [_ListWriter(lines1), _ListWriter(lines2)])
        acc._flush()
        assert len(lines1) == len(lines2) == 1


class _ListWriter:
    """Test double that collects flushed Lines in a list."""
    def __init__(self, lines: list):
        self._lines = lines

    def flush(self, lines: list[Line]) -> None:
        self._lines.extend(lines)

    def finalize(self) -> None:
        pass
