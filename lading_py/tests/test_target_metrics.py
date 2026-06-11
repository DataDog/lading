"""Tests for Prometheus text parser and Expvar path resolver."""
import pytest
from lading_py.target_metrics.prometheus import _parse_text, _parse_labels
from lading_py.target_metrics.expvar import _resolve_path


# ---------------------------------------------------------------------------
# Prometheus text format parser
# ---------------------------------------------------------------------------

PROM_SAMPLE = """
# HELP http_requests_total Total HTTP requests
# TYPE http_requests_total counter
http_requests_total{method="GET",status="200"} 1234
http_requests_total{method="POST",status="500"} 5

# HELP cpu_usage CPU utilisation
# TYPE cpu_usage gauge
cpu_usage 0.73

# HELP memory_bytes Memory usage in bytes
# TYPE memory_bytes gauge
memory_bytes{host="web01"} 536870912

# TYPE some_histogram histogram
some_histogram_bucket{le="0.1"} 10
some_histogram_bucket{le="+Inf"} 100
some_histogram_sum 55.5
some_histogram_count 100
"""


class TestParseLabels:
    def test_empty(self):
        assert _parse_labels("") == {}

    def test_single(self):
        assert _parse_labels('{env="prod"}') == {"env": "prod"}

    def test_multiple(self):
        result = _parse_labels('{a="1",b="2",c="3"}')
        assert result == {"a": "1", "b": "2", "c": "3"}

    def test_spaces_ignored(self):
        result = _parse_labels('{method="GET", status="200"}')
        assert "method" in result
        assert "status" in result


class TestParseText:
    def _by_name(self, results, name):
        return [(k, v, l) for n, k, v, l in results if n == name]

    def test_counter_type(self):
        results = _parse_text(PROM_SAMPLE)
        names_kinds = {n: k for n, k, v, l in results}
        assert names_kinds.get("http_requests_total") == "counter"

    def test_gauge_type(self):
        results = _parse_text(PROM_SAMPLE)
        names_kinds = {n: k for n, k, v, l in results}
        assert names_kinds.get("cpu_usage") == "gauge"

    def test_counter_values(self):
        results = _parse_text(PROM_SAMPLE)
        req_results = [(v, l) for n, k, v, l in results if n == "http_requests_total"]
        assert (1234.0, {"method": "GET", "status": "200"}) in req_results
        assert (5.0, {"method": "POST", "status": "500"}) in req_results

    def test_gauge_no_labels(self):
        results = _parse_text(PROM_SAMPLE)
        cpu = [(v, l) for n, k, v, l in results if n == "cpu_usage"]
        assert len(cpu) == 1
        assert cpu[0][0] == pytest.approx(0.73)
        assert cpu[0][1] == {}

    def test_gauge_with_labels(self):
        results = _parse_text(PROM_SAMPLE)
        mem = [(v, l) for n, k, v, l in results if n == "memory_bytes"]
        assert len(mem) == 1
        assert mem[0][1] == {"host": "web01"}
        assert mem[0][0] == 536870912.0

    def test_histogram_type(self):
        results = _parse_text(PROM_SAMPLE)
        names_kinds = {n: k for n, k, v, l in results}
        assert names_kinds.get("some_histogram_bucket") == "histogram"

    def test_comments_skipped(self):
        results = _parse_text("# HELP foo bar\n# TYPE foo gauge\nfoo 1.0\n")
        assert len(results) == 1
        assert results[0][0] == "foo"

    def test_empty_input(self):
        assert _parse_text("") == []

    def test_unknown_type_defaults_to_gauge(self):
        results = _parse_text("unknown_metric 42.0\n")
        assert results[0][1] == "gauge"

    def test_inf_value(self):
        results = _parse_text("# TYPE b histogram\nb_bucket{le=\"+Inf\"} 100\n")
        vals = [v for _, _, v, _ in results]
        assert any(v == 100.0 for v in vals)

    def test_scientific_notation(self):
        results = _parse_text("# TYPE m gauge\nm 1.5e3\n")
        assert results[0][2] == pytest.approx(1500.0)

    def test_negative_value(self):
        results = _parse_text("# TYPE m gauge\nm -3.14\n")
        assert results[0][2] == pytest.approx(-3.14)

    def test_multiline_no_crash(self):
        lines = ["# TYPE requests counter"]
        lines += [f'requests{{path="/api/{i}"}} {i * 10}' for i in range(50)]
        results = _parse_text("\n".join(lines))
        assert len(results) == 50


# ---------------------------------------------------------------------------
# Expvar path resolver
# ---------------------------------------------------------------------------

class TestResolvePath:
    def _data(self):
        return {
            "cmdline": ["agent", "-config", "agent.yaml"],
            "uptime": 12345,
            "forwarder": {
                "Transactions": {
                    "Success": 99,
                    "Errors": 3,
                },
                "FileStorage": {
                    "FilesCount": 7,
                },
            },
        }

    def test_top_level_numeric(self):
        assert _resolve_path(self._data(), "/uptime") == 12345

    def test_nested_two_levels(self):
        assert _resolve_path(self._data(), "/forwarder/FileStorage/FilesCount") == 7

    def test_nested_three_levels(self):
        assert _resolve_path(self._data(), "/forwarder/Transactions/Success") == 99

    def test_missing_top_level(self):
        assert _resolve_path(self._data(), "/nonexistent") is None

    def test_missing_nested(self):
        assert _resolve_path(self._data(), "/forwarder/Transactions/Missing") is None

    def test_missing_mid_path(self):
        assert _resolve_path(self._data(), "/forwarder/NoSuchKey/Count") is None

    def test_returns_dict(self):
        result = _resolve_path(self._data(), "/forwarder/Transactions")
        assert isinstance(result, dict)
        assert result["Success"] == 99

    def test_leading_slash_handled(self):
        # Both with and without leading slash should work
        assert _resolve_path(self._data(), "/uptime") == 12345

    def test_empty_path(self):
        # Empty path should return the whole dict
        result = _resolve_path(self._data(), "/")
        assert result == self._data()

    def test_path_into_list_returns_none(self):
        # /cmdline points to a list; trying to go deeper should return None
        assert _resolve_path(self._data(), "/cmdline/0") is None
