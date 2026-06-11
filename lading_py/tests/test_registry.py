"""Tests for the thread-safe metric registry."""
import threading
from lading_py.telemetry.registry import Registry, _key


class TestKey:
    def test_same_labels_same_key(self):
        k1 = _key("metric", {"a": "1", "b": "2"})
        k2 = _key("metric", {"b": "2", "a": "1"})  # different insertion order
        assert k1 == k2

    def test_different_names_differ(self):
        assert _key("a", {}) != _key("b", {})

    def test_different_labels_differ(self):
        assert _key("m", {"a": "1"}) != _key("m", {"a": "2"})


class TestRegistryCounters:
    def test_increment_basic(self):
        r = Registry()
        r.increment("hits", 5)
        counters, _, _ = r.snapshot()
        assert counters[_key("hits", {})] == 5

    def test_increment_accumulates(self):
        r = Registry()
        r.increment("hits", 3)
        r.increment("hits", 7)
        counters, _, _ = r.snapshot()
        assert counters[_key("hits", {})] == 10

    def test_increment_default_value(self):
        r = Registry()
        r.increment("x")
        counters, _, _ = r.snapshot()
        assert counters[_key("x", {})] == 1

    def test_increment_with_labels(self):
        r = Registry()
        r.increment("req", 1, {"status": "200"})
        r.increment("req", 2, {"status": "500"})
        counters, _, _ = r.snapshot()
        assert counters[_key("req", {"status": "200"})] == 1
        assert counters[_key("req", {"status": "500"})] == 2

    def test_counters_persist_across_snapshots(self):
        r = Registry()
        r.increment("x", 10)
        r.snapshot()
        r.increment("x", 5)
        counters, _, _ = r.snapshot()
        assert counters[_key("x", {})] == 15  # cumulative total

    def test_thread_safety(self):
        r = Registry()
        N = 1000
        threads = [
            threading.Thread(target=lambda: r.increment("counter", 1))
            for _ in range(N)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        counters, _, _ = r.snapshot()
        assert counters[_key("counter", {})] == N


class TestRegistryGauges:
    def test_set_gauge(self):
        r = Registry()
        r.set_gauge("cpu", 42.5)
        _, gauges, _ = r.snapshot()
        assert gauges[_key("cpu", {})] == 42.5

    def test_gauge_overwritten(self):
        r = Registry()
        r.set_gauge("mem", 100.0)
        r.set_gauge("mem", 200.0)
        _, gauges, _ = r.snapshot()
        assert gauges[_key("mem", {})] == 200.0

    def test_gauge_persists_across_snapshots(self):
        r = Registry()
        r.set_gauge("g", 5.0)
        r.snapshot()
        _, gauges, _ = r.snapshot()
        assert gauges[_key("g", {})] == 5.0

    def test_gauge_with_labels(self):
        r = Registry()
        r.set_gauge("temp", 37.0, {"zone": "a"})
        r.set_gauge("temp", 22.0, {"zone": "b"})
        _, gauges, _ = r.snapshot()
        assert gauges[_key("temp", {"zone": "a"})] == 37.0
        assert gauges[_key("temp", {"zone": "b"})] == 22.0


class TestRegistryHistograms:
    def test_record_histogram(self):
        r = Registry()
        r.record_histogram("latency", 12.5)
        _, _, histograms = r.snapshot()
        assert histograms[_key("latency", {})] == [12.5]

    def test_histogram_drained_on_snapshot(self):
        r = Registry()
        r.record_histogram("h", 1.0)
        r.snapshot()
        _, _, histograms = r.snapshot()
        assert _key("h", {}) not in histograms or histograms[_key("h", {})] == []

    def test_multiple_samples_collected(self):
        r = Registry()
        for v in [1.0, 2.0, 3.0]:
            r.record_histogram("h", v)
        _, _, histograms = r.snapshot()
        assert sorted(histograms[_key("h", {})]) == [1.0, 2.0, 3.0]

    def test_histogram_thread_safety(self):
        r = Registry()
        N = 500
        threads = [
            threading.Thread(target=lambda: r.record_histogram("h", 1.0))
            for _ in range(N)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        _, _, histograms = r.snapshot()
        assert len(histograms[_key("h", {})]) == N
