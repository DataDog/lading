"""Tests for the DogStatsD generator: dispatch to dogstatsd-py and rate limiter."""
import time
from unittest.mock import MagicMock, call, patch
from lading_py.generator.dogstatsd import TokenBucket, _send_block, _send_metric
from lading_py.payload.dogstatsd import MetricCall, EventCall, ServiceCheckCall


# ---------------------------------------------------------------------------
# TokenBucket
# ---------------------------------------------------------------------------

class TestTokenBucket:
    def test_acquires_immediately_when_tokens_available(self):
        tb = TokenBucket(rate=1_000_000)
        t0 = time.monotonic()
        tb.acquire(100)
        elapsed = time.monotonic() - t0
        assert elapsed < 0.05  # should be near-instant

    def test_throttles_when_rate_exceeded(self):
        rate = 500  # 500 bytes/sec
        tb = TokenBucket(rate=rate)
        tb.acquire(rate)  # drain the bucket
        t0 = time.monotonic()
        tb.acquire(250)   # should wait ~0.5s
        elapsed = time.monotonic() - t0
        assert elapsed >= 0.4, f"expected >= 0.4s, got {elapsed:.3f}s"
        assert elapsed < 2.0, "took too long"

    def test_multiple_acquires_accumulate(self):
        rate = 1000
        tb = TokenBucket(rate=rate)
        tb.acquire(rate)  # drain
        t0 = time.monotonic()
        tb.acquire(500)
        tb.acquire(500)
        elapsed = time.monotonic() - t0
        assert elapsed >= 0.9


# ---------------------------------------------------------------------------
# _send_block dispatch
# ---------------------------------------------------------------------------

def _make_client() -> MagicMock:
    client = MagicMock()
    buf = MagicMock()
    client.open_buffer.return_value.__enter__ = MagicMock(return_value=buf)
    client.open_buffer.return_value.__exit__ = MagicMock(return_value=False)
    return client


class TestSendMetric:
    def test_gauge(self):
        client = _make_client()
        m = MetricCall("my.gauge", 42.0, "gauge", ["env:prod"], 1.0)
        _send_metric(client, m)
        client.gauge.assert_called_once_with("my.gauge", 42.0, tags=["env:prod"], sample_rate=1.0)

    def test_count(self):
        client = _make_client()
        m = MetricCall("hits", 7.0, "count", [], None)
        _send_metric(client, m)
        client.increment.assert_called_once_with("hits", value=7, tags=[], sample_rate=1)

    def test_histogram(self):
        client = _make_client()
        m = MetricCall("h", 3.14, "histogram", [], 0.5)
        _send_metric(client, m)
        client.histogram.assert_called_once_with("h", 3.14, tags=[], sample_rate=0.5)

    def test_distribution(self):
        client = _make_client()
        m = MetricCall("d", 1.0, "distribution", ["a:b"], 1.0)
        _send_metric(client, m)
        client.distribution.assert_called_once_with("d", 1.0, tags=["a:b"], sample_rate=1.0)

    def test_timing(self):
        client = _make_client()
        m = MetricCall("t", 99.9, "timing", [], None)
        _send_metric(client, m)
        client.timing.assert_called_once_with("t", 99.9, tags=[], sample_rate=1)

    def test_set(self):
        client = _make_client()
        m = MetricCall("s", 42.0, "set", [], None)
        _send_metric(client, m)
        client.set.assert_called_once_with("s", 42, tags=[], sample_rate=1)

    def test_sample_rate_none_defaults_to_one(self):
        client = _make_client()
        m = MetricCall("g", 1.0, "gauge", [], None)
        _send_metric(client, m)
        _, kwargs = client.gauge.call_args
        assert kwargs["sample_rate"] == 1

    def test_sample_rate_passed_through(self):
        client = _make_client()
        m = MetricCall("g", 1.0, "gauge", [], 0.25)
        _send_metric(client, m)
        _, kwargs = client.gauge.call_args
        assert kwargs["sample_rate"] == 0.25


class TestSendBlock:
    def test_single_metric_call(self):
        client = _make_client()
        m = MetricCall("g", 1.0, "gauge", [], None)
        _send_block(client, m)
        client.gauge.assert_called_once()
        client.open_buffer.assert_not_called()

    def test_batch_uses_open_buffer(self):
        client = _make_client()
        batch = [
            MetricCall("a", 1.0, "gauge", [], None),
            MetricCall("b", 2.0, "gauge", [], None),
        ]
        _send_block(client, batch)
        client.open_buffer.assert_called_once()
        buf = client.open_buffer.return_value.__enter__.return_value
        assert buf.gauge.call_count == 2

    def test_batch_all_metrics_sent(self):
        client = _make_client()
        batch = [
            MetricCall("a", 1.0, "gauge", [], None),
            MetricCall("b", 2.0, "count", [], None),
            MetricCall("c", 3.0, "distribution", [], None),
        ]
        _send_block(client, batch)
        buf = client.open_buffer.return_value.__enter__.return_value
        assert buf.gauge.call_count == 1
        assert buf.increment.call_count == 1
        assert buf.distribution.call_count == 1

    def test_event_call(self):
        client = _make_client()
        e = EventCall("My Title", "Some text", ["env:prod"], "error", "normal")
        _send_block(client, e)
        client.event.assert_called_once_with(
            "My Title", "Some text",
            tags=["env:prod"],
            alert_type="error",
            priority="normal",
        )

    def test_event_no_alert_type(self):
        client = _make_client()
        e = EventCall("T", "B", [], None, None)
        _send_block(client, e)
        client.event.assert_called_once()

    def test_service_check_ok(self):
        client = _make_client()
        sc = ServiceCheckCall("check.name", 0, ["host:foo"], None)
        _send_block(client, sc)
        client.service_check.assert_called_once_with(
            "check.name", 0,
            tags=["host:foo"],
            message=None,
        )

    def test_service_check_with_message(self):
        client = _make_client()
        sc = ServiceCheckCall("check", 2, [], "something is broken")
        _send_block(client, sc)
        _, kwargs = client.service_check.call_args
        assert kwargs["message"] == "something is broken"

    def test_tags_passed_to_metric(self):
        client = _make_client()
        m = MetricCall("g", 1.0, "gauge", ["a:1", "b:2"], None)
        _send_block(client, m)
        _, kwargs = client.gauge.call_args
        assert kwargs["tags"] == ["a:1", "b:2"]
