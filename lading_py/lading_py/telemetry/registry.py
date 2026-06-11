"""Thread-safe metric registry. Counters, gauges, histograms."""
import threading
from collections import defaultdict


def _key(name: str, labels: dict) -> tuple:
    return (name, tuple(sorted(labels.items())))


class Registry:
    def __init__(self):
        self._lock = threading.Lock()
        self._counters: dict[tuple, int] = defaultdict(int)
        self._gauges: dict[tuple, float] = {}
        self._histograms: dict[tuple, list[float]] = defaultdict(list)

    def increment(self, name: str, value: int = 1, labels: dict | None = None):
        k = _key(name, labels or {})
        with self._lock:
            self._counters[k] += value

    def set_gauge(self, name: str, value: float, labels: dict | None = None):
        k = _key(name, labels or {})
        with self._lock:
            self._gauges[k] = value

    def record_histogram(self, name: str, value: float, labels: dict | None = None):
        k = _key(name, labels or {})
        with self._lock:
            self._histograms[k].append(value)

    def snapshot(self) -> tuple[dict, dict, dict]:
        """Returns (counters, gauges, histograms). Drains histogram samples."""
        with self._lock:
            counters = dict(self._counters)
            gauges = dict(self._gauges)
            histograms = {k: list(v) for k, v in self._histograms.items()}
            self._histograms.clear()
        return counters, gauges, histograms
