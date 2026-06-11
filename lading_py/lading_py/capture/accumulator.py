"""
Periodically snapshots the registry and emits Lines to configured writer(s).
Counters are differenced (delta per tick); gauges and histograms pass through.
"""
import asyncio
import time
from lading_py.capture.line import Line, MetricKind
from lading_py.telemetry.registry import Registry


def _parse_key(k: tuple) -> tuple[str, dict]:
    name, label_pairs = k
    return name, dict(label_pairs)


class Accumulator:
    def __init__(self, run_id: str, registry: Registry, writers: list, flush_seconds: int = 60):
        self._run_id = run_id
        self._registry = registry
        self._writers = writers
        self._flush_seconds = flush_seconds
        self._prev_counters: dict[tuple, int] = {}
        self._fetch_index = 0

    async def run(self, signals) -> None:
        while not signals.shutdown.is_set():
            await asyncio.sleep(self._flush_seconds)
            self._flush()
        # Final flush on shutdown
        self._flush()

    def _flush(self) -> None:
        now_ms = int(time.time() * 1000)
        counters, gauges, histograms = self._registry.snapshot()
        lines: list[Line] = []

        for k, total in counters.items():
            delta = total - self._prev_counters.get(k, 0)
            self._prev_counters[k] = total
            name, labels = _parse_key(k)
            lines.append(Line(
                run_id=self._run_id,
                time=now_ms,
                fetch_index=self._fetch_index,
                metric_name=name,
                metric_kind=MetricKind.Counter,
                value=float(delta),
                labels=labels,
            ))

        for k, val in gauges.items():
            name, labels = _parse_key(k)
            lines.append(Line(
                run_id=self._run_id,
                time=now_ms,
                fetch_index=self._fetch_index,
                metric_name=name,
                metric_kind=MetricKind.Gauge,
                value=float(val),
                labels=labels,
            ))

        for k, samples in histograms.items():
            if not samples:
                continue
            name, labels = _parse_key(k)
            mean = sum(samples) / len(samples)
            lines.append(Line(
                run_id=self._run_id,
                time=now_ms,
                fetch_index=self._fetch_index,
                metric_name=name,
                metric_kind=MetricKind.Histogram,
                value=mean,
                labels=labels,
            ))

        self._fetch_index += 1
        for writer in self._writers:
            writer.flush(lines)
