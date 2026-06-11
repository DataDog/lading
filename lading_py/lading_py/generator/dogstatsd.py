"""
DogStatsD generator. All emission goes through dogstatsd-py (DogStatsd client).
Multi-value batches use client.open_buffer() so dogstatsd-py packs them into
one datagram internally.
"""
import asyncio
import time
import threading
from dataclasses import dataclass

from datadog.dogstatsd.base import DogStatsd

from lading_py.config import UnixDatagramConfig
from lading_py.payload.dogstatsd import (
    Block, BlockCache, MetricCall, EventCall, ServiceCheckCall,
    _estimate_block_bytes,
)
from lading_py.signal import Signals
from lading_py.telemetry.registry import Registry


# ---------------------------------------------------------------------------
# Dispatch table: metric_type → DogStatsd method
# ---------------------------------------------------------------------------

def _send_metric(client: DogStatsd, m: MetricCall) -> None:
    sr = m.sample_rate if m.sample_rate is not None else 1
    if m.metric_type == "gauge":
        client.gauge(m.name, m.value, tags=m.tags, sample_rate=sr)
    elif m.metric_type == "count":
        client.increment(m.name, value=int(m.value), tags=m.tags, sample_rate=sr)
    elif m.metric_type == "histogram":
        client.histogram(m.name, m.value, tags=m.tags, sample_rate=sr)
    elif m.metric_type == "distribution":
        client.distribution(m.name, m.value, tags=m.tags, sample_rate=sr)
    elif m.metric_type == "timing":
        client.timing(m.name, m.value, tags=m.tags, sample_rate=sr)
    elif m.metric_type == "set":
        client.set(m.name, int(m.value), tags=m.tags, sample_rate=sr)


def _send_block(client: DogStatsd, block: Block) -> None:
    if isinstance(block, list):
        with client.open_buffer() as buf:
            for m in block:
                _send_metric(buf, m)
    elif isinstance(block, MetricCall):
        _send_metric(client, block)
    elif isinstance(block, EventCall):
        client.event(
            block.title, block.text,
            tags=block.tags,
            alert_type=block.alert_type,
            priority=block.priority,
        )
    elif isinstance(block, ServiceCheckCall):
        client.service_check(
            block.name, block.status,
            tags=block.tags,
            message=block.message,
        )


# ---------------------------------------------------------------------------
# Token bucket (synchronous, for use in worker threads)
# ---------------------------------------------------------------------------

class TokenBucket:
    def __init__(self, rate: int):
        self._rate = rate
        self._tokens = float(rate)
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self, n: int) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                elapsed = now - self._last
                self._tokens = min(self._rate, self._tokens + elapsed * self._rate)
                self._last = now
                if self._tokens >= n:
                    self._tokens -= n
                    return
                wait = (n - self._tokens) / self._rate
            time.sleep(wait)


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------

class DogStatsDGenerator:
    def __init__(
        self,
        cfg: UnixDatagramConfig,
        registry: Registry,
    ):
        self._cfg = cfg
        self._registry = registry
        dsd_cfg = cfg.dogstatsd
        # Pre-build block cache; cap count at 20k regardless of prebuild size config
        self._cache = BlockCache(dsd_cfg, cfg.seed, max_count=20_000)
        self._rate_limiter = TokenBucket(cfg.bytes_per_second_int)
        self._gen_id = {"generator": "dogstatsd"}

    async def run(self, signals: Signals) -> None:
        await signals.experiment_started.wait()

        async def _wrap(i: int):
            await asyncio.to_thread(self._send_loop, signals)

        await asyncio.gather(*[_wrap(i) for i in range(self._cfg.parallel_connections)])

    def _send_loop(self, signals: Signals) -> None:
        client = DogStatsd(socket_path=self._cfg.path)
        while not signals.shutdown_is_set():
            block = self._cache.next()
            est = _estimate_block_bytes(block)
            self._rate_limiter.acquire(est)
            try:
                _send_block(client, block)
                self._registry.increment("bytes_written", est, self._gen_id)
                self._registry.increment("packets_sent", 1, self._gen_id)
            except Exception as exc:
                self._registry.increment(
                    "request_failure", 1,
                    {**self._gen_id, "error": type(exc).__name__},
                )
