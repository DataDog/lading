"""Scrapes a Prometheus text-format endpoint and records metrics in the registry."""
import asyncio
import re
import aiohttp
from lading_py.config import PrometheusTargetConfig
from lading_py.signal import Signals
from lading_py.telemetry.registry import Registry

_LINE_RE = re.compile(
    r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?|[+-]?Inf|NaN)'
)
_LABEL_RE = re.compile(r'(\w+)="((?:[^"\\]|\\.)*)"')


def _parse_labels(labels_str: str) -> dict[str, str]:
    return {m.group(1): m.group(2) for m in _LABEL_RE.finditer(labels_str)}


def _parse_text(text: str) -> list[tuple[str, str, float, dict]]:
    """Returns list of (name, kind, value, labels)."""
    results = []
    kinds: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("# TYPE"):
            parts = line.split(None, 4)
            if len(parts) >= 4:
                kinds[parts[2]] = parts[3]
            continue
        if line.startswith("#"):
            continue
        m = _LINE_RE.match(line)
        if not m:
            continue
        name = m.group(1)
        labels = _parse_labels(m.group(2) or "")
        try:
            value = float(m.group(3))
        except ValueError:
            continue
        # Prometheus histogram/summary data lines have suffixes; look up base name
        kind = kinds.get(name, "")
        if not kind:
            for suffix in ("_bucket", "_sum", "_count", "_total", "_created"):
                if name.endswith(suffix):
                    kind = kinds.get(name[: -len(suffix)], "")
                    if kind:
                        break
        results.append((name, kind or "gauge", value, labels))
    return results


class PrometheusScraper:
    def __init__(self, cfg: PrometheusTargetConfig, registry: Registry, sample_period_secs: float):
        self._cfg = cfg
        self._registry = registry
        self._period = sample_period_secs

    async def run(self, signals: Signals) -> None:
        await signals.experiment_started.wait()
        async with aiohttp.ClientSession() as session:
            while not signals.shutdown.is_set():
                try:
                    async with session.get(self._cfg.uri, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        text = await resp.text()
                    metrics = _parse_text(text)
                    allowed = set(self._cfg.metrics) if self._cfg.metrics else None
                    for name, kind, value, labels in metrics:
                        if allowed and name not in allowed:
                            continue
                        merged = {**labels, **self._cfg.tags}
                        if kind == "counter":
                            self._registry.increment(name, int(value), merged)
                        elif kind == "histogram" or kind == "summary":
                            self._registry.record_histogram(name, value, merged)
                        else:
                            self._registry.set_gauge(name, value, merged)
                except Exception:
                    pass
                await asyncio.sleep(self._period)
