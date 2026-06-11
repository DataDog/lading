"""Linux /proc/{pid}/smaps_rollup sampler."""
import asyncio
import os
import re
from lading_py.config import ObserverConfig
from lading_py.signal import Signals
from lading_py.telemetry.registry import Registry

_KB_RE = re.compile(r"^(\w+):\s+(\d+)\s+kB$")


def _parse_smaps_rollup(pid: int) -> dict[str, int]:
    path = f"/proc/{pid}/smaps_rollup"
    result = {}
    try:
        with open(path) as f:
            for line in f:
                m = _KB_RE.match(line.strip())
                if m:
                    result[m.group(1)] = int(m.group(2)) * 1024  # bytes
    except OSError:
        pass
    return result


def _parse_smaps(pid: int) -> dict[str, int]:
    """Aggregate all mappings from /proc/{pid}/smaps."""
    path = f"/proc/{pid}/smaps"
    totals: dict[str, int] = {}
    try:
        with open(path) as f:
            for line in f:
                m = _KB_RE.match(line.strip())
                if m:
                    totals[m.group(1)] = totals.get(m.group(1), 0) + int(m.group(2)) * 1024
    except OSError:
        pass
    return totals


class ProcObserver:
    def __init__(self, cfg: ObserverConfig, registry: Registry, sample_period_secs: float):
        self._cfg = cfg
        self._registry = registry
        self._period = sample_period_secs

    async def run(self, signals: Signals, pid: int) -> None:
        await signals.experiment_started.wait()
        tick = 0
        labels = {"pid": str(pid)}
        while not signals.shutdown.is_set():
            if self._cfg.enable_smaps_rollup:
                for field, val in _parse_smaps_rollup(pid).items():
                    self._registry.set_gauge(f"smaps_rollup.{field}", float(val), labels)

            if self._cfg.enable_smaps and tick % 10 == 0:
                for field, val in _parse_smaps(pid).items():
                    self._registry.set_gauge(f"smaps.{field}", float(val), labels)

            tick += 1
            await asyncio.sleep(self._period)
