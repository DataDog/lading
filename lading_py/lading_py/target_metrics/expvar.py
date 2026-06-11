"""Polls a Go expvar /debug/vars JSON endpoint and records values in the registry."""
import asyncio
import aiohttp
from lading_py.config import ExpvarTargetConfig
from lading_py.signal import Signals
from lading_py.telemetry.registry import Registry


def _resolve_path(data: dict, path: str):
    """Navigate '/foo/bar/baz' → data['foo']['bar']['baz']. Returns None if missing."""
    parts = [p for p in path.split("/") if p]
    node = data
    for part in parts:
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


class ExpvarPoller:
    def __init__(self, cfg: ExpvarTargetConfig, registry: Registry, sample_period_secs: float):
        self._cfg = cfg
        self._registry = registry
        self._period = sample_period_secs

    async def run(self, signals: Signals) -> None:
        await signals.experiment_started.wait()
        async with aiohttp.ClientSession() as session:
            while not signals.shutdown.is_set():
                try:
                    async with session.get(self._cfg.uri, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        data = await resp.json(content_type=None)
                    for var_path in self._cfg.vars:
                        value = _resolve_path(data, var_path)
                        if value is None:
                            continue
                        # Flatten non-numeric nested dicts by path extension
                        if isinstance(value, dict):
                            for k, v in value.items():
                                if isinstance(v, (int, float)):
                                    self._registry.set_gauge(
                                        f"{var_path}/{k}".lstrip("/"),
                                        float(v),
                                        self._cfg.tags,
                                    )
                        elif isinstance(value, (int, float)):
                            self._registry.set_gauge(
                                var_path.lstrip("/"),
                                float(value),
                                self._cfg.tags,
                            )
                except Exception:
                    pass
                await asyncio.sleep(self._period)
