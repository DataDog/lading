"""
Passive Prometheus exporter. Syncs from Registry into prometheus_client
collectors and serves GET /metrics via aiohttp.
"""
import asyncio
from aiohttp import web
from prometheus_client import CollectorRegistry, Gauge, Counter, generate_latest, CONTENT_TYPE_LATEST
from lading_py.signal import Signals
from lading_py.telemetry.registry import Registry as LadingRegistry


class PrometheusExporter:
    def __init__(self, lading_registry: LadingRegistry, addr: str):
        host, port = addr.rsplit(":", 1)
        self._host = host
        self._port = int(port)
        self._lading_registry = lading_registry
        self._prom_registry = CollectorRegistry()
        self._counters: dict[str, Counter] = {}
        self._gauges: dict[str, Gauge] = {}

    def _sync(self) -> None:
        counters, gauges, _ = self._lading_registry.snapshot()
        for (name, label_pairs), value in gauges.items():
            safe = name.replace(".", "_").replace("/", "_")
            label_keys = [k for k, _ in label_pairs]
            label_vals = [v for _, v in label_pairs]
            if safe not in self._gauges:
                self._gauges[safe] = Gauge(safe, safe, label_keys, registry=self._prom_registry)
            try:
                self._gauges[safe].labels(*label_vals).set(value)
            except Exception:
                pass

    async def _metrics_handler(self, request: web.Request) -> web.Response:
        self._sync()
        output = generate_latest(self._prom_registry)
        return web.Response(body=output, content_type=CONTENT_TYPE_LATEST)

    async def run(self, signals: Signals) -> None:
        app = web.Application()
        app.router.add_get("/metrics", self._metrics_handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, self._host, self._port)
        await site.start()
        await signals.shutdown.wait()
        await runner.cleanup()
