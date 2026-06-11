"""HTTP blackhole: accepts all requests, discards bodies, counts bytes received."""
import asyncio
from aiohttp import web
from lading_py.config import HttpBlackholeConfig
from lading_py.signal import Signals
from lading_py.telemetry.registry import Registry


class HttpBlackhole:
    def __init__(self, cfg: HttpBlackholeConfig, registry: Registry, bh_id: str = "blackhole"):
        self._cfg = cfg
        self._registry = registry
        self._labels = {"blackhole": bh_id}

    async def _handler(self, request: web.Request) -> web.Response:
        body = await request.read()
        self._registry.increment("blackhole.bytes_received", len(body), self._labels)
        self._registry.increment("blackhole.requests_received", 1, self._labels)
        return web.Response(status=200)

    async def run(self, signals: Signals) -> None:
        host, port = self._cfg.binding_addr.rsplit(":", 1)
        app = web.Application()
        app.router.add_route("*", "/{path_info:.*}", self._handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, host, int(port))
        await site.start()
        await signals.shutdown.wait()
        await runner.cleanup()
