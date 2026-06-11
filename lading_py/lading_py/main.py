"""
lading-py entry point.

Lifecycle:
  warmup → experiment_started → experiment → shutdown → drain
"""
import argparse
import asyncio
import signal
import sys
import uuid

import yaml

from lading_py.config import RootConfig, TelemetryConfig
from lading_py.signal import Signals
from lading_py.telemetry.registry import Registry
from lading_py.capture.accumulator import Accumulator
from lading_py.capture.jsonl_writer import JsonlWriter
from lading_py.capture.parquet_writer import ParquetWriter
from lading_py.generator.dogstatsd import DogStatsDGenerator
from lading_py.blackhole.http import HttpBlackhole
from lading_py.target_metrics.prometheus import PrometheusScraper
from lading_py.target_metrics.expvar import ExpvarPoller
from lading_py.observer.proc import ProcObserver
from lading_py.telemetry.prometheus_exporter import PrometheusExporter


def _build_writers(tel: TelemetryConfig | None) -> list:
    if tel is None or tel.output_path is None:
        return []
    path = tel.output_path
    fmt = tel.format
    if fmt == "parquet":
        return [ParquetWriter(path)]
    elif fmt == "multi":
        return [JsonlWriter(path + ".jsonl"), ParquetWriter(path + ".parquet")]
    else:
        return [JsonlWriter(path)]


async def inner_main(config: RootConfig) -> None:
    run_id = str(uuid.uuid4())
    signals = Signals()
    registry = Registry()

    loop = asyncio.get_running_loop()

    def _on_signal():
        signals.set_shutdown()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _on_signal)

    tasks: list[asyncio.Task] = []

    # Telemetry output
    writers = _build_writers(config.telemetry)
    if writers:
        acc = Accumulator(
            run_id=run_id,
            registry=registry,
            writers=writers,
            flush_seconds=(config.telemetry.flush_seconds if config.telemetry else 60),
        )
        tasks.append(asyncio.create_task(acc.run(signals), name="accumulator"))

    if config.telemetry and config.telemetry.prometheus_addr:
        exp = PrometheusExporter(registry, config.telemetry.prometheus_addr)
        tasks.append(asyncio.create_task(exp.run(signals), name="prometheus_exporter"))

    # Generators
    for i, gen_cfg in enumerate(config.generator):
        if gen_cfg.unix_datagram is None:
            continue
        gen = DogStatsDGenerator(gen_cfg.unix_datagram, registry)
        tasks.append(asyncio.create_task(gen.run(signals), name=f"generator_{i}"))

    # Blackholes
    for i, bh_cfg in enumerate(config.blackhole):
        if bh_cfg.http is None:
            continue
        bh = HttpBlackhole(bh_cfg.http, registry, bh_id=str(i))
        tasks.append(asyncio.create_task(bh.run(signals), name=f"blackhole_{i}"))

    # Target metrics
    period_secs = config.sample_period_milliseconds / 1000.0
    for tm in config.target_metrics:
        if tm.prometheus:
            scraper = PrometheusScraper(tm.prometheus, registry, period_secs)
            tasks.append(asyncio.create_task(scraper.run(signals), name="prom_scraper"))
        if tm.expvar:
            poller = ExpvarPoller(tm.expvar, registry, period_secs)
            tasks.append(asyncio.create_task(poller.run(signals), name="expvar_poller"))

    # Observer (target PID must be provided for proc observer to be useful)
    target_pid: int | None = None
    if config.observer and target_pid is not None:
        obs = ProcObserver(config.observer, registry, period_secs)
        tasks.append(asyncio.create_task(obs.run(signals, target_pid), name="observer"))

    # Lifecycle
    if config.warmup_duration_secs > 0:
        await asyncio.sleep(config.warmup_duration_secs)

    signals.experiment_started.set()

    await asyncio.sleep(config.experiment_duration_secs)

    signals.set_shutdown()

    await asyncio.gather(*tasks, return_exceptions=True)

    for writer in writers:
        writer.finalize()


def main() -> None:
    parser = argparse.ArgumentParser(description="lading-py: DogStatsD load generator")
    parser.add_argument("--config", required=True, help="Path to lading YAML config")
    args = parser.parse_args()

    with open(args.config) as f:
        raw = yaml.safe_load(f)

    config = RootConfig.model_validate(raw)
    asyncio.run(inner_main(config))


if __name__ == "__main__":
    main()
