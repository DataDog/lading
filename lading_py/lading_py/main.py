"""
lading-py entry point.

CLI is compatible with Rust lading:
  lading-py [--config-path PATH] [--no-target] [flags...]
  lading-py run [--config-path PATH] [--no-target] [flags...]
  lading-py config-check [--config-path PATH]

Config is also accepted via the LADING_CONFIG environment variable (raw YAML).

Lifecycle:
  warmup → experiment_started → experiment → shutdown → drain
"""
import argparse
import asyncio
import os
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

DEFAULT_CONFIG_PATH = "/etc/lading/lading.yaml"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _add_run_args(p: argparse.ArgumentParser) -> None:
    """Attach all runtime flags to a parser (shared between flat and `run`)."""
    p.add_argument(
        "--config-path",
        default=os.environ.get("LADING_CONFIG_PATH", DEFAULT_CONFIG_PATH),
        metavar="PATH",
        help=f"path to lading YAML config (default: {DEFAULT_CONFIG_PATH})",
    )
    p.add_argument("--global-labels", default=None, metavar="KEY=VAL,...",
                   help="additional labels applied to all captures")

    # Target group — one is required but lading-py only supports --no-target
    # and --target-pid; others are accepted and ignored for compat
    tgt = p.add_mutually_exclusive_group(required=False)
    tgt.add_argument("--no-target", action="store_true",
                     help="disable target measurement (default behaviour)")
    tgt.add_argument("--target-pid", type=int, default=None, metavar="PID",
                     help="measure an externally-launched process by PID")
    tgt.add_argument("--target-path", default=None, metavar="PATH",
                     help="(accepted for compat; target execution not supported)")
    tgt.add_argument("--target-container", default=None, metavar="NAME",
                     help="(accepted for compat; container targeting not supported)")

    # Telemetry overrides
    p.add_argument("--capture-path", default=None, metavar="PATH",
                   help="override telemetry output path from config")
    p.add_argument("--capture-format", default=None, choices=["jsonl", "parquet", "multi"],
                   help="override capture format (jsonl|parquet|multi)")
    p.add_argument("--capture-flush-seconds", type=int, default=None, metavar="N",
                   help="override capture flush interval")
    p.add_argument("--capture-compression-level", type=int, default=3, metavar="N",
                   help="parquet compression level 1-22 (default: 3)")
    p.add_argument("--capture-expiration-seconds", type=int, default=None, metavar="N",
                   help="(accepted for compat; not implemented)")
    p.add_argument("--prometheus-addr", default=None, metavar="ADDR",
                   help="override prometheus exporter bind address")
    p.add_argument("--prometheus-path", default=None, metavar="PATH",
                   help="override prometheus exporter socket path")

    # Lifecycle overrides
    p.add_argument("--experiment-duration-seconds", type=int, default=None, metavar="N",
                   help="override experiment duration from config")
    p.add_argument("--experiment-duration-infinite", action="store_true",
                   help="run indefinitely (until SIGTERM/SIGINT)")
    p.add_argument("--warmup-duration-seconds", type=int, default=None, metavar="N",
                   help="override warmup duration from config")
    p.add_argument("--max-shutdown-delay", type=int, default=30, metavar="N",
                   help="maximum seconds to wait for graceful shutdown (default: 30)")

    # Misc
    p.add_argument("--disable-inspector", action="store_true",
                   help="(accepted for compat; inspector not implemented)")


def _build_parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        prog="lading-py",
        description="lading-py: DogStatsD load generator",
    )
    root.add_argument("--json-logs", action="store_true",
                      help="output logs in JSON format")

    subs = root.add_subparsers(dest="subcommand")

    # `run` subcommand
    run_p = subs.add_parser("run", help="run lading with the specified configuration")
    _add_run_args(run_p)

    # `config-check` subcommand
    check_p = subs.add_parser("config-check", help="validate configuration file and exit")
    check_p.add_argument(
        "--config-path",
        default=os.environ.get("LADING_CONFIG_PATH", DEFAULT_CONFIG_PATH),
        metavar="PATH",
        help=f"path to lading YAML config (default: {DEFAULT_CONFIG_PATH})",
    )

    # Legacy flat mode (no subcommand) — add run args directly to root
    _add_run_args(root)

    return root


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def _load_raw_config(config_path: str) -> dict:
    lading_config_env = os.environ.get("LADING_CONFIG")
    if lading_config_env:
        return yaml.safe_load(lading_config_env)
    with open(config_path) as f:
        return yaml.safe_load(f)


def _apply_cli_overrides(config: RootConfig, args: argparse.Namespace) -> RootConfig:
    """Return a new RootConfig with CLI flag overrides applied."""
    raw = config.model_dump()

    # Experiment / warmup duration
    if getattr(args, "experiment_duration_seconds", None) is not None:
        raw["experiment_duration_secs"] = args.experiment_duration_seconds
    if getattr(args, "warmup_duration_seconds", None) is not None:
        raw["warmup_duration_secs"] = args.warmup_duration_seconds

    # Telemetry
    capture_path = getattr(args, "capture_path", None)
    capture_format = getattr(args, "capture_format", None)
    capture_flush = getattr(args, "capture_flush_seconds", None)
    prom_addr = getattr(args, "prometheus_addr", None)
    prom_path = getattr(args, "prometheus_path", None)

    if any(x is not None for x in (capture_path, capture_format, capture_flush, prom_addr, prom_path)):
        tel = raw.get("telemetry") or {}
        if capture_path:
            tel["path"] = capture_path
        if capture_format:
            tel.setdefault("log", {})["format"] = {capture_format: {
                "flush_seconds": capture_flush or 60
            }}
        elif capture_flush and not capture_format:
            tel.setdefault("log", {}).setdefault("format", {}).setdefault(
                "jsonl", {})["flush_seconds"] = capture_flush
        if prom_addr:
            tel["prometheus"] = {"addr": prom_addr}
        if prom_path:
            tel["prometheus_socket"] = {"path": prom_path}
        raw["telemetry"] = tel

    # Global labels
    global_labels_str = getattr(args, "global_labels", None)
    if global_labels_str:
        pairs = {}
        for token in global_labels_str.split(","):
            if "=" in token:
                k, _, v = token.partition("=")
                pairs[k.strip()] = v.strip()
        tel = raw.setdefault("telemetry", {})
        tel["global_labels"] = pairs

    return RootConfig.model_validate(raw)


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

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


async def inner_main(config: RootConfig, target_pid: int | None = None) -> None:
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

    # Observer
    if config.observer and target_pid is not None:
        obs = ProcObserver(config.observer, registry, period_secs)
        tasks.append(asyncio.create_task(obs.run(signals, target_pid), name="observer"))

    # Lifecycle
    if config.warmup_duration_secs > 0:
        await asyncio.sleep(config.warmup_duration_secs)

    signals.experiment_started.set()

    if config.experiment_duration_secs > 0:
        await asyncio.sleep(config.experiment_duration_secs)
    else:
        # infinite mode — wait for shutdown signal
        await signals.shutdown.wait()

    signals.set_shutdown()

    await asyncio.gather(*tasks, return_exceptions=True)

    for writer in writers:
        writer.finalize()


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    subcommand = getattr(args, "subcommand", None)

    # config-check: validate and exit
    if subcommand == "config-check":
        try:
            raw = _load_raw_config(args.config_path)
            RootConfig.model_validate(raw)
            print(f"Config OK: {args.config_path}")
            sys.exit(0)
        except Exception as exc:
            print(f"Config invalid: {exc}", file=sys.stderr)
            sys.exit(1)

    # run or legacy flat mode
    config_path = getattr(args, "config_path", DEFAULT_CONFIG_PATH)
    raw = _load_raw_config(config_path)
    config = RootConfig.model_validate(raw)
    config = _apply_cli_overrides(config, args)

    # --experiment-duration-infinite → set duration to 0 (signals infinite loop)
    if getattr(args, "experiment_duration_infinite", False):
        config = config.model_copy(update={"experiment_duration_secs": 0})

    target_pid = getattr(args, "target_pid", None)

    asyncio.run(inner_main(config, target_pid=target_pid))


if __name__ == "__main__":
    main()
