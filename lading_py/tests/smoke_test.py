"""
Smoke test: spin up a Unix datagram socket server, run lading-py for 3 seconds,
assert bytes were received and the output file was written.
"""
import asyncio
import os
import socket
import tempfile
import threading
import time
import pytest
import yaml


SOCKET_PATH = "/tmp/lading_smoke_test.socket"


def _socket_server(sock_path: str, received: list, stop_event: threading.Event):
    if os.path.exists(sock_path):
        os.unlink(sock_path)
    s = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    s.bind(sock_path)
    s.settimeout(0.1)
    while not stop_event.is_set():
        try:
            data, _ = s.recvfrom(65536)
            received.append(len(data))
        except socket.timeout:
            pass
    s.close()


@pytest.fixture
def smoke_config(tmp_path):
    out_path = str(tmp_path / "output")
    cfg = {
        "generator": [{
            "unix_datagram": {
                "seed": list(range(32)),
                "path": SOCKET_PATH,
                "bytes_per_second": "1 MiB",
                "parallel_connections": 1,
                "variant": {
                    "dogstatsd": {
                        "contexts": {"inclusive": {"min": 10, "max": 10}},
                        "tags_per_msg": {"inclusive": {"min": 2, "max": 2}},
                        "multivalue_count": {"inclusive": {"min": 2, "max": 4}},
                        "multivalue_pack_probability": 0.1,
                        "kind_weights": {"metric": 90, "event": 5, "service_check": 5},
                        "metric_weights": {"count": 1, "gauge": 1, "distribution": 3, "set": 0, "timer": 0, "histogram": 0},
                        "metric_names": ["test.metric{{0-4}}"],
                        "tag_names": ["env", "host"],
                        "tag_values": ["prod{{0-2}}"],
                    }
                },
            }
        }],
        "telemetry": {"path": out_path},
        "target_metrics": [],
        "warmup_duration_secs": 0,
        "experiment_duration_secs": 3,
    }
    cfg_path = str(tmp_path / "config.yaml")
    with open(cfg_path, "w") as f:
        yaml.dump(cfg, f)
    return cfg_path, out_path


def test_smoke(smoke_config):
    cfg_path, out_path = smoke_config

    received: list[int] = []
    stop = threading.Event()
    srv = threading.Thread(target=_socket_server, args=(SOCKET_PATH, received, stop), daemon=True)
    srv.start()
    time.sleep(0.1)

    from lading_py.config import RootConfig
    import lading_py.main as lm

    with open(cfg_path) as f:
        raw = yaml.safe_load(f)
    config = RootConfig.model_validate(raw)
    asyncio.run(lm.inner_main(config))

    stop.set()
    srv.join(timeout=2)

    assert sum(received) > 0, "no bytes received at socket"
    assert os.path.exists(out_path), "output file not created"
    with open(out_path) as f:
        lines = [l for l in f if l.strip()]
    assert len(lines) > 0, "output file is empty"

    if os.path.exists(SOCKET_PATH):
        os.unlink(SOCKET_PATH)
