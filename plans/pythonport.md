# Lading Python Port Plan

Port lading to Python with DogStatsD emission only. All telemetry collection,
reporting, and output functionality must be preserved.

---

## Scope

**In scope:**
- DogStatsD generator (emit via `dogstatsd-py` library)
- Telemetry collection from Datadog agent (Prometheus scrape + Expvar poll)
- Capture output (JSONL, Parquet, both)
- Prometheus exporter (passive HTTP `/metrics` endpoint)
- Observer (Linux `/proc` sampling)
- Config parsing (same YAML schema as Rust lading)
- Graceful lifecycle (warmup, experiment, shutdown)
- Blackhole (HTTP sink for target output)

**Out of scope:**
- All non-DogStatsD generators (TCP, UDP, HTTP, Unix stream, Fluent, OTLP, etc.)
- All non-DogStatsD payload types
- Windows support

---

## Reference Files

| Concern | Rust source |
|---------|-------------|
| DogStatsD payload generation | `lading_payload/src/dogstatsd.rs` |
| Unix datagram transport | `lading/src/generator/unix_datagram.rs` |
| Capture line schema | `lading_capture/src/line.rs` |
| Capture accumulator | `lading_capture/src/accumulator.rs` |
| Prometheus target metrics | `lading/src/target_metrics/prometheus.rs` |
| Expvar target metrics | `lading/src/target_metrics/expvar.rs` |
| Config schema | `lading/src/config.rs` |
| Example config | `lading.yaml` |

---

## Technology Choices

| Concern | Library | Rationale |
|---------|---------|-----------|
| DogStatsD emission | `dogstatsd-py` (`datadog`) | Required by spec |
| Async runtime | `asyncio` | Standard; matches Tokio concurrency model |
| HTTP client | `aiohttp` | Async Prometheus/Expvar scraping |
| Config parsing | `pydantic` + `PyYAML` | Validated schema, matches Rust serde |
| Prometheus export | `prometheus-client` | Passive `/metrics` scrape endpoint |
| JSONL output | stdlib `json` + `gzip` | Zero dependencies |
| Parquet output | `pyarrow` | Industry standard; schema matches Rust |
| Protobuf (DDSketch) | `protobuf` + datadog proto | Histogram serialization |
| `/proc` parsing | stdlib only (Linux) | Avoids psutil divergence from Rust impl |
| Structured logging | `structlog` | JSON-friendly |

---

## Project Layout

```
lading_py/
├── pyproject.toml
├── lading_py/
│   ├── __init__.py
│   ├── main.py               # Entry point; lifecycle orchestration
│   ├── config.py             # Pydantic config models (mirrors lading/src/config.rs)
│   ├── signal.py             # asyncio.Event wrappers for lifecycle signals
│   │
│   ├── generator/
│   │   ├── __init__.py
│   │   └── dogstatsd.py      # DogStatsD generator (uses dogstatsd-py)
│   │
│   ├── payload/
│   │   ├── __init__.py
│   │   └── dogstatsd.py      # Payload construction (context pool, tag pool)
│   │
│   ├── blackhole/
│   │   ├── __init__.py
│   │   └── http.py           # HTTP blackhole (aiohttp server)
│   │
│   ├── target_metrics/
│   │   ├── __init__.py
│   │   ├── prometheus.py     # Prometheus scraper
│   │   └── expvar.py         # Expvar poller
│   │
│   ├── observer/
│   │   ├── __init__.py
│   │   └── proc.py           # /proc/{pid}/ sampler (Linux)
│   │
│   ├── capture/
│   │   ├── __init__.py
│   │   ├── line.py           # Line dataclass (mirrors lading_capture/src/line.rs)
│   │   ├── accumulator.py    # Rolling metric accumulator
│   │   ├── jsonl_writer.py   # JSONL output
│   │   └── parquet_writer.py # Parquet output
│   │
│   └── telemetry/
│       ├── __init__.py
│       ├── registry.py       # Thread-safe counter/gauge/histogram registry
│       └── prometheus_exporter.py  # Passive HTTP /metrics endpoint
│
└── tests/
    ├── test_config.py
    ├── test_payload.py
    ├── test_capture.py
    └── test_target_metrics.py
```

---

## Step-by-Step Implementation Plan

### Phase 1: Foundation

#### Step 1 — Project scaffold

Create `pyproject.toml` with dependencies, entry point `lading-py`, and Python >= 3.11 requirement. Pin all deps.

```toml
[project]
name = "lading-py"
requires-python = ">=3.11"
dependencies = [
    "datadog>=0.49",          # dogstatsd-py
    "pydantic>=2",
    "PyYAML>=6",
    "aiohttp>=3.9",
    "prometheus-client>=0.20",
    "pyarrow>=15",
    "protobuf>=4",
    "structlog>=24",
]

[project.scripts]
lading-py = "lading_py.main:main"
```

#### Step 2 — Config models (`config.py`)

Pydantic models mirroring Rust config structs. Must parse the same YAML that Rust lading accepts.

Key models:
- `ConfRange` — `{inclusive: {min, max}}` or `{exclusive: ...}`
- `KindWeights` — `{metric, event, service_check}`
- `MetricWeights` — `{count, gauge, timer, distribution, set, histogram}`
- `DogStatsDConfig` — full dogstatsd variant config
- `UnixDatagramConfig` — transport config (path, bytes_per_second, etc.)
- `GeneratorConfig` — wrapper with optional `id`
- `BlackholeConfig` — HTTP blackhole
- `TargetMetricsConfig` — list of prometheus/expvar entries
- `TelemetryConfig` — log/prometheus/prometheus_socket variant
- `ObserverConfig`
- `RootConfig` — top-level; holds all above

Validation rules to replicate from Rust:
- `bytes_per_second` parsed from human-readable string ("1 MiB" → 1048576)
- `seed` must be 32 bytes
- `kind_weights` values must not all be zero
- `tag_length.end > MIN_TAG_LENGTH` check (PR #1875)

#### Step 3 — Signals (`signal.py`)

```python
class Signals:
    experiment_started: asyncio.Event
    shutdown: asyncio.Event
    target_pid: asyncio.Event  # set when target PID is known
```

Wraps broadcast-style coordination. All tasks await `experiment_started` before operating, all tasks check `shutdown` to terminate.

---

### Phase 2: DogStatsD Generator

#### Step 4 — Payload context pool (`payload/dogstatsd.py`)

This is the most complex piece. Must replicate lading's weighted random generation.

**Context pool:**
- Pre-generate N contexts (N = `contexts.max`)
- Each context: a fixed `(metric_name, tag_set)` tuple
- Tag sets sampled from tag name/value pools
- `unique_tag_ratio` controls how much tag reuse happens

**Metric name templates:**
- `name{{0-2}}` expands to `name0`, `name1`, `name2`
- Expand all templates at startup, store as flat list
- Sample uniformly from expanded list

**Payload generation produces call descriptors, not raw bytes.** All serialization
is delegated to dogstatsd-py at send time.

```python
@dataclass
class MetricCall:
    name: str
    value: float
    metric_type: str           # "gauge" | "count" | "histogram" | "distribution" | "timing" | "set"
    tags: list[str]            # ["tag1:val1", "tag2:val2"]
    sample_rate: float | None
    timestamp: int | None      # unix seconds, maps to |T field

@dataclass
class EventCall:
    title: str
    text: str
    tags: list[str]
    alert_type: str | None     # "error" | "warning" | "info" | "success"
    priority: str | None

@dataclass
class ServiceCheckCall:
    name: str
    status: int                # 0=OK 1=WARNING 2=CRITICAL 3=UNKNOWN
    tags: list[str]
    message: str | None

# A "block" is either a single call or a batch (multi-value)
Block = MetricCall | EventCall | ServiceCheckCall | list[MetricCall]

def generate_block(rng, config, contexts) -> Block:
    kind = weighted_choice(rng, config.kind_weights)
    if kind == "metric":
        count = 1
        if rng.random() < config.multivalue_pack_probability:
            count = rng.randint(config.multivalue_count.min, config.multivalue_count.max)
        calls = [_gen_metric_call(rng, config, contexts) for _ in range(count)]
        return calls if count > 1 else calls[0]
    elif kind == "event":
        return _gen_event_call(rng, config)
    else:
        return _gen_service_check_call(rng, config)
```

**Multi-value packing:**
- With probability `multivalue_pack_probability`, generate `multivalue_count` metric
  calls returned as a `list[MetricCall]`; the generator sends them inside a single
  `client.open_buffer()` context so dogstatsd-py packs them into one datagram

#### Step 5 — Block cache (`payload/dogstatsd.py`)

Pre-build a cache of `Block` descriptors (call parameter tuples) before the run starts.
`maximum_prebuild_cache_size_bytes` bounds the cache: estimate each `MetricCall` at
~200 bytes of Python object overhead (not wire bytes) for the purposes of capping count.

```python
class BlockCache:
    def __init__(self, config, seed, contexts, max_count):
        rng = Random(seed)  # deterministic
        self._blocks: list[Block] = []
        for _ in range(max_count):
            self._blocks.append(generate_block(rng, config, contexts))
        self._idx = 0

    def next(self) -> Block:
        block = self._blocks[self._idx]
        self._idx = (self._idx + 1) % len(self._blocks)
        return block
```

Use `random.Random` with a seed derived from the `seed` config field (32-byte array).
Python's Mersenne Twister differs from Rust's StdRng but exact RNG parity is not
required — statistical properties matter, not bit-for-bit reproducibility.

#### Step 6 — Generator task (`generator/dogstatsd.py`)

All emission goes through `dogstatsd-py` (`datadog.dogstatsd.DogStatsd`). No raw socket
fallback. Each `Block` from the cache is dispatched to the appropriate client method.
Multi-value batches use `client.open_buffer()` so dogstatsd-py packs them into one
datagram internally.

```python
_DISPATCH = {
    "gauge":        lambda c, m: c.gauge(m.name, m.value, tags=m.tags, sample_rate=m.sample_rate or 1),
    "count":        lambda c, m: c.increment(m.name, m.value, tags=m.tags, sample_rate=m.sample_rate or 1),
    "histogram":    lambda c, m: c.histogram(m.name, m.value, tags=m.tags, sample_rate=m.sample_rate or 1),
    "distribution": lambda c, m: c.distribution(m.name, m.value, tags=m.tags, sample_rate=m.sample_rate or 1),
    "timing":       lambda c, m: c.timing(m.name, m.value, tags=m.tags, sample_rate=m.sample_rate or 1),
    "set":          lambda c, m: c.set(m.name, m.value, tags=m.tags),
}

def _send_block(client: DogStatsd, block: Block) -> int:
    """Send block via dogstatsd-py; return estimated wire bytes for rate limiting."""
    if isinstance(block, list):
        # Multi-value batch — pack into one datagram
        with client.open_buffer() as buf:
            for m in block:
                _DISPATCH[m.metric_type](buf, m)
        return sum(_estimate_bytes(m) for m in block)
    elif isinstance(block, MetricCall):
        _DISPATCH[block.metric_type](client, block)
        return _estimate_bytes(block)
    elif isinstance(block, EventCall):
        client.event(block.title, block.text, tags=block.tags,
                     alert_type=block.alert_type, priority=block.priority)
        return _estimate_bytes(block)
    else:  # ServiceCheckCall
        client.service_check(block.name, block.status, tags=block.tags, message=block.message)
        return _estimate_bytes(block)

class DogStatsDGenerator:
    async def run(self, signals: Signals):
        await signals.experiment_started.wait()
        # One DogStatsd client per parallel connection (each has its own socket)
        clients = [DogStatsd(socket_path=self.config.path)
                   for _ in range(self.config.parallel_connections)]
        rate_limiter = TokenBucket(self.config.bytes_per_second)
        tasks = [
            asyncio.create_task(self._send_loop(client, rate_limiter, signals))
            for client in clients
        ]
        await asyncio.gather(*tasks)

    async def _send_loop(self, client: DogStatsd, rate_limiter: TokenBucket, signals: Signals):
        while not signals.shutdown.is_set():
            block = self.cache.next()
            est_bytes = _estimate_bytes_block(block)
            await rate_limiter.acquire(est_bytes)
            try:
                actual = _send_block(client, block)
                self.registry.increment("bytes_written", actual)
                self.registry.increment("packets_sent", 1)
            except Exception as exc:
                self.registry.increment("request_failure", 1, {"error": type(exc).__name__})
```

**Limitation:** `length_prefix_framed: true` is unsupported. dogstatsd-py does not
expose length-prefix framing and there is no compliant way to implement it without
bypassing the library. Config validation will reject `length_prefix_framed: true`
with a clear error message.

**Rate limiter:** Token bucket on estimated wire bytes (`len(name) + len(tags) + ~20`).
Async sleep to yield when bucket is empty.

---

### Phase 3: Telemetry Collection

#### Step 7 — Prometheus target metrics scraper (`target_metrics/prometheus.py`)

```python
class PrometheusScraper:
    async def run(self, signals: Signals):
        await signals.experiment_started.wait()
        async with aiohttp.ClientSession() as session:
            while not signals.shutdown.is_set():
                text = await session.get(self.config.uri)
                metrics = parse_prometheus_text(text)
                for m in metrics:
                    self.registry.record(m.name, m.kind, m.value, m.labels | self.config.tags)
                await asyncio.sleep(self.sample_period)
```

Prometheus text format parser: parse `# TYPE`, `# HELP`, metric lines. Handle counter,
gauge, histogram, summary. Map to lading's `MetricKind` (Counter/Gauge/Histogram).

#### Step 8 — Expvar target metrics poller (`target_metrics/expvar.py`)

```python
class ExpvarPoller:
    async def run(self, signals: Signals):
        await signals.experiment_started.wait()
        async with aiohttp.ClientSession() as session:
            while not signals.shutdown.is_set():
                data = await session.get(self.config.uri)  # JSON
                for var_path in self.config.vars:
                    value = jsonpath_get(data, var_path)  # e.g. "/forwarder/Transactions/Success"
                    self.registry.record(var_path, MetricKind.Gauge, value, self.config.tags)
                await asyncio.sleep(self.sample_period)
```

Path resolution: split `/foo/bar/baz` → nested dict lookup `data["foo"]["bar"]["baz"]`.

---

### Phase 4: Observer

#### Step 9 — `/proc` observer (`observer/proc.py`)

Linux only. Samples `/proc/{pid}/smaps_rollup` and optionally `/proc/{pid}/smaps`
every `sample_period` seconds after `experiment_started`.

Key metrics from `smaps_rollup`:
- `Rss` → gauge `smaps_rollup.Rss`
- `Pss` → gauge `smaps_rollup.Pss`
- `Private_Clean`, `Private_Dirty` → gauge
- `Anonymous` → gauge

Parse format: `FieldName: <value> kB` lines.

Record all fields as gauges with label `pid=<target_pid>`.

---

### Phase 5: Capture Output

#### Step 10 — Metric registry (`telemetry/registry.py`)

Thread-safe in-process registry. All generator/collector/observer code calls into this.

```python
class Registry:
    def increment(self, name: str, value: int, labels: dict): ...
    def set_gauge(self, name: str, value: float, labels: dict): ...
    def record_histogram(self, name: str, value: float, labels: dict): ...
    def snapshot(self) -> list[Line]: ...  # drain for flush
```

Internal storage: `threading.Lock` guarding dicts of `Counter`, `Gauge`, `DDSketch`.

#### Step 11 — Line model (`capture/line.py`)

Mirrors `lading_capture/src/line.rs`:

```python
@dataclass
class Line:
    run_id: str          # UUID
    time: int            # ms since epoch
    fetch_index: int     # flush counter
    metric_name: str
    metric_kind: str     # "counter" | "gauge" | "histogram"
    value: float | int
    labels: dict[str, str]
    value_histogram: bytes  # protobuf DDSketch, empty if not histogram
```

#### Step 12 — JSONL writer (`capture/jsonl_writer.py`)

```python
class JsonlWriter:
    def flush(self, lines: list[Line], fetch_index: int):
        with open(self.path, "a") as f:
            for line in lines:
                f.write(json.dumps(dataclasses.asdict(line)) + "\n")
```

Flush every `flush_seconds` seconds via `asyncio.sleep` loop.
`value_histogram` bytes field: base64-encode in JSON output (matches Rust behavior).

#### Step 13 — Parquet writer (`capture/parquet_writer.py`)

Schema mirrors Rust Parquet output:

```python
SCHEMA = pa.schema([
    ("run_id", pa.string()),
    ("time", pa.int64()),
    ("fetch_index", pa.int64()),
    ("metric_name", pa.string()),
    ("metric_kind", pa.string()),
    ("value", pa.float64()),
    ("labels", pa.map_(pa.string(), pa.string())),
    ("value_histogram", pa.binary()),
])
```

Accumulate rows in memory, flush to Parquet file at `flush_seconds` interval using
`pyarrow.parquet.write_table`. Append row groups (open file in append mode or write
separate files per flush and concatenate on shutdown).

#### Step 14 — Accumulator (`capture/accumulator.py`)

60-tick rolling window matching Rust accumulator behavior.

Tracks per-metric history for computing rates (counters are differenced across ticks).
On each flush tick:
1. Snapshot registry
2. Diff counters vs previous snapshot
3. Pass gauge/histogram values through directly
4. Write `Line` objects to writer(s)

---

### Phase 6: Telemetry Export

#### Step 15 — Prometheus exporter (`telemetry/prometheus_exporter.py`)

Passive HTTP endpoint. Uses `prometheus_client` library.

```python
class PrometheusExporter:
    async def run(self, signals: Signals):
        # aiohttp handler for GET /metrics
        # prometheus_client.generate_latest() for text format
        # Periodically syncs from Registry to prometheus_client collectors
```

#### Step 16 — Blackhole HTTP (`blackhole/http.py`)

`aiohttp` server that accepts all POST/PUT requests and discards the body.
Records bytes received as a counter. Binds to configured address.

---

### Phase 7: Lifecycle Orchestration

#### Step 17 — Main (`main.py`)

```python
async def inner_main(config: RootConfig):
    signals = Signals()
    run_id = str(uuid.uuid4())

    # Build telemetry output
    registry = Registry()
    writer = build_writer(config.telemetry, run_id)

    # Build and start all components as asyncio tasks
    tasks = []

    if config.generator:
        for gen_cfg in config.generator:
            dsd_cfg = gen_cfg.unix_datagram.variant.dogstatsd
            contexts = build_context_pool(dsd_cfg)
            cache = BlockCache(dsd_cfg, gen_cfg.unix_datagram.seed, contexts, max_count=10_000)
            tasks.append(asyncio.create_task(
                DogStatsDGenerator(gen_cfg, cache, registry).run(signals)
            ))

    for bh_cfg in config.blackhole or []:
        tasks.append(asyncio.create_task(BlackholeHttp(bh_cfg).run(signals)))

    for tm_cfg in config.target_metrics or []:
        tasks.append(asyncio.create_task(build_target_metrics(tm_cfg, registry, signals)))

    if config.observer:
        tasks.append(asyncio.create_task(Observer(config.observer, registry).run(signals)))

    tasks.append(asyncio.create_task(accumulate_and_flush(registry, writer, signals)))

    # Lifecycle
    await asyncio.sleep(config.warmup_seconds or 0)
    signals.experiment_started.set()
    await asyncio.sleep(config.experiment_duration_seconds)
    signals.shutdown.set()

    await asyncio.gather(*tasks, return_exceptions=True)
    writer.finalize()

def main():
    import argparse, yaml
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    with open(args.config) as f:
        raw = yaml.safe_load(f)
    config = RootConfig.model_validate(raw)
    asyncio.run(inner_main(config))
```

Signal handling: install `SIGTERM`/`SIGINT` handler that sets `signals.shutdown`.

---

### Phase 8: Testing

#### Step 18 — Unit tests

- `test_config.py`: load `lading.yaml`, verify all fields parse correctly
- `test_payload.py`: generate 1000 messages, verify wire format parses as valid DogStatsD
- `test_capture.py`: write Lines to JSONL and Parquet, read back, verify schema
- `test_target_metrics.py`: mock aiohttp responses, verify Prometheus text parse

#### Step 19 — Integration smoke test

`tests/smoke_test.py`:
1. Spin up a UDP socket server (mimicking the agent's DogStatsD socket)
2. Run `lading-py --config tests/smoke.yaml` for 5 seconds
3. Assert bytes received > 0
4. Assert JSONL output file exists and has lines

---

## Implementation Order

1. `config.py` — foundation everything else depends on
2. `signal.py` — trivial, needed early
3. `payload/dogstatsd.py` — context pool, block cache
4. `generator/dogstatsd.py` — core feature
5. `telemetry/registry.py` — all metrics recording
6. `capture/line.py` + `capture/jsonl_writer.py` — minimum viable output
7. `main.py` — wire everything together; can test end-to-end here
8. `target_metrics/prometheus.py` + `target_metrics/expvar.py`
9. `observer/proc.py`
10. `capture/parquet_writer.py` + `capture/accumulator.py`
11. `telemetry/prometheus_exporter.py` + `blackhole/http.py`
12. Tests

---

## Key Fidelity Decisions

| Behavior | Rust | Python |
|----------|------|--------|
| RNG | SeededStdRng (ChaCha) | `random.Random(seed)` |
| RNG parity | Bit-exact reproducibility | Not required; stats parity only |
| Concurrency | Tokio async | asyncio |
| Socket type | Raw Unix datagram | dogstatsd-py exclusively; `open_buffer()` for batches |
| Histogram sketch | DDSketch (protobuf) | Same protobuf schema |
| Time unit | ms (u128) | `int(time.time() * 1000)` |
| Byte sizes | `bytesize` crate parsing | Manual parser: "1 MiB" → 1048576 |
| Config YAML | serde_yaml | PyYAML + pydantic |

---

## Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| `length_prefix_framed: true` unsupported by dogstatsd-py | Reject at config validation with clear error; all other wire formats work |
| Python throughput lower than Rust | Pre-built block cache + asyncio avoids per-message allocation; accept perf trade-off |
| DDSketch protobuf schema not public | Extract `.proto` from datadog-agent repo; codegen with `protoc` |
| Prometheus text format edge cases | Use `prometheus_client`'s own parser instead of hand-rolling |
| `/proc` parsing changes across kernel versions | Mirror Rust's exact field-by-field parsing; skip unknown fields |
