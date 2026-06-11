# lading-py

A Python port of [lading](https://github.com/datadog/lading) focused on DogStatsD
load generation. Uses the [dogstatsd-py](https://github.com/DataDog/datadogpy)
library for all metric emission, making it suitable for testing the client library
itself under realistic load.

All other lading capabilities are preserved: Prometheus and expvar telemetry
collection from a running Datadog Agent, JSONL/Parquet capture output, and a
passive Prometheus exporter for real-time scraping.

## Requirements

- Python 3.10+
- A Unix domain socket to send DogStatsD traffic to (typically the Datadog Agent's
  `/tmp/dsd.socket` or `DD_DOGSTATSD_SOCKET`)

## Installation

```bash
pip install -e /path/to/lading_py
```

Or from the directory:

```bash
cd lading_py
pip install -e .
```

This installs the `lading-py` command.

## Configuration

lading-py uses the same YAML config format as the Rust lading binary. A minimal
config that sends DogStatsD metrics and writes a JSONL capture file:

```yaml
generator:
  - unix_datagram:
      seed: [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53,
             59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131]
      path: "/tmp/dsd.socket"
      bytes_per_second: "1 MiB"
      parallel_connections: 1
      variant:
        dogstatsd:
          contexts:
            inclusive:
              min: 50
              max: 50
          tags_per_msg:
            inclusive:
              min: 3
              max: 3
          kind_weights:
            metric: 90
            event: 5
            service_check: 5
          metric_weights:
            count: 1
            gauge: 1
            distribution: 3
            timer: 1
            set: 0
            histogram: 0
          metric_names:
            - myapp.requests{{0-9}}
          tag_names:
            - env
            - service
            - version
          tag_values:
            - prod{{0-2}}

telemetry:
  path: "/tmp/lading-output.jsonl"

warmup_duration_secs: 5
experiment_duration_secs: 60
```

### Config reference

#### `generator[].unix_datagram`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `seed` | list[int] (32 bytes) | required | RNG seed for deterministic payload generation |
| `path` | string | required | Unix domain socket path |
| `bytes_per_second` | string | `"1 MiB"` | Rate limit. Accepts human-readable sizes: `"500 KiB"`, `"4 MiB"`, `"1 GiB"` |
| `parallel_connections` | int | `1` | Number of concurrent sender threads |
| `variant.dogstatsd` | object | | DogStatsD payload config (see below) |

#### `variant.dogstatsd`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `contexts` | ConfRange | `{inclusive: {min: 50, max: 50}}` | Number of unique metric contexts (name + tag set) to pre-generate |
| `tags_per_msg` | ConfRange | `{inclusive: {min: 3, max: 3}}` | Tags per metric |
| `multivalue_count` | ConfRange | `{inclusive: {min: 2, max: 32}}` | Messages per batch when multi-value packing fires |
| `multivalue_pack_probability` | float | `0.08` | Probability of packing multiple metrics into one datagram |
| `kind_weights` | object | `{metric: 90, event: 0, service_check: 0}` | Relative weight of each DogStatsD message kind |
| `metric_weights` | object | `{distribution: 5, ...rest 0}` | Relative weight of each metric type |
| `metric_names` | list[string] | `["metric{{0-9}}"]` | Metric name templates. `{{0-9}}` expands to 10 variants |
| `tag_names` | list[string] | `["tag1","tag2","tag3"]` | Tag name templates |
| `tag_values` | list[string] | `["value{{0-9}}"]` | Tag value templates |
| `sampling_range` | ConfRange | `{inclusive: {min: 0.1, max: 1.0}}` | Range for sample rate values |
| `sampling_probability` | float | `0.5` | Probability that a metric includes a sample rate |
| `length_prefix_framed` | bool | `false` | **Unsupported** — lading-py will reject configs with this set to `true` |

#### `telemetry`

Short form (JSONL output):
```yaml
telemetry:
  path: "/tmp/output.jsonl"
```

Long form with format control:
```yaml
telemetry:
  log:
    path: "/tmp/output"
    format:
      jsonl:
        flush_seconds: 60
      # or: parquet: {flush_seconds: 60}
      # or: multi: {flush_seconds: 60}   # writes both .jsonl and .parquet
```

Prometheus exporter (passive scrape endpoint):
```yaml
telemetry:
  prometheus:
    addr: "0.0.0.0:9000"
```

#### `target_metrics`

Collect telemetry from a running Datadog Agent:

```yaml
target_metrics:
  - prometheus:
      uri: "http://127.0.0.1:5000/telemetry"
      tags:
        sub_agent: "core"
  - expvar:
      uri: "http://127.0.0.1:5012/debug/vars"
      vars:
        - "/forwarder/Transactions/Success"
        - "/uptime"
      tags:
        sub_agent: "trace"

sample_period_milliseconds: 1000
```

#### `blackhole`

Absorb HTTP traffic from the target (e.g. agent intake forwarder in test):

```yaml
blackhole:
  - http:
      binding_addr: "127.0.0.1:9091"
```

#### Lifecycle

```yaml
warmup_duration_secs: 10     # wait before starting emission
experiment_duration_secs: 60  # how long to run after warmup
```

## Running

```bash
lading-py --config lading.yaml
```

The process runs for `warmup_duration_secs + experiment_duration_secs` seconds,
then exits. The capture file (if configured) is finalized on exit.

## Output format

### JSONL

One JSON object per line, one line per metric per flush interval:

```json
{"run_id": "550e8400-...", "time": 1717959420000, "fetch_index": 0, "metric_name": "bytes_written", "metric_kind": "counter", "value": 1048576.0, "labels": {"generator": "dogstatsd"}}
{"run_id": "550e8400-...", "time": 1717959420000, "fetch_index": 0, "metric_name": "cpu_usage", "metric_kind": "gauge", "value": 0.73, "labels": {"sub_agent": "core"}}
```

Fields:

| Field | Type | Description |
|-------|------|-------------|
| `run_id` | UUID string | Unique identifier for this lading-py run |
| `time` | int | Milliseconds since Unix epoch |
| `fetch_index` | int | Flush counter (increments each flush interval) |
| `metric_name` | string | Metric name |
| `metric_kind` | string | `"counter"`, `"gauge"`, or `"histogram"` |
| `value` | float | Counter delta, gauge value, or histogram mean |
| `labels` | object | Key-value label pairs |
| `value_histogram` | string (base64) | Protobuf DDSketch bytes (omitted if empty) |

### Parquet

Same schema as JSONL, written as columnar Parquet. Suitable for analysis with
pandas, DuckDB, or similar:

```python
import pyarrow.parquet as pq
table = pq.read_table("/tmp/output.parquet")
df = table.to_pandas()
```

## Docker

```bash
docker build -t lading-py /path/to/lading
docker run --rm \
  -v /tmp/dsd.socket:/tmp/dsd.socket \
  -v /path/to/lading.yaml:/etc/lading/lading.yaml \
  -v /tmp/output:/tmp/output \
  lading-py --config /etc/lading/lading.yaml
```

## Differences from Rust lading

| Feature | Rust lading | lading-py |
|---------|------------|-----------|
| Emission library | Raw Unix datagram socket | `dogstatsd-py` (`datadog` package) |
| Generators | TCP, UDP, HTTP, Unix stream, Fluent, OTLP, DogStatsD | DogStatsD only |
| `length_prefix_framed` | Supported | **Not supported** (rejected at config load) |
| RNG | ChaCha (SeededStdRng) | Mersenne Twister (`random.Random`) |
| Reproducibility | Bit-exact across runs with same seed | Statistically equivalent; not bit-exact |
| Histogram output | Full DDSketch protobuf | Mean value only; `value_histogram` always empty |

## Development

```bash
pip install -e ".[dev]"
pytest tests/
```

Run just the unit tests (fast, no socket needed):

```bash
pytest tests/ -k "not smoke"
```
