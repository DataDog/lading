# lading_proptest

Property-based integration testing for the Datadog Agent logs pipeline. Generates structurally diverse log data, feeds it to a real agent via file tailing, collects what the agent sends to a fake HTTP intake, and asserts properties on the output.

## How It Works

Each test case:

1. **Proptest generates parameters** — log format, structure, line counts, etc.
2. **Input is generated** — unique log lines with embedded UUID markers for correlation
3. **Orchestrator runs the agent** — starts a Docker container (or local binary), mounts config and log files
4. **Logs are written** — the generated lines are written to a file the agent tails
5. **Agent processes and sends** — the agent tails the file, applies multiline aggregation/truncation/etc., and POSTs JSON payloads to our fake intake
6. **Intake collects** — our HTTP server decompresses and parses the agent's payloads
7. **Properties are checked** — declarative assertions verify data integrity, field preservation, correct aggregation, etc.

On failure, proptest shrinks the input to find a minimal reproducing case. Each shrink attempt spins up a new agent container.

## Scenarios

### Truncation

Tests that the agent correctly truncates lines exceeding `max_message_size_bytes`.

- Generates lines of varying sizes: well under limit, near boundary, well over
- Verifies lines under the limit arrive intact
- Verifies lines over the limit are split into a head chunk (with `...TRUNCATED...` appended) and tail chunks
- Tests across all 5 log formats: PlainText, Json, Syslog5424, ApacheCommon, TimestampPrefixed
- Config variants: truncation limit at 1KB, 64KB, 256KB (agent default)

**Test functions:** `truncation`, `truncation_plain_text`, `truncation_json`, `truncation_syslog`, `truncation_apache`, `truncation_timestamp_prefixed`

### Timestamp Multiline

Tests that the agent aggregates continuation lines with timestamp-prefixed headers.

- Generates header lines with distinct timestamps (so each triggers `startGroup`)
- Continuation lines have no timestamp or format structure
- Verifies all continuations are merged into the correct header's output entry
- Verifies continuations are in order
- Tests across 3 timestamp formats: TimestampPrefixed, Syslog5424, ApacheCommon

**Test functions:** `multiline_timestamp_prefixed`, `multiline_syslog`, `multiline_apache`, `multiline_all_formats`

### JSON Multiline

Tests the agent's JSON aggregation — buffering incomplete JSON objects and compacting them with `json.Compact()`.

- Generates structurally diverse valid JSON split across multiple lines
- 11 structural variants: Flat, Nested, WithArray, TopLevelArray, DeepNested, MixedNesting, EmptyObject, EmptyArray, EscapedStrings, UnicodeValues, MixedValueTypes
- Verifies compacted output is valid JSON with all fields preserved
- Does not encode knowledge of agent internals — discovers gaps mechanically

**Known discovery:** TopLevelArray (`[{...}]`) is not supported by the agent's JSON aggregator. The test finds this every time the variant appears.

**Test function:** `json_multiline`

### Mixed Multiline

Tests interactions between the agent's two aggregation systems when different formats are interleaved in the same file.

- Mixes all 3 timestamp formats, 5+ JSON variants, and plain text lines in random order
- Tests format transitions: syslog → JSON → apache → plain text → JSON
- Tests JSON with embedded timestamp values (JSON detection should take priority)
- Verifies both aggregation systems work correctly when rapidly alternating

**Test function:** `mixed_multiline`

## Running

### Prerequisites

- Docker with the agent image pulled:
  ```bash
  docker pull datadog/agent:7.78.2-full
  ```
- If using Colima on macOS, set `DOCKER_HOST`:
  ```bash
  export DOCKER_HOST=unix:///Users/$USER/.colima/default/docker.sock
  ```

### Basic Usage

```bash
# Run a specific test (1 case, no shrinking)
PROPTEST_MAX_SHRINK_ITERS=0 \
  DD_AGENT_IMAGE=datadog/agent:7.78.2-full \
  PROPTEST_CASES=1 \
  RUST_LOG=info \
  cargo test -p lading_proptest --lib truncation_plain_text -- --nocapture --test-threads=1

# Run all truncation tests
cargo test -p lading_proptest --lib truncation -- --nocapture --test-threads=1

# Run all multiline tests
cargo test -p lading_proptest --lib multiline -- --nocapture --test-threads=1

# Run everything
cargo test -p lading_proptest --lib tests:: -- --nocapture --test-threads=1
```

### Unit Tests Only (no agent needed)

```bash
cargo test -p lading_proptest --lib log_format
cargo test -p lading_proptest --lib log_gen
```

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `DD_AGENT_IMAGE` | `datadog/agent:latest` | Docker image for the agent container |
| `DD_AGENT_BINARY` | (none) | Path to agent binary (uses binary mode instead of container) |
| `DOCKER_HOST` | `/var/run/docker.sock` | Docker daemon socket (set for Colima on macOS) |
| `PROPTEST_CASES` | `1` | Number of proptest cases per test function |
| `PROPTEST_MAX_SHRINK_ITERS` | auto | Max shrink attempts on failure (set `0` to disable) |
| `RUST_LOG` | (none) | Tracing log level (`info`, `debug`, `trace`) |
| `LADING_KEEP_TEMP` | (unset) | If set, preserves temp directories even on success |

## Temp Directory

Each test case creates a temp directory under `~/.lading_proptest_tmp/` containing:

```
lading_proptest_XXXXXX/
  config/
    datadog.yaml              # Agent main config
    conf.d/proptest.d/
      conf.yaml               # Log source config
  logs/
    proptest.log              # Input: the log file the agent tails
  output.json                 # Output: full JSON of all received intake entries
  output_messages.txt         # Output: one message per line with byte count
```

On test success, the directory is cleaned up (unless `LADING_KEEP_TEMP=1`). On failure, it is preserved and the path is printed in diagnostics.

## Properties

| Property | What it checks |
|---|---|
| `AllLinesDelivered` | Every input UUID appears in at least one output entry |
| `NoExtraLines` | No output entry has a UUID not in the input |
| `ContentPreserved` | Non-truncated lines match content exactly |
| `TruncationRespected` | Over-limit lines truncated, under-limit lines intact |
| `MultilineAggregated` | All expected continuations present and in order |
| `ExpectedEntryCount` | Output entry count matches expected logical entries |
| `JsonIntegrity` | Output JSON has all expected fields with correct values |

## Agent Safety

The agent is configured to not contact real Datadog infrastructure:

- `DD_LOGS_CONFIG_LOGS_DD_URL` points to our local intake server
- `DD_DD_URL=http://localhost:1` catches any non-logs telemetry
- `DD_API_KEY=fake_key_for_proptest` would be rejected by real intake
- APM, process collection, metadata collection, and inventories are disabled

Orphaned containers are cleaned up via a `Drop` implementation that force-removes them.

## Timing

Each test case takes ~49 seconds:
- 30s pipeline warmup (agent initialization)
- 15s drain wait (agent processing + sending)
- ~4s container start/stop overhead

With `PROPTEST_CASES=5`, expect ~4 minutes per test function. With `--test-threads=1`, test functions run sequentially.

## Future Work

- **JSON multiline: top-level arrays** — the agent currently does not support aggregating top-level JSON arrays (`[{...}]`). The `TopLevelArray` variant is included to detect if/when support is added.
- **Non-atomic write simulation** — model scenarios where multiple applications write to the same log file, causing interleaved/corrupted multi-line entries. When a pretty-printed JSON object is written line-by-line and another process writes between lines, the agent sees invalid JSON mid-buffer. This could test the agent's error recovery and buffer flushing behavior. Relevant when writes exceed `PIPE_BUF` (4KB) or when applications flush per-line rather than per-entry.
- **Explicit multiline patterns** — PlainText with `log_processing_rules` regex patterns for aggregation (currently untested since PlainText has no auto-detection signal).
- **JSON multiline: incomplete objects** — test what happens when a JSON object is never closed (e.g., application crashes mid-write). Does the agent flush on timeout? How long?
- **Intake hard cap (~900KB)** — the Datadog intake enforces a hard size limit around 900KB per message, independent of the agent's `max_message_size_bytes` setting. Test behavior when the agent's configured limit is set above the intake cap, and verify the intake rejection is handled gracefully.
- **Action sequences** — extend scenarios with timed actions (write lines, sleep, rotate file, write more) to test the agent's behavior with file rotation, delayed writes, and aggregation timeouts.
- **TCP/UDP log delivery** — currently all tests use file tailing. Testing network-based log sources exercises different agent code paths.
- **Pipeline warmup optimization** — the 30s fixed warmup is conservative. Polling the agent's health or metrics endpoint could reduce this.
- **Parallel test execution** — running test functions in parallel on different ports. Requires a dedicated machine to avoid resource contention.
