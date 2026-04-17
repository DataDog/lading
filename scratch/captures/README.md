# Capture & Analysis System

Offline correctness testing for the logs agent by capturing inputs (FUSE reads)
and outputs (blackhole payloads), then running configurable invariant checks.

## How It Works

### FUSE Read Capture (`read_capture_path`)

When `read_capture_path` is set in the `logrotate_fs` generator config, every
FUSE read is logged as JSONL. A bounded `std::sync::mpsc::sync_channel` (10K
slots) connects the FUSE thread to a dedicated writer thread that drains to a
`BufWriter<File>`.

Record types:
- `block_cache_meta` — cache size and block count, emitted once at startup
- `file_created` — inode, group_id, cache_offset, created_tick, bytes_per_tick
- `file_rotated` — which file rotated, new file's inode and cache_offset
- `file_deleted` — final bytes_written, bytes_read, max_offset_observed
- `read` — relative_ms, inode, group_id, offset, size

To reconstruct the actual bytes returned by a read, you need the capture file
**plus** the config YAML (seed, variant, cache size) to rebuild an identical
block cache, then call `block_cache.read_at(cache_offset + offset, size)`.

### Blackhole HTTP Capture (`capture_path`)

When `capture_path` is set in the `http` blackhole config, every decoded HTTP
payload is logged as JSONL. A bounded `tokio::sync::mpsc::channel` (10K slots)
connects the request handler to a tokio writer task. The blackhole decompresses
payloads (gzip, deflate, zstd) before capturing.

Record fields: `relative_ms`, `compressed_bytes`, `payload` (decoded UTF-8).

A `blackhole_capture_dropped` metric counter is incremented when the channel is
full and a record is dropped.

### Timestamps

Both capture systems share a single `Instant` reference created at lading
startup. All timestamps are `relative_ms: u64` — milliseconds since that shared
epoch. This is zero-aligned with the tick index (`relative_ms / 1000 ~ tick`),
so captures from different runs are directly comparable without wall-clock
offsets.

## Offline Analysis Tool (`lading-analysis`)

A separate binary (`lading-analysis`) reads both capture files, reconstructs
inputs from the block cache, parses outputs, and runs configurable checks.

### Analysis Config

```yaml
inputs:
  fuse_capture: scratch/captures/fuse_reads.jsonl
  blackhole_capture: scratch/captures/blackhole.jsonl
  lading_config: scripts/agent-test/lading_ascii.yaml

output_dir: scratch/captures/analysis  # optional: dump reconstructed data

checks:
  - completeness:
      min_ratio: 0.95
  - fabrication:
      max_count: 0
  - duplication:
      max_ratio: 0.01
  - latency:
      max_p99_ms: 10000  # optional: omit for informational only
```

### Input Reconstruction

The tool always produces two representations:

- **Raw reads** (`reconstructed_inputs_raw.txt`) — one entry per FUSE read with
  exact timestamp, offset, size, and the actual bytes content. Shows exactly what
  the agent received and when.
- **Newline-delimited lines** (`reconstructed_inputs.txt`) — lines stitched
  across read boundaries, each annotated with contributing reads and their
  timestamps. Final partial lines are discarded (the agent's framer won't emit
  them). Used by all correctness checks.

### Checks

**Completeness** — fraction of unique input line hashes that appear in the output.
Reports per-group breakdown. Fails if ratio < `min_ratio`.

**Fabrication** — output lines whose hash doesn't match any input line. Shows up
to 5 example fabricated lines. Fails if count > `max_count`.

**Duplication** — output lines that match the same input hash more than once.
Only counts lines that matched an input (fabricated lines excluded). Fails if
ratio > `max_ratio`.

**Latency** — per-line latency from the last FUSE read contributing to the line
to the blackhole receipt. Reports min, p50, p95, p99, max and a 1-second bucket
histogram. Optionally fails if p99 > `max_p99_ms`. Informational if threshold
omitted.

### CLI

```bash
# Human-readable output (exit code 0 if all pass, 1 if any fail)
cargo run --bin lading-analysis -- --config analysis.yaml

# JSON output for CI integration
cargo run --bin lading-analysis -- --config analysis.yaml --json
```

### Adding Custom Checks

The `Check` trait in `lading_analysis::check` is public and extensible. To add a
new check:
1. Implement `Check` (name + check method taking `&AnalysisContext`)
2. Add a config variant to `CheckConfig` enum
3. Register in `from_config()` match

The `AnalysisContext` provides raw reads, reconstructed lines, output lines, and
raw FUSE/blackhole events for timing or ordering checks.

## Running Tests

```bash
# Exerciser scripts (no real agent needed)
./scripts/run_capture_test.sh cat 60
./scripts/run_capture_test.sh tail 60
./scripts/run_capture_test.sh agent 60

# Real DD agent test (requires Docker)
./scripts/agent-test/run.sh 60                                          # ascii variant
./scripts/agent-test/run.sh 60 scripts/agent-test/lading_templated_json.yaml  # templated JSON

# Standalone analysis on existing captures
cargo run --bin lading-analysis -- --config scripts/agent-test/analysis.yaml
```

Output files land in `scratch/captures/`:
- `fuse_reads.jsonl` — FUSE read capture
- `blackhole.jsonl` — blackhole payload capture
- `analysis/reconstructed_inputs_raw.txt` — per-read content with timestamps
- `analysis/reconstructed_inputs.txt` — per-line content with read annotations
- `analysis/extracted_outputs.txt` — output messages with timestamps

## Architecture Decisions

### ADR-1: Freeze model during cooldown (not unmount)

**Problem:** The cooldown feature was designed to shut down generators before
blackholes so the target can flush its pipeline. But for FUSE-based generators,
"shutting down" triggers `fusermount -u`, which blocks while the target has open
file descriptors on the mount. The target keeps reading, the unmount hangs, and
the FUSE handler continues serving reads for the entire cooldown + shutdown.

**Decision:** On the generator shutdown signal, freeze the model (`advance_time`
is skipped via a frozen tick cap, files stop growing) but keep the FUSE mount
alive. The target naturally reaches EOF, its pipeline drains, and it sends
remaining data to the blackhole. On the main shutdown signal (after cooldown),
the mount is unmounted and the target is killed.

**Key insight:** The kernel won't complete a FUSE unmount while any process has
open FDs on the mount. You cannot use unmounting to "stop" a FUSE generator
while the target is still reading.

### ADR-2: Cooldown period between generator freeze and full shutdown

**Problem:** When lading shuts down, all components receive the shutdown signal
simultaneously. The target has buffered data that hasn't been sent to the
blackhole yet. The FUSE mount and blackhole die before the target can flush.

**Decision:** Added `--cooldown-duration-seconds` CLI flag (default 0). Two
separate signal pairs: `gen_shutdown` (freeze generators) and `shutdown`
(everything else). After the experiment duration, generators freeze, then after
the cooldown period, everything else shuts down.

**Why the cooldown is still needed after freeze:** The DD logs agent batches logs
on a ~5-second timer and does not flush early when it reaches EOF. After freeze,
the agent has up to one batch interval of buffered data. Without cooldown, the
blackhole dies before the agent sends its last batch. A cooldown of ~10s (two
batch cycles) is the minimum safe value.

**Default behavior preserved:** With cooldown=0 (default), both signals fire
back-to-back — identical to the previous single-signal shutdown.

## Known Limitations

### Blackhole capture throughput at high load

Each channel slot holds one `BlackholeCaptureRecord` including the full decoded
payload as a `String`. At 5 MiB max payload size and 10K channel capacity, a
full channel would consume ~50 GiB of memory. At high throughput (e.g., 500
MiB/s input = ~100 payloads/s at 5 MiB each), the writer task must serialize
and write ~500 MiB/s to disk.

Options for high-throughput use:
1. **Size the channel by memory, not count** — cap at e.g. 100 MiB total.
2. **Small channel** — reduce to ~20-50 slots (100-250 MiB worst case).
3. **Capture compressed payloads** — store the raw compressed HTTP body instead
   of decoded text. Much smaller per slot, and the offline validator can
   decompress later. Likely the best tradeoff.

### Line uniqueness and cross-file overlap

The block cache is cyclic — `read_at()` wraps offsets via modulo. Each file gets
a random `cache_offset`. Two files can read from overlapping cache regions,
producing identical lines. This means content-hash matching can over-count
completeness (a line from File A matches output that came from File B).

**Mitigation:** use a cache much larger than `concurrent_logs × max_bytes_per_file`.

**Long-term fix:** prepend per-line unique IDs in the generator (see Future Work).

### Block cache determinism across builds

The block cache is rebuilt from the same seed for offline analysis. `SmallRng`
is not portable across `rand` crate versions. If the analysis tool links a
different `rand` version than lading, the reconstructed cache diverges silently.
Pin `rand` versions to avoid this.

### Output parser only supports JSON intake format

The blackhole capture stores the raw decoded HTTP payload. The output parser
assumes DD logs agent JSON format (`[{"message": "..."}]`). Agents using
protobuf encoding will produce payloads the parser cannot read.

### FUSE capture drain is coupled to FUSE teardown (known bandaid in place)

The FUSE capture writer thread owns a `BufWriter<File>`. Its lifetime is
transitively tied to FUSE teardown: `capture_tx` (the sender) lives inside
`LogrotateFS`, which is owned by `BackgroundSession`. The writer's
`for record in rx` loop only exits when that sender is dropped, which only
happens when the session is dropped.

`BackgroundSession::join()` in turn waits for the FUSE kernel thread to
exit, which requires an unmount. Nothing in lading reliably triggers that
unmount: `AutoUnmount` fires on drop of the session, not on `join()`, and
the target's open FDs can also block `fusermount`. If anything in this
chain hangs and the process is killed externally (docker stop timeout,
Ctrl-C escalation), the writer thread dies with records still buffered in
its `BufWriter`, and those records are lost. This is how we originally saw
the "capture under-counting" bug (`fs_read=128` in the counters, only 91
records on disk).

**Current mitigation (bandaid):** `spawn_capture_writer` flushes the
`BufWriter` to disk after every record. One extra `write()` syscall per
record, but records are durable under abrupt shutdown — and the capture
file can be tailed live during a run, which is genuinely useful. This is
fine for the rates a correctness harness runs at (tens of reads/sec to low
thousands).

**Proper fix (deferred, tracked in Future Work):** Phased shutdown with
explicit ordering. Three pieces:

1. Wait for the target process to exit (FDs closed) *before* attempting to
   tear down the FUSE mount, so unmount can actually succeed.
2. Explicitly drop the `BackgroundSession` (or invoke `fusermount -u`)
   instead of relying on `.join()` to magically trigger an unmount it was
   never designed to trigger.
3. Move `capture_tx` out of `LogrotateFS` so the capture writer can be
   drained independently of FUSE teardown, with a timeout and hard fallback
   (e.g. lazy unmount `fusermount -uz`) so a hung mount can't take out the
   capture.

Keeping flush-per-record even after this fix is fine — it's cheap and
enables live-tailing.

## Resolved Issues

- **Hash collision risk** — originally used `FxHasher` (`u64`); replaced with
  SHA-256 (`[u8; 32]`) for collision-resistant content matching.
- **Agent line-splitting at read boundaries** — resolved by newline-delimited
  reconstruction, which stitches lines across read boundaries and discards
  partial final lines. Kernel page-cache coalescing/reordering of FUSE reads
  is hidden from the checks because they operate on stitched lines, not raw
  per-read byte ranges.
- **Shutdown flush behavior** — resolved by ADR-1 (freeze model, keep mount
  alive) + ADR-2 (cooldown period keeps blackhole alive for agent flush).
  Completeness reached 100% with 30s cooldown.

## Future Work

- **Proper shutdown ordering for FUSE capture drain** — replace the
  flush-per-record bandaid (see Known Limitations) with phased shutdown:
  wait for the target to exit before unmounting, drop the
  `BackgroundSession` explicitly instead of trusting `.join()` to unmount,
  and decouple `capture_tx` from `LogrotateFS` so the writer can drain
  independently under a timeout with a lazy-unmount fallback.
- **Performance validation** — run with and without capture to confirm overhead
  is negligible at target throughput.
- **Per-line unique IDs** — add unique line identifiers to the payload to
  eliminate content-hash ambiguity from block cache wrapping.
- **Protobuf intake format** — extend the output parser to decode protobuf
  payloads in addition to JSON, for agents that use proto encoding.
- **YAML-driven check discovery** — the `Check` trait and framework are
  production-ready; what's missing is dynamic registration so users can add
  checks via config without modifying Rust code.
- **Unit tests for checks** — check logic is pure functions operating on
  `AnalysisContext`; add tests with synthetic data for edge cases.
