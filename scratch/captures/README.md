# Capture System

Offline correctness testing for the logs agent by capturing inputs (FUSE reads)
and outputs (blackhole payloads), then running assertions offline.

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
connects the request handler to a tokio writer task.

Record fields: `relative_ms`, `compressed_bytes`, `payload` (decoded UTF-8).

### Timestamps

Both capture systems share a single `Instant` reference created at lading
startup. All timestamps are `relative_ms: u64` — milliseconds since that shared
epoch. This is zero-aligned with the tick index (`relative_ms / 1000 ~ tick`),
so captures from different runs are directly comparable without wall-clock
offsets.

## Running a Capture Test

```bash
# Build and run with exerciser scripts (cat, tail, or agent mode)
./scripts/run_capture_test.sh cat 60
./scripts/run_capture_test.sh tail 60
./scripts/run_capture_test.sh agent 60                          # minimal agent emulator
./scripts/run_capture_test.sh agent 60 scripts/capture_test_happy.yaml  # custom config

# Run with the real DD agent
./scripts/agent-test/run.sh 60

# Output lands in this directory
cat scratch/captures/fuse_reads.jsonl
cat scratch/captures/blackhole.jsonl

# Run offline analysis
cargo run --bin lading-analysis -- --config scripts/agent-test/analysis.yaml
# Reconstructed data written to scratch/captures/analysis/
```

## Known Limitations

### Blackhole capture throughput at high load

Each channel slot holds one `BlackholeCaptureRecord` including the full decoded
payload as a `String`. At 5 MiB max payload size and 10K channel capacity, a
full channel would consume ~50 GiB of memory. At high throughput (e.g., 500
MiB/s input to the logs agent = ~100 payloads/s at 5 MiB each), the writer task
must serialize and write ~500 MiB/s to disk. If it falls behind, memory grows
fast.

For initial low-throughput correctness testing this is fine. For high-throughput
use, options include:

1. **Size the channel by memory, not count** — cap at e.g. 100 MiB total,
   track cumulative payload size, drop when budget is exceeded.
2. **Small channel** — reduce to ~20-50 slots (100-250 MiB worst case) and rely
   on the writer keeping up.
3. **Capture compressed payloads** — store the raw compressed HTTP body instead
   of the decoded text. Much smaller per slot, eliminates the
   `String::from_utf8_lossy().into_owned()` allocation, and the offline
   validator can decompress later. Likely the best tradeoff.

### Line uniqueness and cross-file overlap

The block cache is cyclic — `read_at()` wraps offsets via modulo. Each file gets
a random `cache_offset`, so different files read from different starting
positions. However:

- **Single-file wrapping**: if `max_bytes_per_file < total_cache_size`, a single
  file never wraps. This is the weaker (and sufficient) condition. The stronger
  condition `total_cache_size > load_rate × experiment_duration` also works but
  is more than needed since files rotate at `max_bytes_per_file`.
- **Cross-file overlap**: two files with different random cache_offsets can read
  from overlapping cache regions, producing identical lines. This means
  content-hash matching can over-count completeness (a line from File A matches
  output that came from File B).
- **Mitigation for now**: use a cache much larger than
  `concurrent_logs × max_bytes_per_file` to make overlap unlikely.
- **Long-term fix**: prepend per-line unique IDs in the generator (see Future
  Work).

### Shutdown ordering and flush behavior

When lading shuts down, the FUSE mount, blackhole, and target all receive the
shutdown signal simultaneously. The target (e.g., DD logs agent) may have
buffered data in its pipeline — already read from FUSE but not yet POSTed to the
blackhole. If the FUSE mount and blackhole tear down before the target finishes
flushing, those in-flight lines appear as missed in the analysis.

Observed behavior: the FUSE mount unmounts before the target's graceful shutdown
completes, so any final flush attempt finds nothing to read.

Options to investigate:
- **Shutdown reordering**: SIGTERM the target first, wait for it to exit, then
  tear down generators and blackholes. Lading already SIGTERMs the target and
  waits, but generators/blackholes shut down concurrently.
- **Drain period**: stop the generator (freeze the model, no new bytes) but keep
  the FUSE mount and blackhole alive for a configurable cooldown, then shut down.
- **Analysis-side margin**: exclude input lines from the last N seconds of the
  experiment when computing completeness, since they may be in-flight.

### Reconstructed input timestamps are collapsed

The input reconstruction accumulates all FUSE reads per file into one contiguous
range and splits into lines once (to avoid partial-line artifacts at read
boundaries). A side effect is that all lines from a file get the timestamp of the
earliest read, not the read that actually contained them. This means
`first_seen_ms` in `reconstructed_inputs.txt` is inaccurate — all lines show the
same time even though the file was read over many seconds.

Fix: after splitting lines from the full content, walk the individual read
records to assign each line the timestamp of the read whose byte range covers it.

### Hash collision risk in analysis tool (resolved)

The analysis tool originally used `FxHasher` (`u64`) for line matching — a
non-cryptographic hash prone to collisions. This was replaced with SHA-256
(`[u8; 32]`) using the `sha2` crate, which is collision-resistant.

### Agent line-splitting at read boundaries (resolved)

Previously, without `direct_io`, the kernel page cache coalesced reads and the
analysis tool split individual reads into lines, producing partial-line
fragments that didn't match output messages (~65 fabricated lines in early
testing). This was resolved by:
1. `direct_io: true` — every agent read hits FUSE directly (no kernel coalescing)
2. `newline_delimited` reconstruction mode — stitches lines across read
   boundaries, discards final partial lines
With both enabled, fabrication dropped to 0 in real agent testing.

### FUSE capture BufWriter sizing

The FUSE writer thread uses `BufWriter::new()` (8 KB default buffer). This is
fine for the small `read` records but could be tuned with
`BufWriter::with_capacity()` if write syscall overhead becomes measurable.

## Architecture Decisions

### ADR-1: `direct_io` for correctness testing

**Problem:** The Linux kernel page cache sits between the target's `read()`
syscalls and the FUSE handler. The kernel coalesces, reorders, and caches reads.
FUSE capture records don't represent what the target actually read — they
represent what the kernel requested from FUSE to fill its page cache.

**Decision:** Added an opt-in `direct_io: bool` config field on `logrotate_fs`.
When enabled, every file opened by the target gets `FOPEN_DIRECT_IO`, bypassing
the page cache entirely. Every target `read()` syscall becomes a 1:1 FUSE
request with an accurate timestamp.

**Tradeoff:** Less realistic I/O performance (no readahead, no page cache), but
perfect input observability. Use `direct_io: true` for correctness experiments,
`false` (default) for performance benchmarks.

### ADR-2: Freeze model during cooldown (not unmount)

**Problem:** The cooldown feature was designed to shut down generators before
blackholes so the target can flush its pipeline. But for FUSE-based generators,
"shutting down" triggers `fusermount -u`, which blocks while the target has open
file descriptors on the mount. The target keeps reading, the unmount hangs, and
the FUSE handler continues serving reads for the entire cooldown + shutdown.

**Decision:** On the generator shutdown signal, freeze the model (`advance_time`
is skipped, files stop growing) but keep the FUSE mount alive. The target
naturally reaches EOF, its pipeline drains, and it sends remaining data to the
blackhole. On the main shutdown signal (after cooldown), the mount is unmounted
and the target is killed.

**Key insight:** The kernel won't complete a FUSE unmount while any process has
open FDs on the mount. You cannot use unmounting to "stop" a FUSE generator
while the target is still reading.

### ADR-3: Cooldown period between generator freeze and full shutdown

**Problem:** When lading shuts down, all components receive the shutdown signal
simultaneously. The target has buffered data that hasn't been sent to the
blackhole yet. The FUSE mount and blackhole die before the target can flush.

**Decision:** Added `--cooldown-duration-seconds` CLI flag (default 0). Two
separate signal pairs: `gen_shutdown` (freeze generators) and `shutdown`
(everything else). After the experiment duration, generators freeze, then after
the cooldown period, everything else shuts down.

**Default behavior preserved:** With cooldown=0 (default), both signals fire
back-to-back — identical to the previous single-signal shutdown.

## Future Work

- **Performance validation** — run with and without capture to confirm overhead
  is negligible at target throughput.
- **Per-line unique IDs** — add unique line identifiers to the payload to
  eliminate content-hash ambiguity from block cache wrapping.
- **User-defined invariant checks** — YAML-driven framework for custom
  assertions (timing, ordering, aggregation). The `Check` trait in
  `lading_analysis` is designed for this.
