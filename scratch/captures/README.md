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
# Build and run with the exerciser (cat mode, 60 seconds)
./scripts/run_capture_test.sh cat 60

# Or tail mode (tracks offsets, reads only new bytes)
./scripts/run_capture_test.sh tail 60

# Output lands in this directory
cat scratch/captures/fuse_reads.jsonl
cat scratch/captures/blackhole.jsonl
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

### FUSE capture BufWriter sizing

The FUSE writer thread uses `BufWriter::new()` (8 KB default buffer). This is
fine for the small `read` records but could be tuned with
`BufWriter::with_capacity()` if write syscall overhead becomes measurable.

## Future Work

- **Offline validator** — separate tool that reads both JSONL files, reconstructs
  content from block cache params, and computes completeness / fabrication /
  duplication metrics.
- **Performance validation** — run with and without capture to confirm overhead
  is negligible at target throughput.
- **Per-line unique IDs** — add unique line identifiers to the payload to
  eliminate content-hash ambiguity from block cache wrapping.
