---
slug: jsonl-prefix-valid-after-kill
subsystem: capture
assertion_type: Always
priority: P1
needs_target: false
needs_sut_fix: none (holds by construction)
---

# A SIGKILLed jsonl capture is always a valid parseable prefix

## Statement

Under abrupt kill (`node_termination`/SIGKILL) any surviving jsonl capture parses as a valid
prefix with no torn final record and strictly-increasing per-series `fetch_index`/`time`.

## Code evidence trail

- `anytime_capture_consistent.rs:44` — `always! (torn_before_final == 0)`.
- `anytime_capture_consistent.rs:49` — `always! (invariants_hold)`.
- `accumulator.rs:492-496` — `flush_tick = current_tick - INTERVALS`; flush emits ticks in
  strictly increasing order (monotonic flush).
- `state_machine.rs:305-307` — `time_ms = start_ms + tick*1000`, `fetch_index == tick`, so
  fetch_index->time is a global bijection.

## Assertion-type rationale

**Always**: every prefix on every timeline must validate. Oracle already implemented (harness
checker, `MIN_RECORDS=10` non-vacuity floor).

## Mode / observability

`anytime_capture_consistent.rs` `always! (torn_before_final==0)` and `always! (invariants_hold)`.
Loss of the last `<=60s` maturity window is tolerated by design; a torn/reordered record is a
defect.

## Mechanism

None (holds by construction); property guards against regressions in the accumulator flush
ordering.

## needs_sut_fix

None. This is a positive/regression-guard property.

## Investigation Log

- [2026-07-24] Confirmed the by-construction argument: `flush()` emits ticks strictly
  increasing (`accumulator.rs:496`) and drain continues from `last_flushed_tick+1` upward, so
  per-series fetch_index/time are strictly increasing in any truncated jsonl prefix.
- [2026-07-24] CAVEAT (SMPTNG-694): captures are now zstd-compressed. A truncated zstd stream
  may be undecodable past the last flushed frame — the "valid parseable prefix" assumption must
  be validated against the actual on-disk framing (see `recorded-traffic-crash-consistency`
  and `capture-no-fsync-durability`).

## Open Questions

- Does the jsonl-on-disk format survive a SIGKILL as a decodable prefix once zstd-framed, or
  only up to the last flushed frame boundary?
