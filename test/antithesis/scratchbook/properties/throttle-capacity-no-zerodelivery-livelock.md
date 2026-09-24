---
slug: throttle-capacity-no-zerodelivery-livelock
subsystem: generators/throttle
assertion_type: Always
priority: P0
needs_target: true
needs_sut_fix: validate maximum_block_size <= bytes_per_second/parallel_connections (post-divide) at construction
---

# A block larger than throttle capacity never becomes a zero-delivery busy loop

## Statement

No config leaves a generator discarding every block (`block > per-worker capacity`) in a hot
loop at ~100% CPU delivering ~zero bytes; such a config is rejected at startup.

## Code evidence trail

- `common.rs:147-157` — `wait_for_block` requests `peek_next_size` tokens (the block's
  `total_bytes`).
- `stable.rs:151-156` — a block exceeding `maximum_capacity` returns `Error::Capacity`
  immediately with no wait.
- `tcp.rs:273-276` / `udp.rs:284-287` / `unix_stream.rs:320-323` / `unix_datagram.rs:297-300` —
  catch Capacity as "Discarding block", advance, and loop -> hot busy loop delivering ~zero
  bytes at 100% CPU.

## Assertion-type rationale

**Always** (no-livelock): on every timeline a run either delivers a nonzero rate or fails fast
at startup; it never burns a core delivering nothing.

## Mode / observability

A run either delivers a nonzero rate or fails fast at startup; it never burns a core delivering
nothing (the 0.31.2 busy-discard livelock class). Oracle: oversized-block scenario + CPU/
throughput observation; or startup-error assertion.

## Mechanism

Validate `maximum_block_size <= bytes_per_second/parallel_connections` (post-divide) at
construction, returning a clear error instead of a runtime discard/spin.

## needs_sut_fix

Yes.

## Investigation Log

- [2026-07-24] Confirmed: when a block's byte size exceeds the (post-divide) throttle capacity,
  `wait_for()` returns `Error::Capacity` with no wait, and every generator discards + loops.
- [2026-07-24] Closely related to `throttle-divide-no-silent-underdelivery` (the divide case
  that creates this condition) and `discarded-blocks-counted` (observability).

## Open Questions

- Should generators treat throttle `Error::Capacity` as a fatal startup validation error rather
  than a run-time discard/busy-loop, given `maximum_block_size` and `bytes_per_second` are both
  known at construction?
