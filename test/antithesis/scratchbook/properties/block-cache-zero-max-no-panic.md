---
slug: block-cache-zero-max-no-panic
subsystem: no-panic
assertion_type: Unreachable
priority: P1
needs_target: false
needs_sut_fix: add a lower-bound (>=1, or >= serializer floor) check on maximum_block_size
---

# maximum_block_size of 0 is rejected, not a random_range panic

## Statement

A `maximum_block_size` that resolves to 0 is rejected at validation; block-cache construction
never calls `rng.random_range` on an empty `0..0` range.

## Code evidence trail

- `block.rs:628` — `rng.random_range(min_block_size..max_block_size)` panics on an empty range
  when `max_block_size == 0`.
- `block.rs:214-220` — the guard only errors when `maximum_block_bytes` exceeds `u32::MAX` or
  `total_bytes`; a value of 0 passes through as `max_block_size=0`.
- `grpc.rs:205-223` forwards `config.maximum_block_size.as_u128()` unvalidated; a config
  `maximum_block_size: '0 B'` parses to 0.

## Assertion-type rationale

**Unreachable**: the empty-range `random_range` must never be reached. Oracle: zero-block-size
scenario + panic hook.

## Mode / observability

Config with `maximum_block_size '0 B'` produces a clean startup error, not a panic. Currently
reachable via `grpc.rs` forwarding unvalidated `as_u128()`.

## Mechanism

Add a lower-bound (`>=1`, or `>=` serializer floor) check on `maximum_block_size` in
`block.rs:214-220` and at generator call sites (e.g. `grpc.rs:205-223`).

## needs_sut_fix

Yes.

## Investigation Log

- [2026-07-24] Confirmed: `min_block_size` can never independently reach `max_block_size` (it
  is capped at `0.25*block_size < max_block_size`), so `max_block_size==0` is the trigger for
  the empty-range panic.
- [2026-07-24] Sibling of `block-cache-construction-terminates`: zero is the panic case, small
  is the hang case.

## Open Questions

- Does config variation ever reach `maximum_block_size: '0 B'`? If so this is a live startup
  panic, not latent.
