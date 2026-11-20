---
slug: block-cache-construction-terminates
subsystem: payload/determinism
assertion_type: Always
priority: P0
needs_target: false
needs_sut_fix: cap consecutive rejections / add time bound in construct_block_cache_inner; reject max_block_size below serializer floor
---

# Block-cache construction always terminates in bounded time

## Statement

`construct_block_cache_inner` terminates (with blocks or `InsufficientBlockSizes`) in bounded
time for every config; it never spins forever when `max_block_size` is below the serializer's
minimum viable block.

## Code evidence trail

- `block.rs:625-673` — the loop `while bytes_remaining > 0` decrements `bytes_remaining` only on
  `Ok(block)` (`:637`).
- `block.rs:651` — on `EmptyBlock` it sets `min_block_size = block_size*0.25`; since
  `block_size < max_block_size`, `min_block_size` stays `< max_block_size`, so the only other
  exit `if bytes_remaining < min_block_size` (`:670`) never fires when `bytes_remaining` is
  large.
- `block.rs:659-668` — the elapsed-time block only logs progress; it never breaks.
- `grpc.rs:212` forwards `maximum_block_size.as_u128()` with no lower-bound validation.

## Assertion-type rationale

**Always**/liveness (bounded startup): startup must complete or error within a bounded time on
every config. Oracle: small-`maximum_block_size` scenario + external startup-time bound; SUT
`reachable!` at the bounded-exit.

## Mode / observability

Startup completes (or errors) within a bounded time even with a tiny `maximum_block_size`.
Confirmed live: the loop has no time/iteration cap; on repeated `EmptyBlock`, `min_block_size`
stays `< max_block_size` so neither exit fires.

## Mechanism

Cap consecutive rejections / add a time bound in `block.rs:625-673` and return
`InsufficientBlockSizes`; reject `max_block_size` below a serializer-reported floor.

## needs_sut_fix

Yes. The trace_agent v04 variant is fixed on backup branch `456e85a3`; the general case is
still live on main.

## Investigation Log

- [2026-07-24] Verified against source this turn: no iteration or time cap; `min_block_size`
  capped at `0.25*block_size < max_block_size`.
- [2026-07-24] Reachable via small `config.maximum_block_size` (e.g. "10 B") with
  dogstatsd/otel/trace_agent whose minimum message is far larger.

## Open Questions

- Does the deployment/harness ever vary `maximum_block_size` to small values? If config
  variation reaches small block sizes, this hang is a live startup failure, not latent.
- Should `fixed_with_max_overhead` reject `maximum_block_bytes` below a serializer-reported
  minimum viable size, or impose an iteration/time cap?
