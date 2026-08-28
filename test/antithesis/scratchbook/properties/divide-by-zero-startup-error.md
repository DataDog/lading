---
slug: divide-by-zero-startup-error
subsystem: generators/throttle
assertion_type: Always
priority: P2
needs_target: true
needs_sut_fix: distribute the division remainder; surface DivisionByZero as a config validation error
---

# bytes_per_second < parallel_connections fails with a clear error, not DivisionByZero surprise

## Statement

A config where `bytes_per_second` divided by `parallel_connections` rounds to zero produces a
clear startup validation error, and integer-division truncation does not silently drop the
remainder rate.

## Code evidence trail

- `lib.rs` — `divide` returns `DivisionByZero` when `capacity/divisor` rounds to 0.
- `tcp.rs:168-173` — integer division `capacity/divisor` truncates; N workers deliver
  `N*floor(bps/N) < bps` whenever `bps` is not divisible by `parallel_connections`; the
  remainder (up to `N-1` bytes/interval) is never delivered.

## Assertion-type rationale

**Always**: on every timeline a small-bps/high-connection config either errors clearly at
startup or delivers within one interval-quantum of configured. Oracle: startup-error assertion
+ sink rate check.

## Mode / observability

Small-bps/high-connection config errors clearly at startup; aggregate delivered rate is within
one interval-quantum of configured (no systematic under-delivery of up to `N-1` bytes/interval).

## Mechanism

Distribute the division remainder across workers, and surface `DivisionByZero` as a config
validation error naming `bytes_per_second`/`parallel_connections`.

## needs_sut_fix

Yes.

## Investigation Log

- [2026-07-24] Confirmed: integer-division truncation systematically under-delivers the
  remainder, and `bps < parallel_connections` fails to start with a bare `DivisionByZero`
  rather than a config-validation error.

## Open Questions

- None specific.
