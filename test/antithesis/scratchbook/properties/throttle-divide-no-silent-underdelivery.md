---
slug: throttle-divide-no-silent-underdelivery
subsystem: generators/throttle
assertion_type: Always
priority: P0
needs_target: true
needs_sut_fix: divide must shrink block sizing consistently with capacity, or validate maximum_block_size <= bps/parallel_connections upfront
---

# A config that delivers at N=1 still delivers at N>1

## Statement

`throttle.divide` shrinks per-worker capacity consistently with the block size a worker draws,
so a block accepted at `parallel_connections=1` is not rejected by every worker (Capacity) at
`N>1`, yielding silent zero delivery.

## Code evidence trail

- `lib.rs` — `divide` produces per-worker capacity `capacity/divisor` (integer division) but
  does NOT shrink the block a worker draws.
- `tcp.rs:273-276` — on a Capacity error the block is discarded (`continue`) with only a
  `debug!` log.
- A block sized `R/N < block <= R` is accepted at `parallel_connections=1` but rejected with
  `Capacity` by every worker at `N>1` — silent zero delivery.

## Assertion-type rationale

**Always** (rate fidelity): on every timeline a config that delivers at `N=1` must not discard
every block at `N>1`. Provable as a pure proptest against the real Valve (no rig); Antithesis
oracle: sink byte counter vs configured rate.

## Mode / observability

For `bytes_per_second/N < block <= bytes_per_second` the aggregate delivered bytes at `N>1` are
nonzero and match the `N=1` rate; today every worker discards. Silent (only a `debug!` log).

## Mechanism

`divide` must shrink block sizing consistently with capacity, or generators must validate
`maximum_block_size <= bytes_per_second/parallel_connections` upfront.

## needs_sut_fix

Yes. Demonstrated on backup branch (`0868e39c`); live on main.

## Investigation Log

- [2026-07-24] Confirmed live regression on main. Provable as a pure proptest against the real
  Valve without any target rig.
- [2026-07-24] Related to `throttle-capacity-no-zerodelivery-livelock` (the busy-loop / 100% CPU
  angle) and `discarded-blocks-counted` (the observability angle).

## Open Questions

- None specific.
