---
slug: observer-pid-reuse-no-panic
subsystem: observer
assertion_type: Unreachable
priority: P0
needs_target: true
needs_sut_fix: replace the assert! at stat.rs:82 with a skip-and-continue
---

# Observer never aborts on PID reuse / mismatch

## Statement

The stat sampler degrades (skips the stale sample) instead of asserting `cur_pid==pid` when a
recycled/mismatched PID is read.

## Code evidence trail

- `stat.rs:82` — `assert!(cur_pid == pid)`.

## Assertion-type rationale

**Unreachable**: the assert must never fire. Oracle: rapid target-exit/PID-reuse scenario +
panic hook.

## Mode / observability

Target exit + PID recycle races do not SIGABRT the run. Confirmed live: `assert!(cur_pid ==
pid)` at `stat.rs:82`.

## Mechanism

Replace the `assert!` at `stat.rs:82` with a skip-and-continue.

## needs_sut_fix

Yes. Fix `6aa1b1ba` on backup branch; live on main.

## Investigation Log

- [2026-07-24] Verified against source this turn: `stat.rs:82` `assert!(cur_pid == pid)` is live
  on main; `panic=abort` makes any hit an observable abort.
- [2026-07-24] Same fix commit (`6aa1b1ba`) also addresses the cpu.max parse (see
  `observer-cpu-max-parse-no-panic`).

## Open Questions

- None specific.
