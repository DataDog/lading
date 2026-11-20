---
slug: unix-stream-partial-write-shutdown
subsystem: lifecycle/shutdown
assertion_type: Always
priority: P0
needs_target: true
needs_sut_fix: shutdown branch in partial-write and connect loops; bounded readiness instead of bare yield_now
---

# unix_stream partial-write loop is shutdown-aware and not a busy-spin

## Statement

When the receiver's socket buffer is full, the unix_stream inner partial-write loop neither
busy-spins on `WouldBlock` at 100% CPU nor ignores shutdown; it yields to shutdown and bounds
its retry.

## Code evidence trail

- `unix_stream.rs:281-318` — the inner `while blk_offset < blk_max` loop has no shutdown branch
  and busy-spins via `yield_now()` (at `:302`) when `try_write` returns `WouldBlock`.
- The code comment admits: "If the read side has hung up we will never know and will keep
  attempting to write."
- `unix_stream.rs:248-268` — the connect loop also lacks a shutdown check.

## Assertion-type rationale

**Always** (bounded shutdown + no livelock): under a stalled/slow receiver every timeline must
still exit within the bound and must not peg a core.

## Mode / observability

With a stalled/slow unix receiver, lading exits within `max_shutdown_delay` and does not peg a
core. Oracle: slow-receiver scenario + external exit timer and CPU observation.

## Mechanism

Add a shutdown branch to the `while blk_offset < blk_max` loop (`unix_stream.rs:281-318`) and
to the connect loop (`:248-268`); replace the bare `yield_now` spin with a bounded/awaited
readiness.

## needs_sut_fix

Yes. Both the partial-write inner loop and the connect loop need shutdown awareness; the
`WouldBlock` busy-yield must become an awaited readiness.

## Investigation Log

- [2026-07-24] Confirmed live on main: the inner loop busy-spins on `WouldBlock` and the
  comment explicitly acknowledges the read-side-hangup blind spot.
- [2026-07-24] Related but distinct: `unix-stream-write-error-progress` covers the
  non-BrokenPipe write-error busy loop; this property is the shutdown + CPU angle.

## Open Questions

- Is the undivided per-connection throttle in unix_stream intentional (separate property
  `unix-throttle-aggregate-consistent`)? Not relevant to shutdown but shares the file.
