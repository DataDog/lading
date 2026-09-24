---
slug: datadog-blackhole-accept-resilient
subsystem: blackhole/config
assertion_type: Always
priority: P1
needs_target: true
needs_sut_fix: match accept() and continue on Err at datadog.rs:209
---

# Datadog blackhole keeps accepting after a transient accept error

## Statement

The Datadog blackhole's accept loop logs-and-continues on an `accept()` error and never wedges
(silently ceasing to accept while appearing alive), so it never backpressures the target.

## Code evidence trail

- `datadog.rs:207-212` — the `tokio::select!` branch uses a fallible pattern
  `Ok((stream, _addr)) = listener.accept()` with only a shutdown branch and no `else`. On an
  `accept()` `Err` (EMFILE/ENFILE fd exhaustion, ECONNABORTED) the pattern fails to match, tokio
  disables that branch, and `select!` blocks on `shutdown_wait` forever; accept is never re-armed.
- Contrast `common.rs:89-96` which does `match incoming { Ok=>.., Err(e)=>{ error!; continue }}`
  (resilient).

## Assertion-type rationale

**Always** (blackhole never-backpressure): on every timeline, including fd-exhaustion timelines,
the blackhole must keep serving new connections.

## Mode / observability

Under fd exhaustion / transient accept errors the blackhole keeps serving new connections.
Confirmed live: the fallible select arm with no `else` disables the branch on `Err`, then blocks
on shutdown forever. Oracle: fd-exhaustion fault scenario asserting the target's connections
still succeed.

## Mechanism

Match `accept()` and `continue` on `Err` (as `common.rs:89-96` does) at `datadog.rs:209`.

## needs_sut_fix

Yes.

## Investigation Log

- [2026-07-24] Verified against source this turn: datadog is the buggy outlier — it appears
  alive but stops serving, so the target's connections stall = backpressure, violating the
  blackhole "never backpressure the target" invariant.
- [2026-07-24] Accept-error handling is inconsistent across blackholes (common.rs resilient;
  tcp/udp/unix_stream die; datadog wedges) — worth a single policy decision.

## Open Questions

- Does lading's supervisor treat a blackhole `run()` returning `Err` (tcp/udp/unix_stream
  accept error) as graceful shutdown or hard failure? Determines whether the divergence causes
  rig termination vs silent target backpressure.
