---
slug: shutdown-completes-bounded
subsystem: lifecycle/shutdown
assertion_type: Always
priority: P0
needs_target: true
needs_sut_fix: shutdown branches in every connect/retry loop + timeout on child wait and capture finalize
---

# lading exits within max_shutdown_delay after shutdown is signaled

## Statement

Once the experiment timer fires (or a shutdown signal is broadcast) lading terminates within
`max_shutdown_delay`; it never hangs in `Server::spin` waiting on a worker that cannot observe
shutdown.

## Code evidence trail

- Generator connect/retry loops with no shutdown branch: `tcp.rs:232-251`, `udp.rs`,
  `unix_stream.rs:248-268`, `unix_datagram.rs:246-264`, `grpc.rs:287-299`.
- `target.rs:432` and `inspector.rs:176` — bare `.await` on child `wait()` (untimed).
- `lading.rs:713-715` — unbounded `let _ = handle.await;` on capture finalize before
  `runtime.shutdown_timeout` at `:813`.
- `lading.rs` — `max_shutdown_delay` (30s) feeds `runtime.shutdown_timeout`.

## Assertion-type rationale

**Always** (bounded-latency invariant on every timeline): once shutdown is signaled, exit must
follow within `max_shutdown_delay`. Umbrella liveness for shutdown hangs; per-loop properties
carry the specific mechanisms.

## Mode / observability

Wall time from shutdown-broadcast to process exit `<= max_shutdown_delay` (30s). Violation =
overrun -> deployment watchdog SIGKILL -> unreadable parquet -> total capture loss for the
replicate. Oracle: external timer measuring shutdown-signal->exit, or Antithesis liveness
"eventually exits".

## Mechanism

Add shutdown branches to every pre-select connect/retry loop (tcp/udp/unix_stream/
unix_datagram/grpc) and a timeout to `target_child.wait()` and the capture-finalize await; see
the per-loop child properties.

## needs_sut_fix

Yes — this umbrella is satisfied by the combined per-loop fixes plus the bounded child-wait
and bounded capture-finalize timeouts.

## Investigation Log

- [2026-07-24] Deployment watchdog makes this an operational invariant: RJO arms
  `ceil((warmup + total_samples + 30) * 1.2)` seconds; overrunning lading is SIGKILLed
  mid-run, leaving a footer-less unreadable parquet = total capture loss for the replicate.
- [2026-07-24] Strongest deployment-confirmed lead: SMPTNG-725 "RJO alive but not really" —
  hang-in-spin after "lading shutdown".

## Open Questions

- Does the graceful-shutdown driver in `lading.rs` impose a timeout on generator
  `Server::spin()` completion, or does it rely solely on `runtime.shutdown_timeout(30s)`? That
  determines whether the connect-loop hangs stall shutdown up to 30s or indefinitely.
