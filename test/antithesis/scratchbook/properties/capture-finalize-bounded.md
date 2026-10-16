---
slug: capture-finalize-bounded
subsystem: lifecycle/shutdown
assertion_type: Always
priority: P1
needs_target: false
needs_sut_fix: add a timeout around the capture-manager join await
---

# Capture finalize await is time-bounded

## Statement

The capture-manager join await during shutdown is bounded; a stalled parquet footer write (slow
volume, disk-full) cannot make lading hang before `runtime.shutdown_timeout` can act.

## Code evidence trail

- `lading.rs:713-715` — unbounded `let _ = handle.await;` on the capture-manager join.
- `lading.rs:813` — `runtime.shutdown_timeout` is only reached AFTER `inner_main` returns, so
  it cannot backstop a hang inside this await.
- Finalize path: `lading_capture/src/manager.rs:397-412`, `parquet.rs:318-324` (footer write).

## Assertion-type rationale

**Always** (bounded latency): on every timeline finalize must complete or be timed out so
`runtime.shutdown_timeout` remains the backstop.

## Mode / observability

With a slow/stalled capture volume, lading still exits within `max_shutdown_delay`. Oracle:
slow-disk fault scenario + exit timer.

## Mechanism

Add a timeout around `let _ = handle.await;` (`lading.rs:713-715`) so
`runtime.shutdown_timeout` remains the backstop.

## needs_sut_fix

Yes.

## Investigation Log

- [2026-07-24] Confirmed the ordering hazard: `runtime.shutdown_timeout(30s)` at `lading.rs:813`
  cannot help because it is only reached after this await completes.
- [2026-07-24] Related to `capture-write-failure-not-abort`: a mid-run write error is currently
  a SIGABRT, not a hang; this property covers the stall-without-error case.

## Open Questions

- None specific.
