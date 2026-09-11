---
slug: parquet-footer-on-graceful-exit
subsystem: capture
assertion_type: Always
priority: P0
needs_target: false
needs_sut_fix: none on the pure graceful path (depends on sigterm-graceful-drain + capture-write-failure-not-abort)
---

# Graceful exit always yields a readable parquet capture

## Statement

After any graceful termination (experiment timer or SIGTERM) the parquet/multi capture file has
a finalized footer and is fully readable.

## Code evidence trail

- `parquet.rs:307-324` — the footer is written only in `close()`; doc: "Without calling close()
  the file will be incomplete and unreadable".
- `state_machine.rs:249-259` — `close()` is only reached on `Event::ShutdownSignaled`.

## Assertion-type rationale

**Always**: every graceful exit must produce a readable file. Oracle is the existing
`anytime_capture_consistent` checker's parquet arm (readable => internally consistent) plus a
post-graceful-exit readability assertion.

## Mode / observability

External reader opens `/captures/captures.parquet` post-exit and parses all row groups. This is
the operational invariant the deployment depends on (watchdog/cancel/kill => unreadable =>
capture loss).

## Mechanism

None on the pure graceful path; depends on `sigterm-graceful-drain` and
`capture-write-failure-not-abort` holding.

## needs_sut_fix

Indirect: this property holds on the pure graceful path as-is, but its practical guarantee
depends on `sigterm-graceful-drain` (so SIGTERM takes the graceful path) and
`capture-write-failure-not-abort` (so a write error does not abort mid-flush).

## Investigation Log

- [2026-07-24] Existing harness oracle: `anytime_capture_consistent.rs:66` asserts
  `!readable || invariants_hold` and `:71` `sometimes! readable && records>=10`. This property
  adds the post-graceful-exit readability assertion.
- [2026-07-24] Deployment: on the success path lading self-exits 0, writes the footer, then RJO
  copies the parquet off BEFORE force-killing the pod — so teardown SIGKILL is harmless because
  lading already finished.

## Open Questions

- Does the parquet writer periodically finalize a readable footer on `flush_seconds:60`, or
  only at graceful close? If only at close, EVERY non-graceful stop loses ALL captured data,
  not just the last 60s. (Digest orientation: footer only at close.)
