---
slug: grpc-connect-loop-shutdown
subsystem: lifecycle/shutdown
assertion_type: Always
priority: P1
needs_target: true
needs_sut_fix: add shutdown branch to grpc connect loop
---

# gRPC connect loop observes shutdown

## Statement

The gRPC generator's initial connect loop polls shutdown; a never-available target does not
wedge the generator.

## Code evidence trail

- `grpc.rs:287-299` — the initial connect loop retries `connect()` -> `sleep(100ms)` with no
  shutdown check; if the target never comes up the generator spins in the pre-select connect
  loop and cannot be shut down gracefully.

## Assertion-type rationale

**Always** (bounded shutdown): every timeline that signals shutdown must reach exit within the
bound, including the never-up-target case.

## Mode / observability

Never-up target: lading exits within bound rather than spinning `connect()` -> `sleep(100ms)`.
Oracle: external exit-timer.

## Mechanism

Add a shutdown branch to the `grpc.rs:287-299` connect loop.

## needs_sut_fix

Yes.

## Investigation Log

- [2026-07-24] Confirmed live on main: gRPC connect loop has no shutdown check.
- [2026-07-24] Related: gRPC also ignores the throttle result (`grpc-honors-throttle`) and
  serializes requests, both separate properties.

## Open Questions

- None specific.
