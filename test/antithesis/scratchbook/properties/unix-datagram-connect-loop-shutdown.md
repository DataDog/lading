---
slug: unix-datagram-connect-loop-shutdown
subsystem: lifecycle/shutdown
assertion_type: Always
priority: P0
needs_target: true
needs_sut_fix: add shutdown branch to unix_datagram connect/retry loop
---

# unix_datagram connect loop observes shutdown

## Statement

The unix_datagram initial connect loop polls shutdown; if the socket path never appears the
worker still shuts down within `max_shutdown_delay`.

## Code evidence trail

- `unix_datagram.rs:246-264` — the initial connect loop retries `connect()` -> `sleep(1s)`
  with no shutdown check; if the socket path is never available the worker never reaches the
  `select!` and shutdown hangs.

## Assertion-type rationale

**Always** (bounded shutdown): every timeline that signals shutdown must reach exit within the
bound, including the missing-socket-path case.

## Mode / observability

Missing-socket-path scenario: lading exits within bound rather than spinning
`connect()` -> `sleep(1s)` forever. Oracle: external exit-timer in a never-bound-socket
scenario.

## Mechanism

Add a shutdown branch to the `unix_datagram.rs:246-264` connect/retry loop.

## needs_sut_fix

Yes.

## Investigation Log

- [2026-07-24] Confirmed live on main: connect loop has no shutdown check.
- [2026-07-24] Feeds the `shutdown-completes-bounded` umbrella and the `unreachable-target`
  scenario (socket path that never binds).

## Open Questions

- None specific.
