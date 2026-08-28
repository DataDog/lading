---
slug: unix-datagram-blackhole-removes-stale-socket
subsystem: blackhole/config
assertion_type: Always
priority: P1
needs_target: true
needs_sut_fix: await the remove_file future in unix_datagram.rs:95
---

# unix_datagram blackhole removes a stale socket before bind

## Statement

The unix_datagram blackhole actually removes a leftover socket file before binding, so it starts
cleanly after a hard-kill restart instead of failing bind with EADDRINUSE.

## Code evidence trail

- `blackhole/unix_datagram.rs:95` — `let _res = tokio::fs::remove_file(&self.path).map_err(
  Error::Io);` uses `futures::TryFutureExt::map_err`, returning a lazy `MapErr` future that is
  bound to `_res` and dropped without `.await`, so `remove_file` is never polled/executed.
- The comment at `:93-94` ("Delete the socket, ignore any errors") states the intended behavior,
  which is defeated. `bind()` then runs at `:96` against a possibly-existing path.

## Assertion-type rationale

**Always** (restart resilience): on every timeline the blackhole must bind cleanly even after a
hard-kill left a stale socket.

## Mode / observability

After a `node_termination` leaving a stale socket, the blackhole binds successfully and the
target keeps its sink. Oracle: restart-with-stale-socket scenario asserting bind succeeds.

## Mechanism

Await the `remove_file` future in `unix_datagram.rs:95` (drop the lazy `TryFutureExt::map_err`
binding).

## needs_sut_fix

Yes.

## Investigation Log

- [2026-07-24] Verified against source this turn: the `remove_file` future is built and dropped,
  never awaited; binding to `let _res =` also suppresses the unused-future lint.

## Open Questions

- Is the unix socket path on a persisted named volume in the deployment? If so, a never-removed
  stale socket guarantees bind failure on every SIGKILL restart, not just occasionally.
