---
slug: tcp-rr-listener-no-panic
subsystem: no-panic
assertion_type: Unreachable
priority: P1
needs_target: false
needs_sut_fix: return Result from create_listener instead of expect/panic
---

# tcp_rr blackhole returns an error, not a panic, on bind failure

## Statement

tcp_rr blackhole listener setup failures (address in use, stale bind) return `Error::Bind`
rather than panicking, including the `threads>1` thread-0 prebuild path on the main async task.

## Code evidence trail

- `tcp_rr.rs:345` — `.unwrap_or_else(|e| panic!("failed to bind to {binding_addr}: {e}"))`.
- `tcp_rr.rs:346` — `.expect("failed to listen")`; plus setup at `:313-327`.
- `tcp_rr.rs:179` — for `num_threads>1`, `create_listener(0,..)` is invoked directly in async
  `run()` (thread-0 pre-build), so a bind failure panics the blackhole task rather than
  returning `Error::Bind`.

## Assertion-type rationale

**Unreachable**: the `expect`/`panic!` on listener setup must never be reached. Oracle:
pre-bound-port scenario with `threads>1` + panic hook.

## Mode / observability

A pre-bound data port yields a clean error, not a SIGABRT. Currently `create_listener` uses
`expect()`/`panic!` (`tcp_rr.rs:345-346`) and thread-0 prebuild runs in async `run()` at `:179`.

## Mechanism

Return `Result` from `create_listener` (`tcp_rr.rs:313-346`) instead of `expect`/`panic`.

## needs_sut_fix

Yes.

## Investigation Log

- [2026-07-24] Confirmed: for the worker-thread path a panic is converted to
  `Error::ThreadPanicked` via the ready channel, but the thread-0 prebuild path and the
  socket-setup expects are hard panics on the main async task.

## Open Questions

- Is tcp_rr ever configured with `threads>1` in the harness/deployment? That is the path where
  the bind failure panics the main async task rather than degrading to `ThreadPanicked`.
