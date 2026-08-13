---
slug: observer-target-pid-recv-no-panic
subsystem: no-panic
assertion_type: Unreachable
priority: P0
needs_target: true
needs_sut_fix: handle recv() Err and None with a returned error at observer.rs:114-120
---

# Observer returns an error, not a panic, when the target PID never arrives

## Statement

If a Binary target fails to spawn or exits before sending its PID, the observer returns an error
instead of `.expect("catastrophic failure")` panicking on the closed channel.

## Code evidence trail

- `observer.rs:114-120` — two `.expect()` on the `recv()` result and the `Option`.
- Triggered by `target.rs:395` (TargetSpawn error before send) or `:396` (ProcessFinished before
  send): the target's `tgt_snd` is dropped, `recv()` returns `Err(Closed)`, and `.expect`
  panics.
- The panic is only partly masked in main: `osrv_joinset.join_next()` gets `Err(JoinError)` and
  merely logs "Could not join the spawned observer task" (`lading.rs:675`).

## Assertion-type rationale

**Unreachable**: the closed-channel `.expect` must never be reached. Oracle: instant-exit/
bad-path target scenario + panic hook.

## Mode / observability

A bad target path / instant-exit target does not panic the observer task. Currently the
`recv()` `Err(Closed)` hits `.expect`.

## Mechanism

Handle `recv()` `Err` and `None` with a returned error at `observer.rs:114-120`.

## needs_sut_fix

Yes.

## Investigation Log

- [2026-07-24] Confirmed live on main: two `.expect` on the PID channel recv; a Binary target
  that fails to spawn or exits before sending its PID drops the sender and panics the observer.
- [2026-07-24] MOOT on the production observer path (Container mode gets the PID via the docker
  socket, not this channel), LIVE for Binary-target mode.

## Open Questions

- None specific.
