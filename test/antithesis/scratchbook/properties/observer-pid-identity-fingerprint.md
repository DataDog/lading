---
slug: observer-pid-identity-fingerprint
subsystem: observer
assertion_type: Always
priority: P2
needs_target: true
needs_sut_fix: capture a start-time/identity fingerprint (proc start_time) and validate it in the pidfd/sampler paths
---

# Observer samples the identified target, not a PID-reuse impostor

## Statement

lading reports target-exit iff the process identified at startup exits; after a watched PID is
recycled, the observer/pidfd path does not silently attach to an unrelated process and emit
wrong metrics.

## Code evidence trail

- `target.rs:302-332` — PID mode watch: `kill(pid,0)` validity check (`:314`) and
  `AsyncPidFd::from_pid` (`:327`) are separate steps; the process can vanish and its PID be
  recycled in between (TOCTOU).
- `target.rs:246-257` — `watch_container` pidfd path.
- observer/linux sampler reads `/proc/{pid}` and would attach to an impostor after reuse.
- `PID_MAX` is small (`2^22`), so reuse is realistic on busy hosts.

## Assertion-type rationale

**Always** (metric integrity): on every timeline metrics attributed to the target must
correspond to the original process identity.

## Mode / observability

Metrics attributed to the target correspond to the original process identity; a recycled PID
does not produce plausible-but-wrong metrics with no error. Oracle: PID-reuse scenario checking
metric identity.

## Mechanism

Capture a start-time/identity fingerprint (proc `start_time`) and validate it in the
pidfd/sampler paths (`target.rs:302-332`, observer sampler).

## needs_sut_fix

Yes (defensive; a lead, TOCTOU + small PID_MAX).

## Investigation Log

- [2026-07-24] Distinct from `observer-pid-reuse-no-panic`: that one is the abort on mismatch;
  this is the silent-wrong-metrics case where no assert catches an impostor attachment.

## Open Questions

- Should lading capture a start-time/identity fingerprint to defend the observer sampler and
  pidfd path against PID reuse?
