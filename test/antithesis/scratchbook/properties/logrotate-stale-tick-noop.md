---
slug: logrotate-stale-tick-noop
subsystem: no-panic
assertion_type: Unreachable
priority: P1
needs_target: false
needs_sut_fix: make advance_time early-return on a stale tick in logrotate_fs/model.rs
---

# logrotate_fs treats a stale tick as a no-op, never a panic

## Statement

`Model::advance_time` treats a tick below current model time as a no-op early return (a benign
FUSE scheduling reorder), never asserting/panicking.

## Code evidence trail

- `logrotate_fs/model.rs` — `Model::advance_time` asserted `now >= self.now` and aborted; FUSE
  handlers sample the tick before taking the model lock, so two reordered ops present a stale
  tick (a benign scheduling race, no clock fault needed).

## Assertion-type rationale

**Unreachable**: the stale-tick assert must never fire. Oracle: concurrent FUSE op scenario +
panic hook.

## Mode / observability

Reordered FUSE ops presenting a stale tick do not crash the logrotate_fs generator. No clock
fault needed to trigger.

## Mechanism

Make `advance_time` early-return on a stale tick in `logrotate_fs/model.rs` (fix `220850e5` on
backup branch; live on main).

## needs_sut_fix

Yes. Fix `220850e5` on backup branch; live on main.

## Investigation Log

- [2026-07-24] Confirmed: model time only advances; a tick below current is a benign
  scheduling-reorder that currently panics rather than being a no-op.

## Open Questions

- None specific.
