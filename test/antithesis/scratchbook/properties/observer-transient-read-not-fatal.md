---
slug: observer-transient-read-not-fatal
subsystem: observer
assertion_type: Always
priority: P1
needs_target: true
needs_sut_fix: treat component reads as best-effort in observer/linux.rs sample()
---

# A transient observer read error does not kill the run

## Statement

A single transient procfs/cgroup/wss read error is best-effort (log + skip that component's
sample); it does not `?`-propagate and terminate the whole experiment.

## Code evidence trail

- `observer/linux.rs` — `sample()` `?`-propagates a component read error, killing the whole
  experiment.

## Assertion-type rationale

**Always** (liveness): on every timeline a single transient read error must not terminate the
run. Oracle: transient-read-fault scenario asserting the run continues to self-termination.

## Mode / observability

An injected transient read error yields a skipped sample + warning, not a dead run. Only
persistent problems show as repeated warnings + absent metrics.

## Mechanism

Treat component reads as best-effort in `observer/linux.rs` `sample()` (fix `30b86a71` on backup
branch; live on main).

## needs_sut_fix

Yes. `30b86a71` on backup branch; live on main.

## Investigation Log

- [2026-07-24] Confirmed: a single transient procfs/cgroup/wss read error `?`-propagates and
  kills the whole experiment on main.

## Open Questions

- None specific.
