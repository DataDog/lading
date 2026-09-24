---
slug: target-wait-bounded
subsystem: lifecycle/shutdown
assertion_type: Always
priority: P0
needs_target: true
needs_sut_fix: wrap target/inspector child wait() in a timeout that escalates to SIGKILL
---

# Post-SIGTERM target/inspector wait is time-bounded

## Statement

After lading SIGTERMs a Binary target (or inspector), the `wait()` for the child to exit is
bounded (does not depend solely on external JoinSet-abort/runtime timeout); a SIGTERM-ignoring
target does not hang lading.

## Code evidence trail

- `target.rs:432` — bare `target_child.wait().await` after SIGTERM, no timeout of its own.
- `inspector.rs:176` — bare inspector `wait().await`.
- `target.rs:430-431`, `inspector.rs:175` — SIGTERM sent to a single pid (see
  `orphaned-children-on-signal-death`).
- Safety currently depends on the external JoinSet-abort / `runtime.shutdown_timeout` backstop.

## Assertion-type rationale

**Always** (locally-bounded reap): the reap must be bounded on every timeline, not contingent
on unrelated shutdown wiring holding.

## Mode / observability

With a target that ignores SIGTERM, lading still exits within `max_shutdown_delay`. MOOT on the
deployment's container-observer path (target is a sibling container force-killed by the
runner), LIVE for Binary-target mode. Oracle: SIGTERM-ignoring-target scenario + exit timer.

## Mechanism

Wrap `target_child.wait()` (`target.rs:432`) and the inspector wait (`inspector.rs:176`) in a
timeout that escalates to SIGKILL before returning.

## needs_sut_fix

Yes. The waits are untimed today.

## Investigation Log

- [2026-07-24] Deployment context: production uses Container observer mode (`--target-container`),
  NOT Binary target mode, so lading does not spawn/SIGTERM/reap the target on the production
  path — the target is a sibling container RJO hard-kills. This property is LIVE only for
  Binary-target mode (e.g. local dev / other deployments).
- [2026-07-24] Open orientation question flagged: whether tokio's JoinSet-drop abort reliably
  runs `kill_on_drop` SIGKILL before `runtime.shutdown_timeout`, or a target can survive the
  gap.

## Open Questions

- Does JoinSet-drop abort of the target task reliably fire `kill_on_drop` before
  `runtime.shutdown_timeout`, or can a SIGTERM-ignoring target survive the gap?
