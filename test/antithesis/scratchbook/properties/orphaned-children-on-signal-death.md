---
slug: orphaned-children-on-signal-death
subsystem: lifecycle/shutdown
assertion_type: Always
priority: P1
needs_target: true
needs_sut_fix: install SIGTERM handler and signal the process group, not just the direct child pid
---

# No orphaned target/inspector children on signal-driven death

## Statement

lading reaps its Binary-target and inspector children (and their process groups) even when
killed by a signal, rather than relying solely on `kill_on_drop` which cannot fire on
untrapped-signal death.

## Code evidence trail

- `target.rs:392` `kill_on_drop(true)`; `:430-431` `kill(target_id, SIGTERM)` to a single pid.
- `inspector.rs:146` `kill_on_drop(true)`; `:175-176` SIGTERM to a single pid.
- `kill_on_drop` cannot fire when lading is killed by an untrapped signal (SIGTERM per
  `sigterm-graceful-drain`, or SIGKILL); children are reparented to init. The graceful path only
  signals the direct child, not the process group, so grandchildren are never reaped.

## Assertion-type rationale

**Always** (no-leak): on every signal-driven death, no orphaned target/inspector grandchildren
remain reparented to init.

## Mode / observability

After a SIGTERM (or crash) no orphaned target/inspector grandchildren remain reparented to init.
MOOT under Antithesis whole-container SIGKILL (all pids die together); LIVE in shared-PID-
namespace / bare-host deployments. Oracle: SIGTERM scenario in a shared PID namespace checking
for surviving children.

## Mechanism

Install a SIGTERM handler (see `sigterm-graceful-drain`) and send signals to the process group,
not just the direct child pid (`target.rs:430-431`, `inspector.rs:175-176`).

## needs_sut_fix

Yes.

## Investigation Log

- [2026-07-24] MOOT on the production observer path (target is a sibling container SIGKILLed at
  teardown; whole container dies together). LIVE in shared-PID-namespace or bare-host
  deployments where lading spawns a Binary target/inspector.

## Open Questions

- None specific.
