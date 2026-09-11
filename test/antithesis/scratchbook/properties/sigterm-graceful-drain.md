---
slug: sigterm-graceful-drain
subsystem: lifecycle/shutdown
assertion_type: Always
priority: P0
needs_target: false
needs_sut_fix: add SignalKind::terminate arm to main select
---

# SIGTERM finalizes capture like the experiment timer

## Statement

When lading receives SIGTERM it runs the same graceful path as experiment-timer
self-termination: broadcast shutdown, drain the capture maturity window, write the
parquet/multi footer, exit non-abnormally.

## Code evidence trail

- `lading/src/bin/lading.rs:658` — the main `select!` has only a `signal::ctrl_c()` (SIGINT)
  arm; grep finds no `tokio::signal::unix` `SignalKind::terminate` anywhere.
- With no SIGTERM arm, SIGTERM uses default disposition -> lading dies immediately (exit 143)
  with NO graceful path: `shutdown_broadcast` never fires, capture is never finalized, child
  destructors (`kill_on_drop`) never run.
- Graceful path for reference: `lading_signal` `signal_and_wait` -> capture finalize ->
  `runtime.shutdown_timeout`.

## Assertion-type rationale

**Always**: the graceful contract must hold every time a SIGTERM is delivered. The best oracle
is external — send SIGTERM mid-run, then validate the capture file is footer-complete and the
process exited without abort.

## Mode / observability

After a SIGTERM the on-disk parquet/multi capture is readable (footer present) and lading
exits 143/0, not abort. MOOT under Antithesis `node_termination` (SIGKILL is untrappable) but
LIVE on the deployment's orchestrator-stop path.

## Mechanism

Add a `tokio::signal::unix` `SignalKind::terminate` arm in the `lading.rs` main `select`
(alongside `ctrl_c` at :658) that triggers `shutdown_broadcast`.

## needs_sut_fix

Yes. Add the `SignalKind::terminate` arm to the main select that triggers the same
`shutdown_broadcast` as the experiment timer and ctrl_c.

## Investigation Log

- [2026-07-24] Confirmed live on main: only ctrl_c is trapped at `lading.rs:658`; no SIGTERM
  handler exists (grep).
- [2026-07-24] Deployment context: the production runner never sends lading SIGTERM — lading
  self-terminates on its timer or is SIGKILLed via `docker rm --force`. So this handler is
  effectively dead code on the production path, but the deployment tickets SMPTNG-719/697
  document ungraceful-termination telemetry loss, and a plain orchestrator `docker stop`
  (SIGTERM) would corrupt the parquet and orphan children today.

## Open Questions

- Does Antithesis ever deliver SIGTERM (vs only SIGKILL `node_termination`)? If not, this is
  exercised only by an explicit harness `kill -TERM` step.
