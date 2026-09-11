---
slug: target-grace-period-honored
subsystem: lifecycle/shutdown
assertion_type: Always
priority: P1
needs_target: true
needs_sut_fix: join the target task with a max_shutdown_delay-bounded wait after signaling, instead of dropping the JoinSet immediately
---

# A cooperative slow target gets its full post-SIGTERM cleanup window

## Statement

On graceful shutdown the target receives SIGTERM and is given up to `max_shutdown_delay` to
clean up/flush before SIGKILL, rather than being SIGKILLed within milliseconds when capture
flush returns.

## Code evidence trail

- `lading.rs:690-717` — on graceful/ctrl_c shutdown the main loop breaks, then `inner_main` runs
  `signal_and_wait` + capture flush and RETURNS without ever joining `tsrv_joinset`. Returning
  drops the JoinSet, aborting the still-running target task at `target_child.wait().await`;
  dropping `target_child` triggers `kill_on_drop` -> SIGKILL.
- `target.rs:392` `kill_on_drop(true)`; `:425-433` SIGTERM then wait (comment "give the child a
  chance to clean up").
- `max_shutdown_delay` is only applied later at `runtime.shutdown_timeout`, by which point the
  target is already killed.

## Assertion-type rationale

**Always** (grace contract): on every graceful shutdown a cooperative target must get its full
cleanup window before SIGKILL.

## Mode / observability

A cooperative-but-slow target completes its cleanup/artifact flush before being killed.
Currently `inner_main` returns after capture flush, dropping `tsrv_joinset` and aborting
`target_child.wait` -> `kill_on_drop` SIGKILL. Oracle: SIGTERM-then-slow-cleanup target scenario
checking the target's artifacts are complete.

## Mechanism

Join the target task with a `max_shutdown_delay`-bounded wait after signaling, instead of
dropping the JoinSet immediately (`lading.rs:690-717`).

## needs_sut_fix

Yes.

## Investigation Log

- [2026-07-24] Confirmed the ~0 grace: `inner_main` returns after capture flush (milliseconds),
  dropping the JoinSet and triggering `kill_on_drop` SIGKILL long before
  `runtime.shutdown_timeout` could gate the cleanup window.
- [2026-07-24] MOOT on the production observer path (target is a sibling container), LIVE for
  Binary-target mode. Confirm intended grace semantics before proposing a fix.

## Open Questions

- Is the ~0 grace intentional, or is `max_shutdown_delay` meant to gate the target's post-SIGTERM
  cleanup window?
