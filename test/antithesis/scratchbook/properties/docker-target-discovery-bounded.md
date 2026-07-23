---
slug: docker-target-discovery-bounded
subsystem: lifecycle/shutdown
assertion_type: Always
priority: P0
needs_target: true
needs_sut_fix: add shutdown.recv() arm and/or max-attempts timeout to watch_container loop
---

# Container-target discovery is bounded and shutdown-aware

## Statement

In container/observer mode, the target-container discovery poll loop either finds the
container, times out with an error, or responds to shutdown; it never spins forever so the
experiment timer can start.

## Code evidence trail

- `target.rs:212-244` — the `watch_container()` poll loop has only a container-found break plus
  `sleep(1s)`; no `shutdown.recv` arm, no max-attempts timeout.
- `lading.rs:632-641` — `experiment_sequence` awaits `target_running` before arming the warmup
  and experiment timers, so if the container never appears the timer never starts.
- Contrast PID mode (`target.rs:314-317`) which validates once and errors immediately (bounded).

## Assertion-type rationale

**Always** (bounded startup): on every timeline the discovery loop must terminate (found,
error, or shutdown) so lading can reach self-termination.

## Mode / observability

With a misnamed/never-started target container, lading either errors out bounded or
self-terminates on its timer; it does not block `experiment_sequence` at
`target_running_watcher.recv()` forever. Oracle: wrong `--target-container` name scenario;
assert lading exits (error or timer) within the watchdog.

## Mechanism

Add a `shutdown.recv()` arm and/or a max-attempts timeout to the `target.rs:212-244`
`watch_container` loop.

## needs_sut_fix

Yes.

## Investigation Log

- [2026-07-24] This is the PRODUCTION observer path: the deployment launches lading with
  `--target-container target` and a bind-mounted docker socket, so this loop runs in real
  deployments. High impact.
- [2026-07-24] If discovery never completes, the experiment timer never starts, so lading never
  self-terminates — only ctrl_c could end it. Violates the "lading owns the clock / bounded
  startup" invariant. Strong deployment lead.

## Open Questions

- None specific; strongly deployment-grounded.
