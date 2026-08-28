---
slug: lading-completes-and-exits-cleanly
subsystem: lifecycle/shutdown
assertion_type: Sometimes
priority: P0
needs_target: false
needs_sut_fix: none
---

# lading self-terminates on its experiment timer and exits 0 (non-vacuity)

## Statement

On the happy path lading owns the clock: it reaches experiment end, drains capture, and exits 0
with a readable capture at least once.

## Code evidence trail

- `lading.rs` — `experiment_sequence` + the graceful path (self-termination on the experiment
  timer).

## Assertion-type rationale

**Sometimes** (liveness non-vacuity): the good shutdown path must be reachable on at least one
timeline, guarding against a regression that makes every timeline hang or abort.

## Mode / observability

A run reaches clean self-termination with exit 0 and a footer-complete capture. `reachable!`
anchor for shutdown coverage. Oracle: external exit-code + capture-readable check; SUT
`reachable!` at clean exit.

## Mechanism

Sometimes non-vacuity: at least one timeline must reach exit 0 with a readable capture,
otherwise every shutdown-safety Always property would be vacuously true while lading in fact
never shuts down cleanly.

## needs_sut_fix

None.

## Investigation Log

- [2026-07-24] Deployment context: this is the production success path — lading self-exits 0,
  writes the parquet footer, and RJO copies the capture off before force-killing the pod.
- [2026-07-24] Complements the P0 Always shutdown properties by ensuring the clean-exit path is
  actually exercised (non-vacuity floor).

## Open Questions

- None specific.
