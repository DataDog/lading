---
slug: per-generator-semaphore-no-panic
subsystem: no-panic
assertion_type: Unreachable
priority: P1
needs_target: true
needs_sut_fix: replace static CONNECTION_SEMAPHORE OnceCell with a per-instance Arc<Semaphore>; make hot-path expect stop the worker gracefully
---

# Two HTTP (or two Splunk-HEC) generators coexist without panic

## Statement

Each HTTP/Splunk-HEC generator instance owns its own connection semaphore; configuring two such
generators does not panic on a process-wide `OnceCell::set` and gives independent connection
limits.

## Code evidence trail

- `http.rs:37` + `http.rs:187-189` — `static CONNECTION_SEMAPHORE: OnceCell<Semaphore>` set via
  `.set(..).expect(..)`.
- `splunk_hec.rs:51` + `splunk_hec.rs:243-245` — same pattern.
- `config.rs:101` — `pub generator: Vec<generator::Config>`, so two Http generators is a valid
  config; the second `new()` panics on the second `OnceCell::set`.

## Assertion-type rationale

**Unreachable**: the `OnceCell` double-set panic must never be reached. Oracle: two-http-
generator scenario + panic hook.

## Mode / observability

A config with two `Http` generators starts without the second `new()` panicking; per-generator
concurrency limits are independent. Even short of panic, the shared semaphore makes the
per-generator concurrency limit wrong.

## Mechanism

Replace the static `CONNECTION_SEMAPHORE` `OnceCell` with a per-instance `Arc<Semaphore>` in
`http.rs:37/187-189` and `splunk_hec.rs:51/243-245`; make the hot-path
`.expect("semaphore closed")` stop the worker gracefully.

## needs_sut_fix

Yes. Landed on backup branch (`5f8c375e`); live on main.

## Investigation Log

- [2026-07-24] Confirmed live regression on main: only duplicate generator IDs are rejected, so
  two Http (or two SplunkHec) generators is an allowed config that panics at the second `new()`.

## Open Questions

- None specific.
