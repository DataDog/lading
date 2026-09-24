---
slug: capture-write-failure-not-abort
subsystem: capture
assertion_type: Unreachable
priority: P0
needs_target: false
needs_sut_fix: replace .expect on capture start with Result propagation; plumb mid-run write errors to graceful shutdown
---

# A capture write error is a clean fatal exit, not a process abort

## Statement

A transient capture write/flush error (disk full, EIO) causes a clean non-zero exit with the
parquet footer flushed, not a `panic=abort` that SIGABRTs the whole run mid-flush and leaves an
unreadable file.

## Code evidence trail

- `lading.rs:476`, `:501`, `:526` — `.block_on(capture_manager.start()).expect("failed to
  start capture manager")`.
- `manager.rs:409` — `state_machine.next(event)?` propagates format errors up.
- `state_machine.rs:317`/`:343` `flush()?`, `:396` `write_metric()?` — any IO error becomes an
  `Err` that hits the `.expect`.
- `Cargo.toml:115,120` — `panic = "abort"` turns the expect into a SIGABRT that skips
  `format.close()` (the parquet footer).

## Assertion-type rationale

**Unreachable** on the abort site: a capture error must never reach the abort. The oracle is
the IO-fault-injection scenario + the panic hook (must stay silent) + an external
parquet-readability check.

## Mode / observability

On an injected capture-write IO error the panic hook must NOT fire and the parquet must be
readable. Currently the `.expect` + `panic=abort` turns any flush-tick error into a SIGABRT
that skips `format.close()`.

## Mechanism

Replace the `.expect` on capture start (`lading.rs:476/501/526`) with `Result` propagation to
`Error::CaptureManager` so `BufWriter`s flush on Drop; plumb mid-run write errors to graceful
shutdown.

## needs_sut_fix

Yes. Partly landed on backup branch (`b7624af2`), not on main. Note: `b7624af2` is
fatal-on-startup only; the mid-run write error still needs graceful shutdown plumbing.

## Investigation Log

- [2026-07-24] Confirmed live on main: the `.expect` + `panic=abort` path means a single mid-run
  IO hiccup both kills the experiment AND leaves the parquet footer-less/unreadable, because
  `format.close()` (`state_machine.rs:255`) only runs on `Event::ShutdownSignaled`, which the
  abort bypasses.
- [2026-07-24] Deployment relevance: production captures are parquet-only; an unreadable parquet
  = total capture loss for the replicate (same failure mode as watchdog SIGKILL).

## Open Questions

- Is a mid-run capture-write failure intended to be fatal to the whole experiment? If yes it
  should be a clean abort with a footer-flushed parquet; if no, capture errors should be logged
  and the run continue. Current behavior (SIGABRT + unreadable parquet) is neither.
