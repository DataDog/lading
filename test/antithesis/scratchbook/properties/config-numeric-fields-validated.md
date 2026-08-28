---
slug: config-numeric-fields-validated
subsystem: blackhole/config
assertion_type: Always
priority: P2
needs_target: false
needs_sut_fix: add range checks in config.rs (compression_level 1-22, sample_period_milliseconds > 0)
---

# Numeric config fields are validated at load, not deferred to runtime failure

## Statement

parquet/zstd `compression_level` (1-22) and `sample_period_milliseconds` (>0) are range-checked
at config load, so an out-of-range value fails early rather than corrupting a capture write or
driving a tight sampling loop.

## Code evidence trail

- `config.rs:186-198` — `compression_level: i32` (default 3) with doc "1-22" but no range check;
  an out-of-range value surfaces only as a zstd error during capture writing -> footer-less
  unreadable parquet on failure.
- `config.rs:106-107` — `sample_period_milliseconds: u64` has no lower-bound check; 0 could
  drive a tight observer sampling loop.
- Config validation is centralized here and is the intended crash-early location (module
  docstring `:1-3`).

## Assertion-type rationale

**Always** (validate-early): on every timeline an out-of-range numeric field must be rejected at
startup, not surfaced as a mid-run failure. Oracle: out-of-range-config scenario asserting a
clean startup error.

## Mode / observability

An out-of-range `compression_level` or a zero `sample_period` is rejected at startup (the
documented crash-early location), not surfaced as a mid-run zstd error / busy loop.

## Mechanism

Add range checks in `config.rs:186-198` (`compression_level`) and `:106-107`
(`sample_period_milliseconds > 0`).

## needs_sut_fix

Yes.

## Investigation Log

- [2026-07-24] Confirmed: these numeric fields have no validation; they are validation gaps, not
  by-design deferral, since config.rs is the intended crash-early location.

## Open Questions

- None specific.
