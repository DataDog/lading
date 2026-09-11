---
slug: arbitrary-block-nonzero-no-panic
subsystem: no-panic
assertion_type: Unreachable
priority: P2
needs_target: false
needs_sut_fix: return Err(arbitrary::Error::IncorrectFormat) instead of .expect at block.rs:119-127
---

# Fuzz Arbitrary Block construction handles zero total_bytes without panic

## Statement

The `arbitrary::Arbitrary` impl for `Block` handles a generated `total_bytes` of 0 without
`.expect` panicking, so a fuzz run is not aborted by benign input.

## Code evidence trail

- `block.rs:119-127` — `total_bytes = u32::arbitrary(u)?` then
  `NonZeroU32::new(total_bytes).expect("total_bytes must be non-zero")`. `u32::arbitrary` can
  yield 0, panicking. Only compiled under the `arbitrary` feature (fuzz harness).

## Assertion-type rationale

**Unreachable** within the fuzz harness: the `NonZeroU32` `.expect` must never be reached.
Oracle: `cargo fuzz` run.

## Mode / observability

A fuzz input yielding `total_bytes==0` is rejected gracefully (`arbitrary::Error`) not a
`NonZeroU32` expect panic. Only compiled under the `arbitrary` feature (fuzz harness), so no
production impact.

## Mechanism

Return `Err(arbitrary::Error::IncorrectFormat)` instead of `.expect` at `block.rs:119-127`.

## needs_sut_fix

Yes (fuzz-harness-only; no production no-panic impact).

## Investigation Log

- [2026-07-24] Confirmed: compiled only under the `arbitrary` feature, so it aborts a fuzz run
  rather than affecting production, but it is still a benign-input abort worth fixing.

## Open Questions

- None specific.
