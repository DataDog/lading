---
slug: capture-drift-no-silent-gap
subsystem: capture
assertion_type: Always
priority: P2
needs_target: false
needs_sut_fix: flush between advance_tick iterations in the drift loop; optionally flag large fetch_index gaps in validate
---

# Drift correction does not silently drop unflushed intervals

## Statement

When the flush-tick advances multiple ticks after a scheduling stall, the accumulator does not
overwrite unflushed ring slots, so the capture has no invisible `fetch_index` gaps.

## Code evidence trail

- `state_machine.rs:219-232` — `handle_flush_tick` advances the accumulator `tick_drift` times
  in a loop with no flush between (`:229-231`).
- `accumulator.rs:466-481` — `advance_tick` overwrites ring slots.
- A stall `> ~60s` overwrites unflushed intervals and creates `fetch_index` gaps.
- `validate/jsonl.rs:123-131` — the validator only checks strict monotonicity, NOT contiguity,
  so gaps are invisible.

## Assertion-type rationale

**Always**: on every timeline the capture must retain all intervals; a silent gap is a defect.
Oracle: induce scheduling starvation (Antithesis) + validator extended to detect gaps.

## Mode / observability

Under `>60s` scheduling starvation the capture retains all intervals (no large `fetch_index`
gap). Gaps pass the current strict-monotonic validator, so loss is invisible today.

## Mechanism

Flush between `advance_tick` iterations in the `state_machine.rs:229-231` drift loop;
optionally have `validate` flag large `fetch_index` gaps.

## needs_sut_fix

Yes.

## Investigation Log

- [2026-07-24] Confirmed: the drift loop overwrites ring slots without an interleaved flush; the
  validator's strict-monotonic (not contiguous) check means the loss passes validation.
- [2026-07-24] Antithesis is well-suited to induce the `>60s` scheduling starvation that
  triggers multi-tick drift.

## Open Questions

- Under sustained scheduling starvation, does the drift path produce silently-gapped captures
  that still pass `validate_lines`, and should the validator flag large gaps as suspicious?
