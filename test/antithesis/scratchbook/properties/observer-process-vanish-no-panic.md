---
slug: observer-process-vanish-no-panic
subsystem: observer
assertion_type: Unreachable
priority: P0
needs_target: true
needs_sut_fix: yield empty iterator instead of panic! at process_descendents.rs:13
---

# Observer never panics when a target vanishes mid-listing

## Statement

`ProcessDescendantsIterator` degrades to zero descendants when `Process::new` fails, rather than
panicking, when a target exits before descendant listing.

## Code evidence trail

- `process_descendents.rs:13` — `panic!` when `Process::new` fails, instead of yielding an
  empty iterator.

## Assertion-type rationale

**Unreachable**: the `panic!` must never be reached. Oracle: target-exits-during-sampling
scenario + panic hook.

## Mode / observability

A target exiting mid-listing does not crash the run.

## Mechanism

Yield an empty iterator (degrade to zero descendants) instead of `panic!` at
`process_descendents.rs:13`.

## needs_sut_fix

Yes. Fix `7e1d2968` on backup branch; live on main.

## Investigation Log

- [2026-07-24] Confirmed live on main: `ProcessDescendantsIterator::new` panics when
  `Process::new` fails, so a target that exits before descendant listing crashes the run.

## Open Questions

- None specific.
