---
slug: observer-cpu-max-parse-no-panic
subsystem: observer
assertion_type: Unreachable
priority: P0
needs_target: true
needs_sut_fix: bounds-check cpu.max parsing / guard zero period
---

# Observer never panics on malformed/truncated cpu.max

## Statement

`parse_allowed_cores` is bounds-checked (no index panic, guards a zero period) so a
malformed/truncated cgroup `cpu.max` degrades rather than aborting.

## Code evidence trail

- `stat.rs` — the `cpu.max` parse path (`parse_allowed_cores`); a truncated/malformed read can
  index-panic or divide by a zero period.

## Assertion-type rationale

**Unreachable**: the index/divide panic must never be reached. Oracle: malformed-cpu.max fault
+ panic hook.

## Mode / observability

A truncated `cpu.max` read (fault-injected) does not SIGABRT the run.

## Mechanism

Bounds-check parsing / guard a zero period.

## needs_sut_fix

Yes. Fix `6aa1b1ba` on backup branch; live on main.

## Investigation Log

- [2026-07-24] Same fix commit (`6aa1b1ba`) covers both the PID-reuse assert and the cpu.max
  parse; both are live on main.
- [2026-07-24] Antithesis fault injection can truncate the cgroup `cpu.max` read to trigger this.

## Open Questions

- None specific.
