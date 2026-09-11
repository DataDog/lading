---
slug: linear-ramp-slope-preserved
subsystem: generators/throttle
assertion_type: Always
priority: P0
needs_target: true
needs_sut_fix: divide rate_of_change by divisor in the Linear divide arm
---

# Linear throttle aggregate ramp slope equals configured rate_of_change

## Statement

With a Linear throttle and `parallel_connections>1`, the aggregate ramp slope across workers
equals the single configured `rate_of_change`, not N times it.

## Code evidence trail

- `lib.rs:178-193` — the Linear `divide` arm splits capacities but passes `rate_of_change: rate`
  unchanged, so N parallel workers each ramp at the full rate -> aggregate `N*rate`.
- The call-site comment claims rate preservation, but the aggregate is `N*rate`.

## Assertion-type rationale

**Always** (rate fidelity): on every timeline the aggregate ramp slope must equal the single
configured `rate_of_change`. Pure proptest measuring aggregate slope; or Antithesis oracle
sampling sink rate over the warmup ramp.

## Mode / observability

Aggregate delivered-rate ramp reaches max in the configured time, not `1/N` of it. Confirmed
live: `divide()` divides capacities but passes rate unchanged (`rate_of_change: rate`) so N
workers each ramp at full rate.

## Mechanism

Divide `rate_of_change` by `divisor` in the `lib.rs` Linear `divide` arm (the call-site comment
claims preservation but the aggregate is `N*rate`).

## needs_sut_fix

Yes. Demonstrated on backup branch (`914bb14a`); live on main.

## Investigation Log

- [2026-07-24] Verified against source this turn (`lib.rs:178-193`): capacities divided,
  `rate_of_change` unchanged.
- [2026-07-24] Contradicts the call-site comment; a subtle rate-fidelity regression the harness
  can catch by sampling the warmup ramp slope.

## Open Questions

- None specific.
