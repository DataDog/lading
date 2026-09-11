---
slug: no-wall-clock-in-payloads
subsystem: payload/determinism
assertion_type: Always
priority: P2
needs_target: false
needs_sut_fix: none currently (guards against regressions)
---

# Payload timestamps are seed-derived and monotonic, never wall-clock

## Statement

Timestamps embedded in generated payloads (trace_agent, otel, templated_json) are derived from
the rng/config, never from the system wall clock, and are monotone within a stream.

## Code evidence trail

- `trace_agent/v04.rs:326,384` — rng-derived timestamps.
- `templated_json/generator.rs:170-196` — `Timestamp` is rng-derived.
- `block.rs` — `Instant` feeds only progress logging, never bytes.

## Assertion-type rationale

**Always**: on every timeline payload timestamps must be reproducible and monotone. Oracle:
replay under a perturbed clock (Antithesis clock control) and compare payload timestamps for
equality/monotonicity.

## Mode / observability

Payload timestamps are reproducible across runs and unaffected by a backward clock step.
Violation mirrors SMPTNG-767's out-of-order/duplicate timestamp class.

## Mechanism

None currently; guards against regressions.

## needs_sut_fix

None (regression-guard).

## Investigation Log

- [2026-07-24] Confirmed timestamps are rng-derived, not clock-derived (digest payload
  finding). Antithesis clock control makes the perturbed-clock replay a strong oracle.
- [2026-07-24] SMPTNG-767 (ADP aggregate stamps payloads with a non-monotonic wall clock) is
  the SUT-analog class this property guards against inside lading.

## Open Questions

- None specific.
