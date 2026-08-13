---
slug: stable-burst-envelope-bounded
subsystem: generators/throttle
assertion_type: Always
priority: P1
needs_target: false
needs_sut_fix: none if Kani proofs hold; add feature-gated assert_always! in stable.rs to catch regressions
---

# Stable throttle never exceeds its per-interval burst envelope

## Statement

In the real async `wait_for` path under an adversarial clock, the stable throttle grants at most
`maximum_capacity` at `timeout=0` (no over-delivery) and at most `(MAX_ROLLED_INTERVALS+1)x`
with rolled capacity.

## Code evidence trail

- `stable.rs` — the async `wait_for` grant path.
- Digest `3f4a6bd2` — feature-gated SUT assertion + proptests demonstrating the envelope.

## Assertion-type rationale

**Always** (rate safety): on every timeline, including adversarial-clock timelines, the granted
capacity per interval must stay within the proven envelope. SUT-side feature-gated
`assert_always!` under an adversarial clock (Antithesis controls time) + proptests.

## Mode / observability

Granted capacity per interval stays within the proven envelope; a clock-perturbation-induced
over-grant is a defect. Envelope: `== maximum_capacity` at `timeout=0` (no over-delivery),
`<= (MAX_ROLLED_INTERVALS+1)x` with rolled capacity (up to 11x).

## Mechanism

None if the Kani proofs hold; add a feature-gated `assert_always!` in `stable.rs` (landed on
backup branch `3f4a6bd2`) to catch regressions.

## needs_sut_fix

None expected (Kani proofs); the feature-gated assertion is a regression guard.

## Investigation Log

- [2026-07-24] The throttle has Kani proofs per the ADRs; this property is the runtime
  regression guard under Antithesis clock control, complementing the static proofs.

## Open Questions

- None specific.
