---
slug: discarded-blocks-counted
subsystem: generators/throttle
assertion_type: Always
priority: P1
needs_target: true
needs_sut_fix: add blocks_discarded counters in tcp/udp/grpc
---

# Under-delivery is observable: discarded blocks are counted

## Statement

tcp/udp/grpc generators count throttle-rejected/discarded blocks (`blocks_discarded`) so a run
delivering ~zero bytes is distinguishable from a healthy flat-metrics run.

## Code evidence trail

- `tcp.rs:273-276` — discards with only a `debug!` log, no counter.
- Same discard-without-counter pattern in udp and grpc.

## Assertion-type rationale

**Always** (observability): whenever blocks are discarded the count must be nonzero, so silent
under-delivery is distinguishable. Oracle: oversized-block scenario asserting
`delivered==0 => blocks_discarded>0`.

## Mode / observability

A zero-byte-delivery run surfaces a nonzero `blocks_discarded` rather than only a `debug!` log.
Distinguishes silent under-delivery.

## Mechanism

Add `blocks_discarded` counters in `tcp.rs`/`udp.rs` (`73c4805e` on backup branch) and
`grpc.rs`.

## needs_sut_fix

Yes. `73c4805e` on backup branch; live on main.

## Investigation Log

- [2026-07-24] Confirmed live on main: discards are logged at `debug!` only, so a config that
  discards every block (the 0.31.2 busy-discard livelock class) looks like a healthy
  flat-metrics run.
- [2026-07-24] Observability companion to `throttle-divide-no-silent-underdelivery` and
  `throttle-capacity-no-zerodelivery-livelock`.

## Open Questions

- None specific.
