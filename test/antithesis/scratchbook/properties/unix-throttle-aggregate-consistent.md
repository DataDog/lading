---
slug: unix-throttle-aggregate-consistent
subsystem: generators/throttle
assertion_type: Always
priority: P1
needs_target: true
needs_sut_fix: add .divide(worker_count) in unix_stream/unix_datagram (or document per-connection semantics)
---

# unix_stream/unix_datagram aggregate rate matches configured bytes_per_second

## Statement

unix_stream and unix_datagram divide the throttle across `parallel_connections` so aggregate
delivery approximates `bytes_per_second`, consistent with tcp/udp, rather than delivering
`parallel_connections x` the rate.

## Code evidence trail

- `unix_stream.rs:161-163` — each child gets a full `create_throttle(bytes_per_second)`, no
  `.divide`.
- `unix_datagram.rs:186-188` — same, no `.divide`.
- Contrast `tcp.rs:168-173` which divides. Aggregate delivery = `parallel_connections *
  bytes_per_second` for unix vs `~= bytes_per_second` for tcp/udp.
- Doc mismatch: `unix_stream.rs:46-47` says "per connection"; `unix_datagram.rs:54` implies
  aggregate.

## Assertion-type rationale

**Always** (rate fidelity): on every timeline the same `bytes_per_second` key must have a
consistent aggregate meaning across generators.

## Mode / observability

Aggregate delivered bytes for unix generators at `N>1` approximate `bytes_per_second`, not
`N x it`. Cross-generator inconsistency: same config key means aggregate for tcp/udp but
per-connection for unix. Oracle: sink byte-rate vs configured.

## Mechanism

Add `.divide(worker_count)` in `unix_stream.rs:161-163` and `unix_datagram.rs:186-188` (or
document per-connection semantics deliberately).

## needs_sut_fix

Yes (or a deliberate documentation decision).

## Investigation Log

- [2026-07-24] Confirmed: unix generators do not divide the throttle, so aggregate is `N x` the
  configured rate.

## Open Questions

- Is undivided per-connection throttle intentional for unix_stream (its doc says "per
  connection") vs unix_datagram (whose doc implies aggregate)?
