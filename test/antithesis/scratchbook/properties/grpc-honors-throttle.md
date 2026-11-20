---
slug: grpc-honors-throttle
subsystem: generators/throttle
assertion_type: Always
priority: P0
needs_target: true
needs_sut_fix: honor the throttle Result in grpc.rs (discard+count on rejection)
---

# gRPC generator honors throttle rejections and configured rate

## Statement

The gRPC generator discards+counts throttle-rejected blocks (like tcp/udp) and does not send a
block regardless of the throttle result; delivered rate does not exceed configured.

## Code evidence trail

- `grpc.rs:307-318` — `let _ = result;` at `:309` ignores the throttle outcome and sends the
  block regardless; requests are also awaited sequentially at `:313-318`.

## Assertion-type rationale

**Always** (rate fidelity): on every timeline the gRPC delivered rate must not exceed the
configured `bytes_per_second`, and a rejected block must not be sent.

## Mode / observability

gRPC delivered rate `<= configured bytes_per_second`; on a Capacity error the block is not
sent. Currently `let _ = result;` ignores the outcome and sends anyway. Oracle: sink byte-rate
vs configured under a gRPC scenario.

## Mechanism

Honor the throttle `Result` in `grpc.rs:307-318` (discard + count `blocks_discarded` on
rejection).

## needs_sut_fix

Yes. Landed on backup branch (`944d4be4`); live on main.

## Investigation Log

- [2026-07-24] Confirmed live regression on main: the throttle result is discarded via
  `let _ = result;`.
- [2026-07-24] Separately, gRPC serializes requests (awaits each before the next), so effective
  throughput becomes RTT-bound and under-delivers vs configured when the target is slow — a
  distinct rate-fidelity concern in the same code region.

## Open Questions

- None specific.
