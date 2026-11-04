---
slug: sink-receives-bytes
subsystem: generators/throttle
assertion_type: Sometimes
priority: P1
needs_target: true
needs_sut_fix: none (already instrumented)
---

# The sink receives load (load-arrival non-vacuity)

## Statement

Across a run the sink container receives a nonzero number of bytes from lading generators.

## Code evidence trail

- `sink/main.rs:82` — `sometimes! (total>0)` "sink received bytes".
- `sink/main.rs:69-83` — `handle_connection` records bytes and asserts `total>0`; SDK init at
  `:32`.

## Assertion-type rationale

**Sometimes** (non-vacuity of delivery on at least one timeline): guards against a whole-config
class delivering nothing (divide stall, capacity livelock, throttle bypass).

## Mode / observability

`sink/main.rs:82` `sometimes! (total>0)` "sink received bytes". The oracle is already present in
the never-faulted sink container.

## Mechanism

Sometimes (non-vacuity of delivery). Oracle already present in the never-faulted sink container.

## needs_sut_fix

None (already instrumented).

## Investigation Log

- [2026-07-24] Already wired: the sink container owns the "load arrived" assertion. This is the
  cross-check that catches the throttle-divide / capacity-livelock / gRPC-throttle-bypass
  classes that would otherwise silently deliver zero bytes.

## Open Questions

- None specific.
