---
slug: blackhole-never-backpressures-target
subsystem: blackhole/config
assertion_type: Always
priority: P1
needs_target: true
needs_sut_fix: use try_send + count-on-drop instead of blocking send().await in datadog handle_v2_protobuf
---

# Blackhole responds to the target regardless of capture-channel saturation

## Statement

A blackhole does not block its HTTP response to the target on a full bounded capture channel; a
slow/saturated capture manager cannot stall the target.

## Code evidence trail

- `datadog.rs:296-299` — the HTTP response is built after per-point awaits (status at
  `:289-294`).
- `datadog.rs:398,412,416` — `handle_v2_protobuf` awaits `counter_incr`/`gauge_set` per point
  BEFORE the response, each calling `lading_capture::send_metric`.
- `lib.rs:53-56` — `send_metric` does `sender.snd.send(metric).await` on a bounded
  `mpsc::channel(10_000)` (`manager.rs:302`); a full channel blocks the connection task.
- Contrast: the manager's own histogram path uses `try_send` + drop-on-full (`manager.rs:119-125`).

## Assertion-type rationale

**Always** (never-backpressure): on every timeline the target's request latency must stay
bounded regardless of capture-drain speed.

## Mode / observability

With a slow capture drain, target request latency stays bounded; the Datadog blackhole does not
await `send().await` per metric point before responding. Oracle: slow-capture-drain fault
scenario measuring target response latency.

## Mechanism

Use `try_send` + count-on-drop (consistent with the manager histogram path) instead of a
blocking `send().await` in `datadog.rs` `handle_v2_protobuf` (`:398,412,416`) before the
response is built.

## needs_sut_fix

Yes.

## Investigation Log

- [2026-07-24] Confirmed: the datadog record path blocks on a bounded-channel send per point
  before responding — an inconsistent, backpressure-prone choice vs the manager's own
  try_send+drop histogram path. Amplified under `RecordPolicy::All` with many series/points.

## Open Questions

- Is the capture manager fast enough in practice that the 10k bounded channel never fills under
  the deployment's datadog load, or has channel-full been observed?
