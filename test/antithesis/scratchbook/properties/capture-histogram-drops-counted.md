---
slug: capture-histogram-drops-counted
subsystem: capture
assertion_type: Always
priority: P1
needs_target: false
needs_sut_fix: add capture_histogram_samples_dropped counter (bounded label, in registry not the sample channel)
---

# Dropped histogram samples are counted, not silently lost

## Statement

When the capture channel is full or the recorder is uninitialized, dropped latency/histogram
samples increment a bounded-label counter held in the registry, so tail-biased sample loss is
observable.

## Code evidence trail

- `manager.rs:302` — the sample channel is `mpsc::channel(10_000)` (bounded).
- `manager.rs:121-133` — `CaptureHistogram::record` uses `try_send` and only `warn!`s on a full
  channel, silently dropping the sample.

## Assertion-type rationale

**Always** (observability): whenever samples are lost, the loss must be counted so a run
delivering incomplete histograms is distinguishable from a healthy one.

## Mode / observability

Under high recording load a run delivering incomplete histograms shows a nonzero
`capture_histogram_samples_dropped` rather than looking healthy. Oracle: saturate the channel,
assert dropped count `> 0` when samples were lost.

## Mechanism

Add a `capture_histogram_samples_dropped` counter (bounded reason label, held in the registry
NOT the sample channel) at `manager.rs:121-133`.

## needs_sut_fix

Yes. Landed on backup branch (`39d8ae56`), not main.

## Investigation Log

- [2026-07-24] Confirmed live on main: samples are dropped with only a `warn!`. In a
  latency-measuring tool this is silent tail-biased sample loss with no mark in the capture
  file.
- [2026-07-24] The counter must live in the registry, not the sample channel, so the drop count
  cannot itself feed back into the full-channel condition.

## Open Questions

- None specific.
