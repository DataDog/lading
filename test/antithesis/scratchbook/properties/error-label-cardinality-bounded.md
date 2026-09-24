---
slug: error-label-cardinality-bounded
subsystem: generators/throttle
assertion_type: Always
priority: P1
needs_target: true
needs_sut_fix: map errors to io::ErrorKind for labels in tcp/udp (gRPC tonic error still raw, follow-up)
---

# Generator error-metric label cardinality is bounded

## Statement

`connection_failure`/`request_failure` error labels are drawn from a finite set
(`io::ErrorKind`), not raw `err.to_string()`; a flapping target cannot grow capture memory
without bound.

## Code evidence trail

- `tcp.rs`/`udp.rs` — use full `err.to_string()` as the "error" metric label; raw messages
  embed addresses/errno text.
- Each distinct label mints a new capture accumulator key (`[u64;61]+[f64;61]+[DDSketch;61]`)
  -> unbounded memory growth (ADR-005 OOM class).

## Assertion-type rationale

**Always** (bounded cardinality / memory): on every timeline the distinct error-label values
must stay bounded regardless of failure diversity.

## Mode / observability

Distinct error-label values stay bounded regardless of failure diversity; capture accumulator
key count does not grow unboundedly. Oracle: flapping-target scenario + capture key-count /
memory observation.

## Mechanism

Map errors to `io::ErrorKind` for labels in `tcp.rs`/`udp.rs` (`32dd4cf6` on backup branch);
the gRPC tonic error is still raw (follow-up).

## needs_sut_fix

Yes. `32dd4cf6` on backup branch; live on main. gRPC tonic error label still raw (unbounded),
noted as follow-up in the fix.

## Investigation Log

- [2026-07-24] Confirmed live on main: raw `err.to_string()` labels embed addresses/errno text,
  so a flapping target mints unbounded accumulator keys.

## Open Questions

- None specific (gRPC follow-up is tracked in the fix commit).
