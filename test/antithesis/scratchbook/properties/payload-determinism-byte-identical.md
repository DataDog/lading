---
slug: payload-determinism-byte-identical
subsystem: payload/determinism
assertion_type: Always
priority: P1
needs_target: false
needs_sut_fix: none expected (guards against regressions)
---

# Same seed + config yields byte-identical load

## Statement

For a fixed seed and config, the sequence of bytes lading emits is identical across runs; no
wall-clock, HashMap iteration order, or hidden entropy feeds payload bytes.

## Code evidence trail

- Ordered output uses `BTreeMap`/`BTreeSet`: `fluent.rs:108`, `trace_agent/v04.rs:209-222`,
  `opentelemetry/trace.rs:1332`.
- `templated_json` uses `FxHashMap` (`resolver.rs:14`) but iteration only assigns internal def
  indices resolved by name (`resolver.rs:120-129,178-186`), so output is index-agnostic and
  FxHasher is fixed-seed.
- dogstatsd tags use `HashSet` only for uniqueness checks, never iterated for output; selection
  is via an indexed `Vec` (`common/tags.rs:34-63`).
- Timestamps are rng-derived: `trace_agent/v04.rs:326,384`,
  `templated_json/generator.rs:170-196`.
- `block.rs:614,659` — `tokio::time::Instant` feeds only progress logging, never bytes.

## Assertion-type rationale

**Always** (determinism): every pair of same-seed runs must produce identical bytes. Oracle:
byte-equality across two seeded runs, or a SUT `always!` on a rolling hash of emitted blocks.

## Mode / observability

Two runs with identical seed/config produce identical byte streams at the sink (or identical
block-cache hashes). Violation breaks the determinism ADR and Antithesis reproducibility.

## Mechanism

None expected (BTreeMap/BTreeSet ordering, rng-derived timestamps confirmed); the property
guards against regressions introducing wall-clock/entropy.

## needs_sut_fix

None (regression-guard).

## Investigation Log

- [2026-07-24] Digest confirms byte output is a pure function of `(seed, config)`; no
  wall-clock or HashMap iteration feeds payload bytes.
- [2026-07-24] Lead analog: SMPTNG-762 (AWS-LC CPU-jitter entropy aborts under deterministic
  execution) and SMPTNG-767 (non-monotonic wall clock) are SUT bugs whose class must not exist
  in lading — hunt lading's own crypto/entropy/time usage for the same pattern.

## Open Questions

- No determinism invariant is currently expressed as an SDK `always!` anywhere; the harness
  relies on the sink byte counter and config sampling. Should a direct byte-equality oracle be
  added?
