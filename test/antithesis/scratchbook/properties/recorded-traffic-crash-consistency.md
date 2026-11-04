---
slug: recorded-traffic-crash-consistency
subsystem: blackhole/config
assertion_type: Always
priority: P2
needs_target: true
needs_sut_fix: none identified (LEAD to validate zstd framing / flush boundaries of the recorder)
---

# Blackhole-recorded traffic files are crash-consistent

## Statement

The blackhole traffic recorder writes files that, when compressed, remain decodable up to the
last flushed frame after an abrupt kill, and are deterministic for a fixed input.

## Code evidence trail

- Deployment/history: SMPTNG-390 (blackhole traffic recorder) and the recent record-policy work
  (commits #1911, #1895 OpenMetrics scrape bodies).
- Recorded-traffic files are a crash-consistency + determinism surface analogous to jsonl.

## Assertion-type rationale

**Always** (crash-consistency): on every timeline a recorder file from a killed blackhole must
decode to a valid prefix. Oracle: `node_termination` scenario + external decode check.

## Mode / observability

A recorder file from a SIGKILLed blackhole decodes to a valid prefix (analogous to jsonl), not
undecodable-past-last-frame corruption.

## Mechanism

None identified; LEAD to validate zstd framing / flush boundaries of the recorder (SMPTNG-390 /
record-policy work #1911/#1895).

## needs_sut_fix

None identified yet (a LEAD to validate).

## Investigation Log

- [2026-07-24] Related to SMPTNG-694 (captures now zstd-only): a truncated zstd stream may be
  undecodable past the last flushed frame, so the recorder's flush/framing boundaries determine
  whether a truncated file is a valid prefix.

## Open Questions

- Are recorded-traffic files zstd-framed such that a truncated stream is a valid prefix?
