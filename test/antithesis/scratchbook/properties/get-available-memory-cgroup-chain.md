---
slug: get-available-memory-cgroup-chain
subsystem: observer
assertion_type: Always
priority: P2
needs_target: false
needs_sut_fix: none (merged to main, 1085887c)
---

# Memory limit reflects the tightest cgroup v2 ancestor limit

## Statement

`get_available_memory` walks the cgroup v2 ancestor chain and returns the minimum `memory.max`,
matching kernel hierarchical enforcement, rather than reading "max" from a namespaced root and
believing it has `u64::MAX`.

## Code evidence trail

- Merged fix `1085887c` — `get_available_memory` walks the cgroup v2 ancestor chain from the
  process-specific path upward and returns the tightest (minimum) `memory.max`.
- Injectable file reader `get_available_memory_with` for deterministic tests.

## Assertion-type rationale

**Always** (accuracy): on every timeline the reported available memory must equal the effective
container limit. Oracle: deterministic test with synthetic cgroup files (injectable reader).

## Mode / observability

Reported available memory equals the effective container limit, not `u64::MAX`, in a
cgroup-namespaced container.

## Mechanism

None (merged to main, `1085887c`); the property guards against regression via the injectable
`get_available_memory_with` reader.

## needs_sut_fix

None (merged to main). Regression-guard.

## Investigation Log

- [2026-07-24] Merged to main (`1085887c`, #1824). Previously only read the cgroup v2 root
  `memory.max`, which reads "max" when the container has its own cgroup namespace, so lading
  believed it had `u64::MAX` memory.

## Open Questions

- None specific.
