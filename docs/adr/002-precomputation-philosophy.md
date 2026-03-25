# ADR-002: Pre-computation Philosophy

## Status

**Accepted**

## Date

2024-01-01 (formalized from existing design)

## Context

Lading is a performance measurement tool. Any overhead introduced by lading itself
can interfere with accurate measurement of the target. We needed a strategy to
minimize runtime overhead while maintaining flexibility in payload generation.

The core tension: complex, realistic payloads require computation, but computation
during load generation adds latency and CPU overhead that could affect measurements.

## Decision

**Pre-compute everything possible.** This is a fundamental principle of lading's design.

### Implementation Strategy

1. **Block-based payload caching**: Payloads are generated into "blocks" during
   initialization, then cycled through during the experiment. See `lading_payload/src/block.rs`.

2. **Decouple create/send operations**: Generation happens in initialization or
   background threads; hot paths only select and transmit pre-built data.

3. **Deterministic generation with seeding**: Payloads are generated from seeded
   RNGs, making them reproducible without runtime randomness overhead.

### What Gets Pre-computed

- Payload bytes (protocol-formatted messages)
- String pools for variable content
- Attribute/tag combinations
- Trace hierarchies and span relationships
- File content for file-based generators

### What Cannot Be Pre-computed

- Timestamps (must be current for many protocols)
- Sequence numbers (must increment)
- Connection-specific state

These exceptions are kept minimal and computed with O(1) operations.

## Consequences

### Positive

- **Minimal runtime overhead**: Hot paths are primarily memory reads and I/O
- **Predictable performance**: No GC pauses or allocation spikes during experiments
- **Determinism**: Same seed produces same payloads across runs
- **Accurate measurements**: Lading overhead is constant and measurable

### Negative

- **Memory consumption**: Pre-built blocks consume RAM proportional to cache size
- **Startup latency**: Initialization takes longer as payloads are pre-generated
- **Reduced flexibility**: Cannot generate truly dynamic content (e.g., live timestamps in all fields)
- **Cache coherence**: Very long experiments may cycle through blocks multiple times

### Neutral

- Block cache size is configurable via `maximum_prebuild_cache_size_bytes`
- Trade-off between memory and payload variety is explicit

## Alternatives Considered

### Just-in-time generation

Generate payloads on demand. Rejected because:
- Adds variable latency to hot paths
- CPU overhead interferes with target measurement
- Harder to achieve determinism

### Hybrid approach (partial pre-computation)

Pre-compute templates, fill in dynamic fields at runtime. Current approach does
this for timestamps/sequences but pre-computes everything else. Full hybrid was
rejected as it adds complexity without proportional benefit.

### External payload files

Load payloads from disk. Rejected because:
- I/O adds latency
- Less flexible for parameterized generation
- Harder to achieve variety at scale

## References

- `lading_payload/src/block.rs` - Block cache implementation
- `lading_payload/src/lib.rs` - Payload trait definitions
- ADR-001: Generator-Target-Blackhole Architecture
- ADR-003: Determinism Requirements
