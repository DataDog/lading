# ADR-003: Determinism Requirements

## Status

**Accepted**

## Date

2024-01-01 (formalized from existing design)

## Context

Lading is used to make performance claims about software. These claims must be:

1. **Reproducible**: Running the same experiment twice should produce comparable results
2. **Verifiable**: Others must be able to replicate findings
3. **Debuggable**: When issues occur, we need to recreate the exact conditions

Non-determinism in load generation undermines all three properties. If the load
varies between runs, performance differences could be due to the load, not the
target.

## Decision

**Generators must be deterministic.** Given the same configuration and seed, a
generator must produce identical output.

### Implementation Requirements

1. **Explicit RNG seeding**: All randomness flows through seeded RNGs (typically
   `rand::SeedableRng` implementations). Seeds are either:
   - Specified in configuration
   - Derived deterministically from other config values
   - Logged at startup for reproduction

2. **No implicit randomness**: Functions must not use `thread_rng()` or other
   unseeded random sources. All RNG instances must be passed explicitly.

3. **Deterministic iteration order**: Use `IndexMap`/`IndexSet` instead of
   `HashMap`/`HashSet` where iteration order matters. Or sort before iteration.

4. **Time-independent generation**: Payload content must not depend on wall-clock
   time during generation. Timestamps in payloads use controlled, reproducible values.

5. **Thread-determinism**: Multi-threaded generators must produce deterministic
   output regardless of thread scheduling. This typically means:
   - Assigning work to threads deterministically
   - Using per-thread RNGs derived from a master seed

### Configuration

```yaml
generator:
  - http:
      seed: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
             17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32]
      # ... other config
```

If no seed is provided, one is generated and logged for reproduction.

## Consequences

### Positive

- **Reproducible experiments**: Same config + seed = same load = comparable results
- **Debugging**: Can recreate exact conditions that triggered issues
- **Verification**: Others can independently verify performance claims
- **Bisection**: Can identify regressions by replaying identical loads

### Negative

- **Implementation complexity**: Must thread RNGs through all code paths
- **No true randomness**: Cannot test behavior under truly random loads
- **Subtle bugs**: Determinism violations are hard to detect and debug
- **Performance cost**: Some deterministic alternatives are slower (e.g., sorting)

### Neutral

- Seeds appear in logs and can be extracted for reproduction
- Determinism is validated through fingerprint tests in CI

## Alternatives Considered

### Statistical reproducibility

Run many trials and compare distributions. Rejected because:
- More expensive (many runs needed)
- Harder to debug specific issues
- Statistical analysis adds complexity

### Record/replay

Record generated load, replay for comparison. Rejected because:
- Storage overhead for recorded loads
- Doesn't help with initial generation
- Adds complexity to tooling

### Timestamp-based seeding

Use experiment start time as seed. Rejected because:
- Makes reproduction dependent on knowing exact start time
- Clock skew could cause issues
- Explicit seeds are clearer

## References

- `lading_payload/src/lib.rs` - Payload generation with RNG parameters
- `ci/fingerprint` - Determinism validation in CI
- ADR-002: Pre-computation Philosophy
