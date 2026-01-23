# ADR-005: Performance-First Design

## Status

**Accepted**

## Date

2024-01-01 (formalized from existing design)

## Context

Lading's behavior has consequences on the development practices of a large number
of engineers. Performance claims made using lading influence architectural decisions,
resource allocation, and optimization priorities.

If lading is slower than the target being tested, measurements are invalid. Users
must never wonder: "Is this bottleneck in my target or in lading?"

## Decision

**Lading must be strictly faster than targets.** This is enforced through design
patterns, not just optimization.

### Design Principles

1. **Pre-allocate when size is known**
   ```rust
   // Good
   let mut vec = Vec::with_capacity(known_size);

   // Avoid
   let mut vec = Vec::new();
   for item in items { vec.push(item); }
   ```

2. **Avoid allocations in hot paths**
   - Hot paths should only read from pre-allocated structures
   - Allocations happen in initialization, not in request handling

3. **Prefer worst-case over average-case algorithms**
   - O(n) worst-case is better than O(1) average with O(n^2) worst
   - Bounded operations over unbounded
   - Predictable latency over throughput optimization

4. **Avoid unnecessary cloning or copying**
   - Use references where ownership isn't needed
   - Use `Arc` for shared ownership instead of cloning

5. **Use bounded data structures**
   - Bounded channels over unbounded
   - Fixed-size buffers where possible
   - Explicit limits on growth

### What NOT To Do

**Do NOT dive into optimization rabbit holes without profiling data.**

Benchmarks alone are insufficient. Real performance work requires:
- Profiling with real workloads
- Identifying actual bottlenecks
- Measuring before and after

Stick to "obviously fast" patterns. Micro-optimizations without measurement are
often wrong and always harder to maintain.

### Performance Validation

- `ci/bench-check` validates benchmarks compile
- Criterion benchmarks exist for critical paths
- Integration tests measure end-to-end throughput

## Consequences

### Positive

- **Trustworthy measurements**: Users can trust that bottlenecks are in targets
- **Predictable behavior**: Worst-case focus prevents surprise latency spikes
- **Maintainable code**: "Obviously fast" patterns are also obvious to read
- **Low overhead**: Lading's resource consumption is minimal and constant

### Negative

- **Implementation constraints**: Some elegant solutions are too slow
- **Memory trade-offs**: Pre-allocation increases memory footprint
- **Code verbosity**: Performance patterns can be more verbose

### Neutral

- Performance is validated through benchmarks and integration tests
- Trade-offs between memory and CPU are explicit in configuration

## Patterns Reference

### DO

```rust
// Pre-allocate collections
let mut results = Vec::with_capacity(expected_count);

// Reuse buffers
buffer.clear();
buffer.extend_from_slice(data);

// Bounded channels
let (tx, rx) = tokio::sync::mpsc::channel(BOUND);

// Explicit capacity
let mut map = HashMap::with_capacity(expected_entries);
```

### DON'T

```rust
// Unbounded growth
let mut vec = Vec::new();
loop { vec.push(item); }

// Allocation in hot path
fn handle_request() {
    let buffer = Vec::new(); // Allocates every call
}

// Unbounded channels
let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

// Clone when reference suffices
fn process(data: String) { } // Takes ownership, may clone
fn process(data: &str) { }   // Reference, no clone
```

## Alternatives Considered

### Optimize on demand

Only optimize when benchmarks show problems. Rejected because:
- By then, architecture may prevent optimization
- "Obviously fast" patterns cost little upfront
- Prevention is cheaper than cure

### Profile-guided optimization

Use PGO and LTO extensively. These are fine but:
- Don't replace good design patterns
- Add build complexity
- Are orthogonal to this ADR

### Accept some overhead

Allow lading to be "fast enough" rather than "fastest". Rejected because:
- "Fast enough" is subjective and changes with targets
- Undermines confidence in measurements
- Slippery slope to slower code

## References

- `lading_payload/src/block.rs` - Pre-allocation patterns
- `lading_throttle/src/` - Bounded rate limiting
- `ci/bench-check` - Benchmark validation
- ADR-002: Pre-computation Philosophy
