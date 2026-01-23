# ADR-008: Dependency Philosophy

## Status

**Accepted**

## Date

2024-01-01 (formalized from existing design)

## Context

Lading has unusual constraints that many external crates don't satisfy:

1. **Performance**: Must not introduce overhead that affects measurements
2. **Determinism**: Must not introduce non-determinism (random seeds, timestamps)
3. **Control**: Must allow fine-grained control over behavior
4. **Correctness**: Must be provably correct for critical paths

External dependencies often don't meet these requirements, or meeting them requires
extensive configuration that negates the benefit of using the dependency.

## Decision

**When in doubt, implement rather than import.**

### Evaluation Criteria

Before adding a dependency, consider:

1. **Does it meet performance requirements?**
   - What's the overhead?
   - Does it allocate in hot paths?
   - What's the worst-case latency?

2. **Is it deterministic?**
   - Does it use random number generation?
   - Does it depend on wall-clock time?
   - Does it have non-deterministic iteration order?

3. **Does it give sufficient control?**
   - Can we configure it for our constraints?
   - Can we override problematic behaviors?
   - Can we test it adequately?

4. **Is the functionality core to lading's purpose?**
   - For core functionality, we need maximum control
   - For peripheral functionality, dependencies are more acceptable

### Workspace Dependency Management

Dependencies used in more than one crate must be:

1. Declared in the top-level `Cargo.toml` under `[workspace.dependencies]`
2. Referenced from workspace in sub-crates: `dependency = { workspace = true }`

This ensures:
- Consistent versions across crates
- Single location for version updates
- Visibility of shared dependencies

### Version Specification

Crate versions use `XX.YY` format, not `XX.YY.ZZ`:

```toml
# Good
tokio = "1.40"

# Avoid
tokio = "1.40.0"
```

### Examples of Build vs. Buy Decisions

**Implemented in-house:**
- Throttling (`lading_throttle`) - Core functionality, must be provably correct
- Payload generation (`lading_payload`) - Core functionality, must be deterministic
- Signal handling (`lading_signal`) - Simple, no benefit from dependency

**External dependencies used:**
- `tokio` - Async runtime, well-tested, would be massive to reimplement
- `serde` - Serialization, mature ecosystem, not in hot path
- `proptest` - Testing, not runtime code
- `criterion` - Benchmarking, not runtime code

## Consequences

### Positive

- **Full control**: Can optimize and modify as needed
- **No surprise behaviors**: We understand all code paths
- **Determinism**: Can ensure reproducibility
- **Minimal overhead**: Only include what we need
- **Provability**: Can apply Kani to our implementations

### Negative

- **Implementation cost**: Must write and maintain more code
- **Reinventing wheels**: Some implementations exist elsewhere
- **Bug surface**: Our code may have bugs libraries don't
- **Maintenance burden**: Must track security issues ourselves

### Neutral

- Forces explicit consideration of each dependency
- Creates deeper understanding of problem domain

## Decision Framework

```
Is this core to lading's purpose?
├── Yes
│   └── Do we need formal verification?
│       ├── Yes → Implement (can use Kani)
│       └── No → Does any library meet ALL constraints?
│           ├── Yes → Use library
│           └── No → Implement
└── No
    └── Is there a well-maintained library?
        ├── Yes → Use library
        └── No → Implement minimal version
```

## Alternatives Considered

### Maximize dependencies

Use libraries for everything. Rejected because:
- Dependencies often don't meet our constraints
- Harder to ensure determinism
- Less control over performance characteristics

### Zero dependencies

Implement everything from scratch. Rejected because:
- Impractical (would need to implement async runtime, etc.)
- Some dependencies are well-tested and stable
- Would delay development significantly

### Fork and modify

Fork dependencies and modify them. Used occasionally but:
- Maintenance burden of keeping forks updated
- May diverge significantly over time
- Only practical for small modifications

## References

- Workspace `Cargo.toml` - Dependency declarations
- `lading_throttle/` - Example of in-house implementation
- `lading_payload/` - Example of in-house implementation
- ADR-003: Determinism Requirements (why we need control)
- ADR-005: Performance-First Design (why we need performance)
