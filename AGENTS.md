# Agent Instructions

Guidelines for AI agents working on the lading codebase. For detailed rationale
behind these rules, see the [Architecture Decision Records](docs/adr/README.md).

## Quick Reference

| Need | Answer |
|------|--------|
| Validate changes | `ci/validate` (mandatory, never skip) |
| Run tests | `ci/test` |
| Run Kani proofs | `ci/kani <crate>` |
| Shape rule | 3+ repetitions = create abstraction |
| Dependencies | When in doubt, implement rather than import |
| HashMap allowed? | No - use `IndexMap`/`IndexSet` for determinism |

## Task Routing

| Task | Key Rules | ADRs |
|------|-----------|------|
| Adding a generator | Pre-compute payloads, deterministic RNG, no panics, **property tests for determinism** | [001](docs/adr/001-generator-target-blackhole-architecture.md), [002](docs/adr/002-precomputation-philosophy.md), [003](docs/adr/003-determinism-requirements.md), [006](docs/adr/006-testing-strategy.md) |
| Modifying payload generation | **Property tests for invariants**, deterministic output, no wall-clock time | [002](docs/adr/002-precomputation-philosophy.md), [003](docs/adr/003-determinism-requirements.md), [006](docs/adr/006-testing-strategy.md) |
| Adding a blackhole | Must never backpressure target, **property tests for throughput guarantees** | [001](docs/adr/001-generator-target-blackhole-architecture.md), [005](docs/adr/005-performance-first-design.md), [006](docs/adr/006-testing-strategy.md) |
| Modifying throttle | **Kani proofs required**, formal invariants | [003](docs/adr/003-determinism-requirements.md), [005](docs/adr/005-performance-first-design.md), [006](docs/adr/006-testing-strategy.md) |
| Core algorithm changes | **Proofs where feasible, property tests otherwise** | [006](docs/adr/006-testing-strategy.md) |
| Writing tests | Property tests default, document invariants | [006](docs/adr/006-testing-strategy.md) |
| Adding a dependency | Evaluate: performance, determinism, control | [008](docs/adr/008-dependency-philosophy.md) |
| Code style questions | Shape rule, no mod.rs, imports at top | [007](docs/adr/007-code-style-and-abstraction.md) |

## Architecture Overview

```
Generator --> Target --> Blackhole
```

- **Generators**: Create deterministic load, push to targets
- **Targets**: Programs being tested (subprocesses)
- **Blackholes**: Receive output from targets

Key principle: **Pre-compute everything possible.** See [ADR-001](docs/adr/001-generator-target-blackhole-architecture.md), [ADR-002](docs/adr/002-precomputation-philosophy.md).

## Core Constraints

Lading must be:

1. **Faster than targets** - users must never wonder if lading is the bottleneck
2. **Deterministic** - same seed = same load = reproducible results
3. **Extensible** - support variety of protocols and use-cases

See [ADR-003](docs/adr/003-determinism-requirements.md), [ADR-005](docs/adr/005-performance-first-design.md).

## Rules by Topic

### Performance

- Pre-allocate when size is known
- No allocations in hot paths
- Prefer worst-case over average-case algorithms
- No optimization without profiling data

See [ADR-005](docs/adr/005-performance-first-design.md).

### Determinism

When writing generators or payload code:

- Use explicit seed from configuration (never `thread_rng()`)
- Use `IndexMap`/`IndexSet` over `HashMap`/`HashSet`
- No wall-clock time in payload generation
- Same seed must produce byte-identical output

See [ADR-003](docs/adr/003-determinism-requirements.md).

### Error Handling

- **MUST NOT panic** - use `Result<T, E>` always
- No `.unwrap()` or `.expect()`
- Propagate errors with context using `thiserror`

```rust
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to connect to {addr}: {source}")]
    Connect { addr: SocketAddr, source: io::Error },
}
```

See [ADR-004](docs/adr/004-no-panic-error-handling.md).

### Code Style

- **No `mod.rs`** - use `foo.rs` not `foo/mod.rs`
- **Imports at top** - never inside functions
- **Named format args** - `"{index}"` not `"{}"`
- **Shape rule**: 3+ repetitions = create abstraction
- **No internal backward compatibility** - only user configs need stability

See [ADR-007](docs/adr/007-code-style-and-abstraction.md).

### Testing

**Hierarchy**: Proofs > property tests > unit tests

- Proofs are preferred for all code, but Kani proofs can run for unbounded time
- Trade-off: we require proofs for critical components where feasible
- **The whole program must be correct**, not just the throttle - generators, payload
  generation, blackholes, and protocol implementations all have correctness requirements
- Where proofs are impractical, use property tests via [proptest](https://github.com/proptest-rs/proptest)
- Unit tests only for simple pure functions

**Invariant documentation required** - use formal mathematical language:

```rust
/// For all request > max_capacity: request(ticks, request) returns Err
///
/// Capacity requests exceeding maximum always fail.
#[kani::proof]
fn request_too_large_always_errors() { ... }
```

See [ADR-006](docs/adr/006-testing-strategy.md) for patterns and examples.

### Dependencies

Evaluate before adding: performance, determinism, control, criticality.
When in doubt, implement rather than import.

See [ADR-008](docs/adr/008-dependency-philosophy.md).

## Documentation

Document **why**, not **what**. The code shows what; comments explain why.

- `//!` for crate/module level
- `///` for types and functions
- Functions: imperative verb, `# Errors`, `# Panics` sections
- **US-ASCII only** - no Unicode characters in code or documentation

```rust
/// Send payload bytes to the target.
///
/// # Errors
///
/// Returns error if connection is closed or write times out.
pub async fn send(&mut self, payload: &[u8]) -> Result<(), Error> { ... }
```

## Validation Workflow

**MUST run `ci/validate` after any code changes.**

| Task | Command |
|------|---------|
| Full validation | `ci/validate` |
| Format | `ci/fmt` |
| Check | `ci/check` |
| Lint | `ci/clippy` |
| Test | `ci/test` |
| Kani proofs | `ci/kani <crate>` |
| Outdated deps | `ci/outdated` |
| Benchmarks | `cargo criterion` |

## Property Test Configuration

| Environment | PROPTEST_CASES | PROPTEST_MAX_SHRINK_ITERS |
|-------------|----------------|---------------------------|
| Local (`ci/test`) | 64 | 2048 |
| CI | 512 | 10000 |

Override locally: `PROPTEST_CASES=512 ci/test`

## Key Reminders

1. Run `ci/validate` after changes - never skip
2. Use ci/ scripts, not raw cargo commands
3. No panics - Result types only
4. **The whole program must be correct** - property tests for generators, payloads,
   blackholes; Kani proofs for throttle and core algorithms where feasible
5. Generators must be deterministic (explicit seeding, IndexMap)
6. Pre-compute in init, not hot paths
7. No HashMap/HashSet where iteration order matters
8. Document invariants with formal language in tests/proofs
9. Consider measurement interference
10. No mod.rs files, imports at file top only
