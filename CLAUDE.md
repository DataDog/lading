# Architecture Overview

Lading measures performance of long-running programs (daemons) using synthetic,
repeatable load generation. It operates with three components:

- **Generators**: Create deterministic load and push to targets
- **Targets**: Programs being tested (run as subprocesses by lading)
- **Blackholes**: Optionally receive output from targets

Data flows: Generator → Target → Blackhole

Key principle: Pre-compute everything possible. This minimizes runtime overhead
and interference with the target being measured.

# Design Goals

Lading is a performance tool. It's behavior has consequences on the development
practices of large number of engineers and so lading must be:

* strictly 'faster' than targets, users must not wonder if their target has hit
  a bottleneck or lading has,
* deterministic, targets must receive the same load across all instances of
  lading else replication of results is impossible and
* extensible, lading must be available and capable of being extended for a
  variety of use-cases.

No change is valid to lading that is in conflict with these goals. In specific
this means that lading will pre-allocate where possible, will explicitly thread
randomness and other sources of non-determinism into code paths and will
preference algorithms that have better worst-case behavior.

## Performance Guidance

Always consider performance implications of changes. Focus on "obviously fast" patterns:
- Pre-allocate collections when size is known
- Avoid allocations in hot paths
- Choose algorithms with good worst-case behavior over average-case
- Prefer bounded operations over unbounded ones
- Avoid unnecessary cloning or copying

Do NOT dive into performance optimization rabbit holes without profiling data.
Benchmarks alone are insufficient - real performance work requires profiling.
Stick to obviously correct performance patterns.

## Error Handling

Lading MUST NOT panic. Use controlled shutdowns only. All errors should be
propagated using Result types. Panics are only acceptable for truly exceptional
circumstances that cannot be handled (e.g., fundamental invariant violations that
indicate memory corruption).

When handling errors:
- Always use Result<T, E> for fallible operations
- Propagate errors up to where they can be meaningfully handled
- Provide context when wrapping errors
- Design for graceful degradation where possible

# Code Style

This project enforces code style through automated tooling. Use `ci/validate` to
check style compliance - it will run formatting and linting checks for you.

**Module organization**: Never use `mod.rs` files. Always name modules directly
(e.g., use `foo.rs` instead of `foo/mod.rs`). This makes the codebase easier to
navigate and grep.

**Imports**: NEVER place `use` statements inside functions. All imports must be at
the top of the file, after the module documentation and attributes. This keeps
imports organized and makes dependencies clear.

**String formatting**: When using format strings with `{}`, always include the
variable name inside the braces for clarity (e.g., `"{index}"` instead of `"{}"`).
This makes the code more readable and self-documenting.

We do not allow for warnings: all warnings are errors. Deprecation warnings MUST
be treated as errors. Lading is written in a "naive" style where abstraction is
not preferred if a duplicated pattern will satisfy. Our reasoning for this is it
makes ramp-up for new engineers easier: all you must do is follow the pattern,
not internalize a complex type hierarchy. There are obvious places in the code
base where replicated patterns have been made into abstractions -- we follow the
"shape" rule: if you have three or more repeats, make a jig. This is a hard rule:
3+ repetitions = create an abstraction. Less than 3 = duplicate the pattern.

Lading is written to be as easy to contribute to as possible. We ask that any
dependency used in the project in more than one crate be present in the
top-level Cargo.toml and be referenced from the workspace in sub-crates.

Lading does not care about internal backward compatibility. ALL internal APIs can
be changed freely - none of this project's APIs are used externally. The ONLY
backward compatibility that matters is end-user configuration files. User configs
must continue to work, but all internal code can be refactored without concern
for compatibility. Do not add unnecessary Option types or fallback logic for
internal API changes.

Crate versions are always given as XX.YY and not XX.YY.ZZ.

# Documentation and Comments

Lading follows strict documentation conventions designed for experienced engineers
who can read code but need context about design decisions. The core principle:
document **why**, not **what**. The code shows what it does; comments explain why
it does it that way.

## Crate-Level Documentation

Use `//!` in `lib.rs` files with this pattern:
- Brief description of the crate's purpose
- Statement about supporting the lading project
- Context about whether the crate is intended for external use

Example:
```rust
//! The lading daemon load generation and introspection tool.
//!
//! This library support the lading binary found elsewhere in this project. The
//! bits and pieces here are not intended to be used outside of supporting
//! lading, although if they are helpful in other domains that's a nice
//! surprise.
```

## Module-Level Documentation

Use `//!` at the top of module files. Explain the module's purpose, architectural
role, performance rationale, or design constraints. Focus on why the module exists
and how it fits into lading's architecture.

Example:
```rust
//! Construct byte blocks for use in generators.
//!
//! The method that lading uses to maintain speed over its target is to avoid
//! runtime generation where possible _or_ to generate into a queue and consume
//! from that, decoupling the create/send operations. This module is the
//! mechanism by which 'blocks' -- that is, byte blobs of a predetermined size
//! -- are created.
```

## Struct and Function Documentation

Use `///` for doc comments on types and functions.

**Structs**: Describe what the type represents and its behavioral characteristics.
Include field-level docs for configuration structs.

**Functions**: Start with imperative verb (Create, Send, Wait, Get, Construct).
Keep it brief - details go in implementation comments if needed. Always include:
- `# Errors` section when returning `Result`
- `# Panics` section when function can panic

Example:
```rust
/// Create a new throttle with capacity divided by the divisor
///
/// This is useful when distributing a throttle across multiple workers.
/// For `AllOut` throttles, returns self as they have unlimited capacity.
///
/// # Errors
///
/// Returns an error if the division would result in zero capacity.
pub fn divide(self, divisor: NonZeroU32) -> Result<Self, Error> {
```

## Inline Comments: The "Why" Rule

Inline comments are rare but detailed when present. They explain **why**, never
**what**. An experienced engineer can see what the code does - they need to know
why it does it that way.

**Always add inline comments for:**
- Non-obvious performance optimizations
- Complex algorithm explanations (include examples and walkthroughs)
- Unusual design choices or algorithm selections
- Workarounds for external limitations
- Verification constraints (Kani, Loom)
- Implementation strategies that aren't obvious
- Anything that would make an experienced engineer pause and wonder "why?"

**Never add inline comments for:**
- Simple operations that are clear from the code
- Standard Rust idioms
- Self-explanatory control flow
- Restating what the code does in English

**Good "why" comment example:**
```rust
// Okay, here's the idea. We have bucket that fills every INTERVAL_TICKS
// microseconds and requests draw down on that bucket. When it's empty,
// we return the number of ticks until the next interval roll-over.
// Callers are expected to wait although nothing forces them to.
// Capacity is only drawn on when it is immediately available.
```

**Another good example - explaining a constraint:**
```rust
// Why not use fetch_sub? That function overflows at the zero boundary
// and we don't want the peer count to suddenly be u32::MAX.
let mut old = self.peers.load(Ordering::Relaxed);
```

**Bad "what" comment example:**
```rust
// Increment the counter
counter += 1;
```

## Complex Algorithms

For complex algorithms, include detailed explanations with examples showing how
they work step-by-step. Use conversational tone ("Okay, here's the idea...") to
make complex topics accessible. Include concrete walkthroughs with specific values.

Example:
```rust
// Record unused capacity for each interval we're transitioning past.
// For example, if moving from interval 5 to interval 8
// (intervals_passed = 3):
//
// * i=0: Interval 5 (self.interval -> the current interval we're leaving)
//
//        Amount set to self.capacity
//
// * i=1: Interval 6 (self.interval + 1 -> a skipped interval)
//
//        Amount set to self.maximum_capacity, full capacity since no
//        request made on that interval
//
// * i=2: Interval 7 (self.interval + 2 - another skipped interval)
//
//        Amount set to self.maximum_capacity, same reasoning as
//        Interval 6.
```

## Error Documentation

Use `thiserror` with clear, actionable error messages. Document error variants
with `///` comments explaining when they occur.

```rust
/// Errors produced by [`Throttle`].
#[derive(thiserror::Error, Debug, Clone, Copy)]
pub enum Error {
    /// Division would result in zero capacity
    #[error("Division would result in zero capacity")]
    DivisionByZero,
}
```

# Testing

ALWAYS prefer property tests over unit tests. Unit tests are insufficient for
lading's requirements. We use [proptest](https://github.com/proptest-rs/proptest)
for property testing.

Test naming conventions:
- Don't prefix test names with `test_` - they're obviously tests
- Don't prefix property test generators with `prop_` - they're obviously property tests
- Use descriptive names that explain what property or behavior is being tested

Critical components are those which must function correctly or lading itself
cannot function. These require proofs using [kani](https://github.com/model-checking/kani):
- Throttling (MUST be correct - lading is used to make throughput claims)
- Core data generation
- Fundamental algorithms

The throttle is especially critical: incorrect throttling invalidates all
performance claims made using lading.

When writing tests:
- Default to property tests
- Unit tests are only acceptable for simple pure functions
- Think about the properties your code should maintain
- Test invariants, not specific examples

## Property Testing Configuration

Lading uses different proptest configurations for local development vs CI:

**Local development** (via `ci/test`):
- `PROPTEST_CASES=64` - Fast feedback, ~75% faster than defaults
- `PROPTEST_MAX_SHRINK_ITERS=2048` - Reasonable shrinking for local debugging

**CI** (via `.github/workflows/ci.yml`):
- `PROPTEST_CASES=512` - Thorough testing, 8x more than local, 2x more than defaults
- `PROPTEST_MAX_SHRINK_ITERS=10000` - Better minimal examples for failures
- `PROPTEST_MAX_SHRINK_TIME=60000` - 60s timeout for complex shrinking

**Override locally**: Run `PROPTEST_CASES=512 ci/test` to use CI-level thoroughness.

**All property tests** use these environment variable settings uniformly. There are
no tests with explicit `#![proptest_config(...)]` overrides - all tests respect
the environment variables for consistent control across the entire test suite.

# CRITICAL: Validation Workflow

You MUST run `ci/validate` after making any code changes. This is not optional.
The script runs all checks in optimal order and exits on first failure.

Do not run individual cargo commands - use the ci scripts instead:
- Use `ci/validate` for full validation
- Use `ci/fmt` instead of `cargo fmt`
- Use `ci/check` instead of `cargo check`
- Use `ci/clippy` instead of `cargo clippy`
- Use `ci/test` instead of `cargo nextest run`
- Use `ci/kani <crate>` for kani proofs (valid crates: lading_throttle, lading_payload)
- Use `ci/outdated` instead of `cargo outdated`

# Tools

To identify outdated dependencies: Use `ci/outdated`

To run micro-benchmarks: `cargo criterion`

## Dependencies

Due to lading's unusual constraints (performance, determinism, correctness),
we often must implement functionality rather than use external crates.

Before suggesting a new dependency, consider:
- Does it meet our performance requirements?
- Is it deterministic?
- Does it give us sufficient control?
- Is the functionality core to lading's purpose?

When in doubt, implement rather than import.

## Common Patterns and Anti-Patterns

### DO:
- Pre-allocate buffers and collections when size is known
- Use bounded channels/queues over unbounded
- Design for worst-case scenarios
- Use fixed-size data structures where possible
- Propagate errors up to where they can be handled meaningfully

### DON'T:
- Use `.unwrap()` or `.expect()` (these panic)
- Allocate in hot paths
- Use unbounded growth patterns
- Add external dependencies without careful consideration
- Implement complex abstractions for fewer than 3 use cases

# Key Reminders for Claude

1. ALWAYS use `ci/validate` after code changes - never skip this
2. Do NOT run cargo commands directly - use the ci/ scripts
3. When users ask about build/test failures, suggest running `ci/validate`
4. If users are confused about project correctness criteria, point them to `ci/validate`
5. The ci/ scripts are the single source of truth for validation
6. Always consider performance - suggest "obviously fast" patterns
7. Do NOT suggest performance optimizations without profiling data
8. When in doubt, prefer pre-allocation and good worst-case behavior
9. NEVER suggest code that could panic - use Result types
10. Be skeptical of external dependencies - we often need custom implementations
11. Remember the throttle is critical - any changes there need extra scrutiny
12. Generators must be deterministic - no randomness without explicit seeding
13. Pre-compute in initialization, not in hot paths
14. Think about how your code affects the measurement of the target
15. NEVER use mod.rs files - always name modules directly (e.g., foo.rs not foo/mod.rs)
16. NEVER place `use` statements inside functions - all imports go at the top of the file
17. NO internal backward compatibility - freely change ALL internal APIs. ONLY user configs need compatibility
18. Document "why" not "what" - inline comments explain design decisions, not what the code does
