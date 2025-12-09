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

Lading project uses comments strategically, documenting the "why" and not the
"what". Do not add "what" comments that put into English the behavior of a line
or two of code. Do add comments that explain the "why" of a block of code, how
it functions or is unusual in a way that an experienced engineer might not
understand.

Always add comments for:
- Non-obvious performance optimizations
- Complex state machines or control flow
- Unusual algorithm choices
- Workarounds for external limitations
- Any code that would make an experienced engineer pause and wonder "why?"

Crate versions are always given as XX.YY and not XX.YY.ZZ.

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
- `PROPTEST_CASES=10000` - Very thorough testing, catches rare edge cases
- `PROPTEST_MAX_SHRINK_ITERS=1000000` - Excellent minimal examples for failures
- `PROPTEST_MAX_SHRINK_TIME=300000` - 5 minute timeout for complex shrinking

**Override locally**: Run `PROPTEST_CASES=10000 ci/test` to use CI-level thoroughness.

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
