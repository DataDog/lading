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

# Code Style

This project enforces code style through automated tooling. Use `ci/validate` to
check style compliance - it will run formatting and linting checks for you.

We do not allow for warnings: all warnings are errors. Deprecation warnings MUST
be treated as errors. Lading is written in a "naive" style where abstraction is
not preferred if a duplicated pattern will satisfy. Our reasoning for this is it
makes ramp-up for new engineers easier: all you must do is follow the pattern,
not internalize a complex type hierarchy. There are obvious places in the code
base where replicated patterns have been made into abstractions -- we follow the
"shape" rule, if you have three or more repeats, make a jig -- but we do not
start there.

Lading is written to be as easy to contribute to as possible. We ask that any
dependency used in the project in more than one crate be present in the
top-level Cargo.toml and be referenced from the workspace in sub-crates.

Lading does not care about inward backward compatibility. Behavior observable to
users must be preserved, but our internal APIs are not subject to backward
compatibility concerns.

Lading project uses comments strategically, documenting the "why" and not the
"what". Do not add "what" comments that put into English the behavior of a line
or two of code. Do add comments that explain the "why" of a block of code, how
it functions or is unusual in a way that an experienced engineer might not
understand.

Crate versions are always given as XX.YY and not XX.YY.ZZ.

# Testing

Where possible lading prefers property tests over unit tests and in especially
critical components we require proofs. We use
[proptest](https://github.com/proptest-rs/proptest) in this project for property
tests and the [kani](https://github.com/model-checking/kani) proof tool.

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

# Key Reminders for Claude

1. ALWAYS use `ci/validate` after code changes - never skip this
2. Do NOT run cargo commands directly - use the ci/ scripts
3. When users ask about build/test failures, suggest running `ci/validate`
4. If users are confused about project correctness criteria, point them to `ci/validate`
5. The ci/ scripts are the single source of truth for validation
