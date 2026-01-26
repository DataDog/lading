# ADR-007: Code Style and Abstraction Rules

## Status

**Accepted**

## Date

2024-01-01 (formalized from existing design)

## Context

Lading is written to be as easy to contribute to as possible. New engineers should
be able to:

1. Understand existing code quickly
2. Add new generators/blackholes by following patterns
3. Make changes without understanding complex type hierarchies

We needed a code style that prioritizes readability and onboarding over abstraction
elegance.

## Decision

**Lading uses a "naive" style where duplication is preferred over premature abstraction.**

### The Shape Rule

> If you have three or more repetitions, make a jig. This is a hard rule:
> 3+ repetitions = create an abstraction. Less than 3 = duplicate the pattern.

This prevents:
- Premature abstraction (YAGNI violations)
- Over-engineered type hierarchies
- Abstractions that don't carry their weight

### Module Organization

**Never use `mod.rs` files.** Always name modules directly.

```
# Good
src/
  generator/
    http.rs
    tcp.rs
    udp.rs

# Bad
src/
  generator/
    mod.rs      # Don't do this
    http/
      mod.rs    # Or this
```

Rationale: Direct naming makes the codebase easier to navigate and grep.

### Import Organization

**Never place `use` statements inside functions.** All imports at file top.

```rust
// Good
use std::collections::HashMap;
use crate::config::Config;

fn process() {
    let map = HashMap::new();
}

// Bad
fn process() {
    use std::collections::HashMap;  // Don't do this
    let map = HashMap::new();
}
```

### String Formatting

Include variable names in format strings for clarity.

```rust
// Good
format!("Processing item {index} of {total}")

// Avoid
format!("Processing item {} of {}", index, total)
```

### Warnings as Errors

All warnings are errors. Deprecation warnings MUST be treated as errors.

```toml
[workspace.lints.rust]
warnings = "deny"
```

### Documentation Philosophy

Document **why**, not **what**. The code shows what it does; comments explain
why it does it that way.

```rust
// Good - explains why
// Why not use fetch_sub? That function overflows at the zero boundary
// and we don't want the peer count to suddenly be u32::MAX.
let mut old = self.peers.load(Ordering::Relaxed);

// Bad - restates what the code does
// Decrement the counter
counter -= 1;
```

### Internal API Stability

**Lading does not care about internal backward compatibility.**

- ALL internal APIs can be changed freely
- None of this project's APIs are used externally
- The ONLY backward compatibility that matters is end-user configuration files

Do not add:
- Unnecessary `Option` types for "maybe later" fields
- Fallback logic for internal API changes
- Deprecated function wrappers

## Consequences

### Positive

- **Easy onboarding**: Follow the pattern, don't learn the type system
- **Greppable code**: Find anything with simple text search
- **Low abstraction overhead**: No need to understand inheritance/traits to contribute
- **Freedom to refactor**: Internal APIs can change without concern

### Negative

- **Code duplication**: Some code is intentionally repeated
- **Larger codebase**: Duplication increases line count
- **Missing abstractions**: Sometimes the 4th repetition reveals the 3rd should have abstracted

### Neutral

- Code reviews focus on pattern consistency
- Abstractions are introduced deliberately, not preemptively

## Style Enforcement

Automated via `ci/validate`:
- `cargo fmt` for formatting
- `cargo clippy` for linting
- Custom lints via `ast-grep` for project-specific rules

## Alternatives Considered

### Heavy abstraction

Use traits and generics extensively. Rejected because:
- Higher barrier to contribution
- Harder to understand without IDE support
- Abstractions often don't generalize as expected

### No style guide

Let contributors choose their style. Rejected because:
- Inconsistent codebase is harder to navigate
- Code review becomes style debates
- New contributors don't know what's preferred

### External style guide

Adopt an existing Rust style guide. We do follow Rust conventions but add
project-specific rules (mod.rs, import placement) that standard guides don't cover.

## References

- `ci/validate` - Style enforcement
- `ci/custom_lints` - Project-specific lint rules
- Workspace `Cargo.toml` - Lint configuration
- ADR-008: Dependency Philosophy (related: implement over import)
