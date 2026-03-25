# ADR-004: No-Panic Error Handling Policy

## Status

**Accepted**

## Date

2024-01-01 (formalized from existing design)

## Context

Lading runs performance experiments that can take significant time (minutes to hours).
A panic during an experiment:

1. Loses all collected data up to that point
2. Leaves the target process in an undefined state
3. May corrupt output files
4. Wastes engineer time waiting for failed experiments

Additionally, panics in performance tooling can mask issues in targets or create
false signals.

## Decision

**Lading MUST NOT panic.** All errors must be handled gracefully with controlled
shutdowns.

### Implementation Rules

1. **No `.unwrap()` or `.expect()`**: These panic on `None`/`Err`. Use `?` operator
   or explicit error handling instead.

2. **No `panic!()` macro**: Obviously prohibited.

3. **No `unreachable!()` without proof**: If code is truly unreachable, it should
   be proven via types or documented with strong justification.

4. **No `assert!()` in non-test code**: Use `debug_assert!()` for invariant checks
   that should never fail, or return errors for runtime validation.

5. **Propagate errors with `Result<T, E>`**: All fallible operations return Result.
   Errors propagate up to where they can be meaningfully handled.

6. **Graceful shutdown on fatal errors**: When errors cannot be recovered, lading
   shuts down cleanly:
   - Stops generators
   - Terminates target process
   - Flushes telemetry
   - Reports error to user

### Acceptable Exceptions

Panics are acceptable ONLY for:

- **Memory corruption indicators**: If data structures are impossibly inconsistent,
  continuing could cause worse problems
- **Test code**: Tests can panic to indicate failure
- **Truly impossible states**: Where the type system cannot encode the invariant
  and the state indicates a bug in lading itself

Even these should be rare and well-documented.

### Linting Enforcement

```toml
[workspace.lints.clippy]
unwrap_used = "deny"
expect_used = "deny"
panic = "deny"
```

## Consequences

### Positive

- **Reliability**: Experiments complete or fail gracefully
- **Data preservation**: Partial results are saved on error
- **Clean target lifecycle**: Targets are properly terminated
- **User experience**: Clear error messages instead of stack traces
- **Debuggability**: Error chains provide context

### Negative

- **Verbosity**: Error handling code is more verbose than `.unwrap()`
- **Type complexity**: Error types must be defined and propagated
- **Cognitive load**: Must think about failure modes everywhere

### Neutral

- Forces explicit consideration of error cases
- Makes error handling visible in code review

## Alternatives Considered

### Panic with catch_unwind

Use `std::panic::catch_unwind` at boundaries. Rejected because:
- Doesn't guarantee cleanup
- Not all panics are catchable
- Adds complexity without solving root cause

### Panic with recovery hooks

Register cleanup handlers for panics. Rejected because:
- Cleanup may not complete
- State may be inconsistent
- Harder to reason about

### Allow panics for "impossible" cases

Use `.unwrap()` where errors "shouldn't happen". Rejected because:
- "Shouldn't happen" often does in production
- Makes code review harder (which unwraps are safe?)
- Slippery slope to more unwraps

## References

- `lading/src/signals.rs` - Graceful shutdown handling
- Workspace `Cargo.toml` - Clippy lint configuration
- ADR-005: Performance-First Design (error handling must also be fast)
