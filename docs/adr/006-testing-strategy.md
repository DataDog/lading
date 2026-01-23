# ADR-006: Testing Strategy - Property Tests and Formal Proofs

## Status

**Accepted**

## Date

2024-01-01 (formalized from existing design)

## Context

Lading is used to make performance claims. Incorrect behavior in lading invalidates
those claims. Traditional unit tests are insufficient because:

1. They test specific examples, not properties
2. They miss edge cases the author didn't think of
3. They don't prove correctness, only demonstrate it for chosen inputs

The throttle is especially critical: if throttling is wrong, all throughput claims
made using lading are invalid.

## Decision

**Proofs are more predictive than property tests are more predictive than unit
tests which are the exception. If a proof will not work because of kani
limitations, write a property test. Critical components _require_ formal proofs.**

### Testing Hierarchy

1. **Formal Proofs (Kani)** - For critical components that must be correct
   - Throttling algorithms (MUST be correct)
   - Core payload generation invariants
   - Fundamental algorithms with correctness requirements

2. **Property Tests (proptest)** - Default for all testable code
   - Test invariants, not specific examples
   - Generate random inputs within constraints
   - Shrink failures to minimal reproductions

3. **Unit Tests** - Only for simple pure functions
   - Parsing specific formats
   - Simple transformations
   - Cases where property isn't obvious

### Invariant Documentation Requirements

**Both Kani proofs and property tests REQUIRE an explanation of what invariant is
being checked.** Use formal mathematical language where possible.

#### Formal Language Patterns

**Quantified statements** (preferred):
- "∀ request ≤ max_capacity: capacity' = capacity - request"
- "For all x such that P(x), Q(x) holds"

**Interval bounds**:
- "Shannon entropy for bytes is bounded [0, 8]"
- "x ∈ [a, b]" or "a ≤ x ≤ b"

**Implications** (if...then):
- "If capacity < request ≤ max_capacity ∧ ticks ≤ INTERVAL, then slop > 0"

**Arithmetic relationships**:
- "capacity' = capacity - request" (new value equals expression of old)
- "slop = INTERVAL_TICKS - ticks_in_interval"

**Order invariants**:
- "reset_capacity must never exceed maximum_capacity"
- "reset_capacity ≤ maximum_capacity"

#### Kani Proof Documentation

Every proof must have a doc comment stating the invariant as a formal claim:

```rust
/// ∀ request > maximum_capacity: request(ticks, request) returns Err
///
/// Capacity requests that exceed the throttle's maximum always error,
/// regardless of elapsed time or current capacity state.
#[kani::proof]
fn request_too_large_always_errors() {
    let maximum_capacity: NonZeroU32 = kani::any();
    let request: u32 = kani::any_where(|r: &u32| *r > maximum_capacity.get());
    let ticks_elapsed: u64 = kani::any();

    let res = valve.request(ticks_elapsed, request);
    kani::assert(res.is_err(), "Requests > max_capacity must always fail");
}
```

```rust
/// ∀ request ≤ max_capacity, ∀ ticks ≤ INTERVAL_TICKS:
///   slop = 0 ∧ capacity' = max_capacity - request
///
/// If a request fits within capacity and we're in the first interval,
/// the request succeeds immediately and capacity decreases by exactly
/// the request amount.
#[kani::proof]
fn request_in_cap_interval() { ... }
```

#### Property Test Documentation

Property tests should document the mathematical property being verified:

```rust
proptest! {
    /// Shannon entropy is bounded [0, 8] for byte data.
    /// H(X) ∈ [0, log₂(256)] = [0, 8] bits
    #[test]
    fn entropy_bounded(data: Vec<u8>) {
        let entropy = shannon_entropy(&data);
        prop_assert!(entropy >= 0.0, "H(X) ≥ 0");
        prop_assert!(entropy <= 8.0, "H(X) ≤ 8");
    }

    /// Entropy is order-invariant: H(sort(X)) = H(X)
    ///
    /// Shannon entropy H = -Σ p(x) * log₂(p(x)) depends only on frequency
    /// counts, not element order. Sorting preserves frequencies.
    #[test]
    fn order_invariant(mut data in prop::collection::vec(any::<u8>(), 1..1000)) {
        let original = shannon_entropy(&data);
        data.sort();
        let sorted = shannon_entropy(&data);
        prop_assert!((original - sorted).abs() < 1e-10);
    }
}
```

### Critical Components Requiring Proofs

The throttle is the primary example. From `lading_throttle/src/stable.rs`:

```rust
#[cfg(kani)]
mod proofs {
    #[kani::proof]
    fn capacity_request_never_exceeds_maximum() {
        // Prove that capacity requests are bounded
    }

    #[kani::proof]
    fn wait_time_is_bounded() {
        // Prove that wait times don't overflow
    }
}
```

### Property Test Configuration

**Local development** (fast feedback):
```bash
PROPTEST_CASES=64
PROPTEST_MAX_SHRINK_ITERS=2048
```

**CI** (thorough testing):
```bash
PROPTEST_CASES=512
PROPTEST_MAX_SHRINK_ITERS=10000
PROPTEST_MAX_SHRINK_TIME=60000
```

All tests respect environment variables - no hardcoded `#![proptest_config(...)]`.

### Naming Conventions

- Don't prefix test names with `test_` - they're obviously tests
- Don't prefix generators with `prop_` - they're obviously property tests
- Use descriptive names explaining the property being tested

```rust
// Good
#[test]
fn divide_preserves_rate_limiting_behavior() { }

// Avoid
#[test]
fn test_divide() { }
```

## Consequences

### Positive

- **Higher confidence**: Properties proven, not just demonstrated
- **Edge case coverage**: Random generation finds cases humans miss
- **Regression prevention**: Properties catch regressions unit tests miss
- **Formal documentation**: Invariant doc comments serve as precise specifications
- **Mathematical rigor**: Formal language eliminates ambiguity in requirements

### Negative

- **Learning curve**: Property testing requires different thinking
- **Slower tests**: More test cases take longer to run
- **Shrinking complexity**: Minimal failure cases can be hard to understand
- **Kani limitations**: Not all code can be proven with Kani

### Neutral

- Property tests require thinking about invariants upfront
- Kani proofs require understanding formal verification concepts

## Implementation Guidelines

### Writing Property Tests

Always document the invariant being tested using formal language:

```rust
proptest! {
    /// ∀ sequence of requests R: Σ granted(r) ≤ capacity
    ///
    /// The total capacity granted across any sequence of requests never
    /// exceeds the throttle's configured maximum capacity.
    #[test]
    fn throttle_never_allows_more_than_capacity(
        capacity in 1u64..1_000_000,
        requests in prop::collection::vec(1u64..1000, 1..100),
    ) {
        let throttle = Throttle::new(capacity);
        let mut total_granted = 0;

        for request in requests {
            if let Some(granted) = throttle.try_acquire(request) {
                total_granted += granted;
            }
        }

        prop_assert!(total_granted <= capacity, "Σ granted ≤ capacity");
    }
}
```

### Writing Kani Proofs

Always document the invariant with a formal statement of what is being proven:

```rust
#[cfg(kani)]
mod proofs {
    use super::*;

    /// ∀ ticks < u64::MAX/2, ∀ interval > 0:
    ///   calculate_interval(ticks, interval) terminates without panic
    ///
    /// Interval calculation is total (always terminates) and never overflows
    /// for inputs within the specified bounds.
    #[kani::proof]
    #[kani::unwind(10)]
    fn interval_calculation_never_overflows() {
        let ticks: u64 = kani::any();
        let interval: u64 = kani::any();

        kani::assume(interval > 0);
        kani::assume(ticks < u64::MAX / 2);

        let result = calculate_interval(ticks, interval);
        // Kani verifies this is total (no panic, no infinite loop)
    }
}
```

### When to Use Each Level

| Situation | Testing Approach |
|-----------|------------------|
| Throttle correctness | Kani proof |
| Payload generation invariants | Property test |
| Configuration parsing | Property test |
| Simple string formatting | Unit test |
| Error message content | Unit test |

## Alternatives Considered

### Unit tests only

Traditional approach. Rejected because:
- Misses edge cases
- Tests examples, not properties
- Doesn't scale to complex invariants

### Fuzzing only

Use cargo-fuzz for everything. Rejected because:
- Doesn't express properties explicitly
- Harder to reproduce failures
- Less structured than property tests

### TLA+ specifications

Formal specifications in TLA+. Rejected because:
- High learning curve
- Separate from implementation
- Kani proofs are closer to code

## References

- `lading_throttle/src/stable.rs` - Kani proofs for throttling
- `lading_payload/src/dogstatsd/common/tags.rs` - Property test examples
- `ci/test` - Test configuration
- `ci/kani` - Kani proof runner
- [proptest documentation](https://proptest-rs.github.io/proptest/)
- [Kani documentation](https://model-checking.github.io/kani/)
