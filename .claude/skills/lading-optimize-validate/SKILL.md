---
name: lading-optimize-validate
description: Validates discovered bugs with reproducing tests and validates fixes with regression tests. Called by other skills when bugs are found during optimization hunting. Creates property tests (proptest) and Kani proofs when feasible.
---

# Correctness Validation

When optimization hunting discovers a bug instead of an optimization opportunity, this skill validates the finding through tests. **No bug fix should be merged without a test that would have caught it.**

## When This Skill Is Called

Other skills invoke `/lading-optimize-validate` when:
- `/lading-optimize-hunt` discovers a bug instead of an optimization
- `/lading-optimize-rescue` finds broken code during salvage
- `/lading-optimize-review` identifies correctness issues during review

```
Optimization hunt discovers bug
            |
            v
    /lading-optimize-validate
            |
            +-- Attempt Kani proof (if feasible)
            +-- Create property test (proptest)
            +-- Validate fix works
            +-- Record in assets/db.yaml
            |
            v
    Return to calling skill with VALIDATED status
```

---

## Philosophy

> "A bug without a test is just an anecdote. A bug with a test is knowledge."

Finding bugs during optimization work is **valuable**, not a failure. But a bug fix without a reproducing test:
1. Can't prove the bug existed
2. Can't prove the fix works
3. Can regress silently later

**Every bug fix MUST include a test that fails before the fix and passes after.**

### Property Tests vs Unit Tests

Per lading's CLAUDE.md: **ALWAYS prefer property tests over unit tests.**

- Property tests (proptest) verify invariants across generated inputs
- Unit tests only check specific examples you thought of
- Property tests find edge cases you didn't anticipate

### Kani Proofs

**Kani provides exhaustive verification when feasible.**

Kani constraints:
- More labor-intensive to write
- May not compile complex code
- Runs EXTREMELY slowly for complex code
- When it works, it's more complete than property tests

**Approach: Try Kani first. Fall back to proptest if Kani fails.**

---

## Phase 1: Understand the Bug

### 1.1 Document the Bug
```yaml
bug:
  file: lading_payload/src/block.rs
  function: Cache::fixed
  discovered_by: hunt-optimization
  description: "Off-by-one in capacity calculation causes short read"
  impact: "Generated payloads truncated by 1 byte"
  root_cause: "Used < instead of <= in boundary check"
```

### 1.2 Identify Bug Category

| Category | Example | Test Strategy |
|----------|---------|---------------|
| **Off-by-one** | Wrong bounds | Property test: output length = expected |
| **Capacity error** | Wrong preallocation | Property test: no panic, correct size |
| **Determinism** | Non-reproducible output | Property test: same seed = same output |
| **Overflow** | Integer overflow | Kani proof (if feasible) or proptest |
| **Logic error** | Wrong condition | Property test with counterexample |
| **Panic path** | Unwrap on None | Property test: no panic for any input |

### 1.3 Find the Minimal Reproducer

Identify the smallest input that triggers the bug:
```rust
// What input demonstrates the bug?
let config = Config { size: 1024, seed: 42 };
let result = buggy_function(&config);
// Expected: 1024 bytes
// Actual: 1023 bytes (BUG: off by one)
```

---

## Phase 2: Attempt Kani Proof

**Try Kani first - it provides exhaustive verification.**

### 2.1 Check if Kani is Feasible

```bash
# For lading_throttle bugs
ci/kani lading_throttle

# For lading_payload bugs
ci/kani lading_payload
```

### 2.2 If Kani Works

Write a proof that demonstrates the invariant:

```rust
#[cfg(kani)]
mod proofs {
    use super::*;

    #[kani::proof]
    fn capacity_never_underflows() {
        let size: usize = kani::any();
        kani::assume(size <= MAX_REASONABLE_SIZE);

        let result = calculate_capacity(size);

        // The invariant that was violated
        kani::assert(result >= size, "Capacity must be >= requested size");
    }
}
```

### 2.3 If Kani Fails

Document why and proceed to property tests:

```yaml
kani_attempted: true
kani_result: FAILED
kani_reason: "Compilation error: unsupported feature XYZ"
# OR
kani_reason: "Timeout after 30 minutes on complex function"
```

---

## Phase 3: Create Property Test

### 3.1 Write Property Test That Fails on Buggy Code

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn output_length_equals_requested(size in 1usize..10000) {
            let config = Config { size, seed: 42 };
            let result = function_under_test(&config);

            // The property that was violated
            prop_assert_eq!(result.len(), size,
                "Output length {} != requested size {}", result.len(), size);
        }
    }
}
```

### 3.2 Test Naming Convention

Don't prefix with `test_` - they're obviously tests:
```
output_length_equals_requested
determinism_preserved_across_runs
no_panic_on_zero_size
capacity_at_least_requested
```

### 3.3 Verify Test Fails Before Fix

```bash
# Checkout code BEFORE fix
git stash
git checkout origin/main

# Run the new test - MUST FAIL
ci/test

# Return to fix branch
git checkout -
git stash pop
```

**If test passes on buggy code, the test doesn't reproduce the bug. Rewrite it.**

---

## Phase 4: Validate the Fix

### 4.1 Verify Test Passes After Fix

```bash
# On the fix branch
ci/test
```

### 4.2 Run Full Validation

```bash
# MANDATORY: Full validation suite
ci/validate
```

### 4.3 Run Kani (If Applicable)

```bash
# If the bug was in throttle or payload
ci/kani lading_throttle
ci/kani lading_payload
```

### 4.4 Verify Determinism

Determinism is verified via fingerprints. The same config (with fixed seed) must produce identical output:
```bash
CONFIG=ci/fingerprints/json/lading.yaml
./target/release/payloadtool "$CONFIG" --fingerprint > /tmp/run1.txt
./target/release/payloadtool "$CONFIG" --fingerprint > /tmp/run2.txt
diff /tmp/run1.txt /tmp/run2.txt  # Must be identical
```

**Note:** Seed is specified in the config file, not as a CLI flag.

---

## Phase 5: Document and Record

### 5.1 Commit Message Format

```bash
git commit -m "$(cat <<'EOF'
fix(lading_payload): correct off-by-one in capacity calculation

Bug: calculate_capacity returned size-1 instead of size
Fix: Changed < to <= in boundary check

Test: Property test output_length_equals_requested fails before, passes after
Kani: Proof capacity_never_underflows added (or: Kani not feasible - timeout)

Discovered-by: /lading-optimize-hunt
Validated-by: /lading-optimize-validate

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

### 5.2 MANDATORY: Update db.yaml

1. Add entry to `assets/db.yaml` index
2. Create detailed file in `assets/db/` directory

**assets/db.yaml entry:**
```yaml
entries:
  - file: <file path>
    function: <function name>
    category: <off-by-one|capacity|determinism|overflow|logic|panic>
    status: validated
    detail_file: assets/db/<file-function>.yaml
```

**assets/db/<file-function>.yaml:**
```yaml
file: <file path>
function: <function name>
category: <off-by-one|capacity|determinism|overflow|logic|panic>
discovered_by: <hunt|rescue|review>
original_branch: <branch name>
date: <YYYY-MM-DD>
bug_description: |
  <what was wrong>
kani:
  attempted: <yes|no>
  result: <success|failed|n/a>
  reason: <if failed, why>
tests_added:
  - name: <test name>
    type: <proptest|kani>
verification:
  test_fails_before_fix: yes
  test_passes_after_fix: yes
  ci_validate_passes: yes
  determinism_verified: yes
lessons: |
  <what was learned>
```

---

## Phase 6: Return to Calling Skill

After validation complete, return status to the calling skill:

```yaml
validation_result:
  status: VALIDATED  # or INVALID, NEEDS_WORK
  bug_confirmed: true
  fix_confirmed: true
  kani_proof_added: true  # or false
  property_tests_added: 1
  ci_validate_passes: true
  determinism_preserved: true
  ready_for_merge: true
```

The calling skill should:
1. Record the bug discovery as a SUCCESS (not failure)
2. Include validation status in the review
3. Proceed with merge if VALIDATED

---

## Usage

```
/lading-optimize-validate
```

---

## Checklist

Before returning VALIDATED:

- [ ] Bug documented with root cause
- [ ] Kani proof attempted (document if failed)
- [ ] Property test written (proptest, not unit test)
- [ ] Test FAILS on buggy code (verified)
- [ ] Test PASSES on fixed code (verified)
- [ ] `ci/validate` passes
- [ ] Determinism verified (same seed = same output)
- [ ] No `.unwrap()` or `.expect()` in fix (use Result)
- [ ] Recorded in assets/db.yaml with detail file
