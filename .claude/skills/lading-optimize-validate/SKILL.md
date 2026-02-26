---
name: lading-optimize-validate
description: Validates discovered bugs with reproducing tests and validates fixes with regression tests. Called by review when bugs are found during optimization, or by hunt when bugs are found during analysis. Creates property tests (proptest) and Kani proofs when feasible.
allowed-tools: Bash(ci/*:*) Bash(cargo:*) Bash(*/payloadtool:*) Bash(diff:*) Read Write Edit Glob Grep
---

# Correctness Validation

When optimization hunting discovers a bug instead of an optimization opportunity, this skill validates the finding through tests. **No bug fix should be merged without a test that would have caught it.**

## When This Skill Is Called

### Primary Caller: Review

`/lading-optimize-review` invokes this skill when:
- The Skeptic persona finds benchmark anomalies suggesting correctness issues
- The Conservative persona finds correctness violations (broken determinism, semantic changes)
- ci/validate fails after an optimization that appeared correct

### Secondary Caller: Hunt

`/lading-optimize-hunt` invokes this skill when:
- A bug is found during code analysis (Phase 2), before any optimization is attempted
- ci/validate repeatedly fails on what appears to be a pre-existing bug (Phase 4)

```
Review or Hunt discovers potential bug
            |
            v
    /lading-optimize-validate
            |
            +-- Confirm this is actually a bug (Phase 1.0)
            +-- Attempt Kani proof (if feasible)
            +-- Create property test (proptest)
            +-- Validate fix works
            +-- Record in .claude/skills/lading-optimize-hunt/assets/db.yaml
            |
            v
    Return to calling skill with status
```

### Bug vs. Failed Optimization Triage

Not every failure is a bug. Use this table before proceeding:

| Signal | Bug? | Action |
|--------|------|--------|
| ci/validate fails on changed code | Possible | Investigate — may be bug or bad optimization |
| ci/validate fails on unchanged code | Likely pre-existing bug | Proceed with validation |
| Determinism broken after optimization | Possible | Investigate — optimization may have introduced it |
| Determinism broken on main branch | Pre-existing bug | Proceed with validation |
| Benchmark regression but correct output | NOT a bug | Return `NOT_A_BUG` — this is a failed optimization |
| No improvement but correct output | NOT a bug | Return `NOT_A_BUG` — this is a failed optimization |
| Wrong output regardless of optimization | Bug | Proceed with validation |
| Panic on valid input | Bug | Proceed with validation |

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

### 1.0 Confirm This Is a Bug

Before investing in test infrastructure, verify this is actually a correctness bug:

| Check | Expected | If Not Met |
|-------|----------|-----------|
| Output is incorrect for valid input | Yes | If output is correct, return `NOT_A_BUG` |
| Issue reproduces without the optimization | Check main branch | If only with optimization, it's a bad optimization — return `NOT_A_BUG` |
| Issue is a correctness problem, not perf | Yes | If purely performance, return `NOT_A_BUG` |
| Issue is in lading code, not test harness | Yes | If test harness, fix test instead |

**If this is NOT a bug**, return immediately:

```yaml
validation_result:
  status: NOT_A_BUG
  reason: "<explanation — e.g., failed optimization, not correctness issue>"
```

The calling skill (review or hunt) will handle this as a rejected optimization, not a bug.

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

**Important:** Ensure the test fails on buggy code before applying the fix. You may need to temporarily revert your fix to verify.

```bash
# Run the new test - should FAIL on buggy code
ci/test
```

**If test passes on buggy code, the test doesn't reproduce the bug. Rewrite it.**

---

## Phase 4: Validate the Fix

### 4.1 Verify Test Passes After Fix

```bash
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

### MANDATORY: Update db.yaml

1. Add entry to `.claude/skills/lading-optimize-hunt/assets/db.yaml` index
2. Create detailed file in `.claude/skills/lading-optimize-hunt/assets/db/` directory

**`.claude/skills/lading-optimize-hunt/assets/db.yaml` entry:**
```yaml
entries:
  - file: <file path>
    function: <function name>
    category: <off-by-one|capacity|determinism|overflow|logic|panic>
    status: validated
    detail_file: .claude/skills/lading-optimize-hunt/assets/db/<file-function>.yaml
```

**`.claude/skills/lading-optimize-hunt/assets/db/<file-function>.yaml`:**
```yaml
file: <file path>
function: <function name>
category: <off-by-one|capacity|determinism|overflow|logic|panic>
discovered_by: <hunt|rescue|review>
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
  status: VALIDATED  # or INVALID, NEEDS_WORK, NOT_A_BUG
  bug_confirmed: true
  fix_confirmed: true
  kani_proof_added: true  # or false
  property_tests_added: 1
  ci_validate_passes: true
  determinism_preserved: true
  ready_for_merge: true
```

### Status Values

| Status | Meaning |
|--------|---------|
| `VALIDATED` | Bug confirmed, fix verified, tests added |
| `INVALID` | Reported issue could not be reproduced |
| `NEEDS_WORK` | Bug confirmed but fix is incomplete |
| `NOT_A_BUG` | Issue is a failed optimization, not a correctness bug |

The calling skill should:
1. Record the bug discovery as a SUCCESS (not failure)
2. Include validation status in the review
3. Proceed with merge if VALIDATED

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
- [ ] Recorded in `.claude/skills/lading-optimize-hunt/assets/db.yaml` with detail file
