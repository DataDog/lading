---
name: lading-optimize-review
description: Reviews optimization patches for lading using a 5-persona peer review system. Requires unanimous approval backed by benchmarks. Bugs discovered during review are valuable - invoke /lading-optimize-validate to validate them.
allowed-tools: Bash(cat:*) Bash(sample:*) Bash(samply:*) Bash(cargo:*) Bash(ci/*:*) Bash(hyperfine:*) Bash(*/payloadtool:*) Bash(tee:*) Read Write Edit Glob Grep Skill
---

# Optimization Patch Review

A rigorous 5-persona peer review system for optimization patches in lading. Requires unanimous approval backed by concrete benchmark data. **Duplicate Hunter persona prevents redundant work.**

## Role: Judge

**Review is the decision-maker. It does NOT record results.**

Review judges using benchmarks and 5-persona review, then returns a structured report.

## Valuable Outcomes

| Outcome | Value | Action |
|---------|-------|--------|
| **Optimization APPROVED** | Improvement validated | Merge |
| **Optimization REJECTED** | Learned where NOT to optimize | Record lesson |
| **Bug DISCOVERED** | Found correctness issue | Invoke `/lading-optimize-validate` |

**Finding bugs during optimization review is SUCCESS, not failure.**

---

## Phase 1: Benchmark Execution

### Step 1: Read Baseline Data

Read the baseline benchmark files captured:

- `/tmp/criterion-baseline.log` — micro-benchmark baseline
- `/tmp/baseline.json` — macro-benchmark timing baseline
- `/tmp/baseline-mem.txt` — macro-benchmark memory baseline

**If baseline data is missing -> REJECT. Baselines must be captured before any code change and before this gets invoked.**

### Step 2: Run Post-Change Micro-benchmarks

```bash
cargo criterion 2>&1 | tee /tmp/criterion-optimized.log
```

Note: Criterion automatically compares against the last run and reports percentage changes.

Compare results — look for "change:" lines showing improvement/regression.

Example output: `time: [1.2345 ms 1.2456 ms 1.2567 ms] change: [-5.1234% -4.5678% -4.0123%]`

### Step 3: Run Post-Change Macro-benchmarks

Use the config provided by the caller (stored in `$PAYLOADTOOL_CONFIG`):

```bash
cargo build --release --bin payloadtool
hyperfine --warmup 3 --runs 30 --export-json /tmp/optimized.json \
  "./target/release/payloadtool $PAYLOADTOOL_CONFIG"
./target/release/payloadtool "$PAYLOADTOOL_CONFIG" --memory-stats 2>&1 | tee /tmp/optimized-mem.txt
```

### Statistical Requirements

- Minimum 30 runs for hyperfine (`--runs 30`)
- Criterion handles statistical significance internally
- Time improvement >= 5% for significance
- Memory improvement >= 10% for significance
- Allocation reduction >= 20% for significance

### NO EXCEPTIONS

- "Test dependencies don't work" -> REJECT. Fix dependencies first.
- "Theoretically better" -> REJECT. Prove it with numbers.
- "Obviously an improvement" -> REJECT. Obvious is not measured.
- "Will benchmark later" -> REJECT. Benchmark now.

---

## Phase 2: Five-Persona Review

### 1. Duplicate Hunter (Checks for Redundant Work)
- [ ] Optimization not already in db.yaml
- [ ] File + technique combo not already approved
- [ ] No substantially similar optimization exists
- [ ] If duplicate found -> REJECT with "DUPLICATE: see <existing entry>"

### 2. Skeptic (Demands Proof)
- [ ] Baseline data verified present
- [ ] Post-change benchmarks executed with identical methodology
- [ ] Hot path verified via profiling (not just guessed)
- [ ] Micro threshold met (>=5% time)
- [ ] Macro threshold met (>=5% time OR >=10% mem OR >=20% allocs)
- [ ] Statistical significance confirmed (p<0.05 or criterion "faster")
- [ ] Improvement is real, not measurement noise
- [ ] Benchmark methodology sound (same config, same machine)

### 3. Conservative (Guards Correctness)
- [ ] Run `ci/validate` and validate that it passes completely
- [ ] No semantic changes to output
- [ ] **Determinism preserved** (same seed -> same output)
- [ ] No `.unwrap()` or `.expect()` added (lading MUST NOT panic)
- [ ] **No bugs introduced** (if bug found -> `/lading-optimize-validate`)
- [ ] Property tests exist for changed code

### 4. Rust Expert (Lading-Specific Patterns)
- [ ] No `mod.rs` files (per CLAUDE.md)
- [ ] All `use` statements at file top (not inside functions)
- [ ] Format strings use named variables (`"{index}"` not `"{}"`)
- [ ] Pre-computation in initialization, not hot paths
- [ ] Worst-case behavior considered, not just average-case
- [ ] No unnecessary cloning or allocation in hot paths

### 5. Greybeard (Simplicity Judge)
- [ ] Code still readable without extensive comments
- [ ] Complexity justified by measured improvement
- [ ] "Obviously fast" pattern, not clever trick
- [ ] Follows 3-repeat abstraction rule (no premature abstraction)
- [ ] Change is minimal - no scope creep

---

## Phase 3: Kani/Property Test Check

If the optimization touches critical code:

### For lading_throttle:
```bash
ci/kani lading_throttle
```

### For lading_payload:
```bash
ci/kani lading_payload
```

**Kani constraints:**
- Kani proofs are more complete but labor-intensive
- Kani may not compile complex code
- Kani runs are EXTREMELY slow for complex code

**If Kani fails to run:**
1. Document why (compilation error? timeout?)
2. Verify comprehensive property tests exist instead
3. This is acceptable - Kani feasibility varies

---

## Phase 4: Decision

| Outcome | Votes | Action |
|---------|-------|--------|
| **APPROVED** | 5/5 APPROVE | Return APPROVED report |
| **REJECTED** | Any REJECT | Return REJECTED report |
| **DUPLICATE** | Duplicate Hunter REJECT | Return DUPLICATE report |
| **BUG FOUND** | Correctness issue | Invoke `/lading-optimize-validate`, return BUG_FOUND report |

### Bug Triage

When issues are discovered, determine the correct action:

| Signal                                  | Is It a Bug? | Action                             |
| --------------------------------------- | ------------ | ---------------------------------- |
| ci/validate fails after optimization    | Possible bug | Invoke `/lading-optimize-validate` |
| Determinism broken                      | Possible bug | Invoke `/lading-optimize-validate` |
| Conservative finds correctness issue    | Possible bug | Invoke `/lading-optimize-validate` |
| Benchmark regression but correct output | NOT a bug    | REJECT optimization                |
| No improvement but correct output       | NOT a bug    | REJECT optimization                |

### When Bug Is Found

If review discovers a bug instead of an optimization:

```
/lading-optimize-validate
```

The validate skill will:
1. Confirm this is actually a bug (not just a failed optimization)
2. Attempt Kani proof (if feasible)
3. Create property test reproducing the bug
4. Verify the fix works
5. Record in its db.yaml

Then return here to return a BUG_FOUND report in Phase 6.

---

## Phase 5: Return Report

**Review does NOT record results and does NOT create files. Return a structured YAML report to the caller.**

Fill in the appropriate template and return the completed YAML:

| Verdict   | Template                         |
| --------- | -------------------------------- |
| approved  | `assets/approved.template.yaml`  |
| rejected  | `assets/rejected.template.yaml`  |
| duplicate | `assets/duplicate.template.yaml` |
| bug_found | `assets/bug-found.template.yaml` |

1. Read the appropriate template from the `assets/` directory
2. Fill in all placeholder values with actual data from the review
3. Return the filled-in report

---
