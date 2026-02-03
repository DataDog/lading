---
name: lading-optimize-review
description: Reviews optimization patches for lading using a 5-persona peer review system. Requires unanimous approval backed by benchmarks. Bugs discovered during review are valuable - invoke /lading-optimize-validate to validate them.
allowed-tools: Bash(cat:*) Bash(ci/*:*) Read Write Edit Glob Grep Skill
---

# Optimization Patch Review

A rigorous 5-persona peer review system for optimization patches in lading. Requires unanimous approval backed by concrete benchmark data. **Duplicate Hunter persona prevents redundant work.**

## CRITICAL: Recording is MANDATORY

**EVERY review outcome MUST be recorded in db.yaml.**

## Valuable Outcomes

| Outcome | Value | Action |
|---------|-------|--------|
| **Optimization APPROVED** | Improvement validated | Merge |
| **Optimization REJECTED** | Learned where NOT to optimize | Record lesson |
| **Bug DISCOVERED** | Found correctness issue | Invoke `/lading-optimize-validate` |

**Finding bugs during optimization review is SUCCESS, not failure.**

---

## Phase 0: Pre-flight

Run `/lading-preflight` first. Then check for duplicate work:

```bash
# Check previous reviews
cat .claude/skills/lading-optimize-review/assets/db.yaml

# Check previous hunts
cat .claude/skills/lading-optimize-hunt/assets/db.yaml
```

**Check for:**
- Same optimization already reviewed -> REJECT as "DUPLICATE"
- Same file + technique already approved -> REJECT as "DUPLICATE"

---

## Phase 1: Validation Gate

**MANDATORY: ci/validate must pass before any review proceeds.**

```bash
ci/validate
```

**If ci/validate fails -> REJECT immediately. No exceptions.**

---

## Phase 2: Measurement

**Review existing benchmark data.** The optimization should already have benchmark results. If missing, request them or REJECT.

**The optimization being reviewed MUST provide benchmark data.** Review should focus on analyzing existing data, not generating new benchmarks.

### Expected Benchmark Data

The optimization should include:

**Micro-benchmarks (criterion):**
- Benchmark name and throughput change
- Example: `syslog_100MiB: +42.0% throughput (481 -> 683 MiB/s)`

**Macro-benchmarks (payloadtool with hyperfine):**
- Time change with absolute values
- Memory change with absolute values
- Allocation count change with absolute values
- Example:
  ```
  Time: -14.5% (8.3 ms -> 7.1 ms)
  Memory: -35.8% (6.17 MiB -> 3.96 MiB)
  Allocations: -49.3% (67,688 -> 34,331)
  ```

### Statistical Requirements
- Minimum 30 runs for hyperfine (`--runs 30`)
- Criterion handles statistical significance internally
- Time improvement >= 5% for significance
- Memory improvement >= 10% for significance
- Allocation reduction >= 20% for significance

### NO EXCEPTIONS

**If benchmark data is missing -> REJECT. Period.**

- "Test dependencies don't work" -> REJECT. Fix deps first.
- "Theoretically better" -> REJECT. Prove it with numbers.
- "Obviously an improvement" -> REJECT. Obvious is not measured.
- "Will benchmark later" -> REJECT. Benchmark now.

---

## Phase 3: Five-Persona Review

### 1. Duplicate Hunter (Checks for Redundant Work)
- [ ] Optimization not already in reviews.yaml
- [ ] File + technique combo not already approved
- [ ] No substantially similar optimization exists
- [ ] If duplicate found -> REJECT with "DUPLICATE: see <existing entry>"

### 2. Skeptic (Demands Proof)
- [ ] Hot path verified via profiling (not just guessed)
- [ ] Benchmark improvement meets thresholds (5% time, 10% mem, 20% allocs)
- [ ] Statistical significance confirmed (p<0.05 or criterion "faster")
- [ ] Improvement is real, not measurement noise

### 3. Conservative (Guards Correctness)
- [ ] `ci/validate` passes completely
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

## Phase 4: Kani/Property Test Check

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

## Phase 5: Decision

| Outcome | Votes | Action |
|---------|-------|--------|
| **APPROVED** | 5/5 APPROVE | Record success |
| **REJECTED** | Any REJECT | Record lesson |
| **DUPLICATE** | Duplicate Hunter REJECT | Record as DUPLICATE |
| **BUG FOUND** | Correctness issue | Invoke `/lading-optimize-validate` |

### When Bug Is Found

If review discovers a bug instead of an optimization:

```
/lading-optimize-validate
```

The validate skill will:
1. Attempt Kani proof (if feasible)
2. Create property test reproducing the bug
3. Verify the fix works
4. Record in its db.yaml

Then return here to record the finding as BUG_FOUND in Phase 6.

---

## Phase 6: MANDATORY Recording

### Update db.yaml

1. Add entry to `assets/db.yaml` index
2. Create detailed file in `assets/db/` directory

**assets/db.yaml entry:**
```yaml
entries:
  - id: <descriptive-name>
    verdict: <approved|rejected|duplicate|bug_found>
    file: assets/db/<review-name>.yaml
```

**assets/db/<review-name>.yaml** for APPROVED:
```yaml
id: <descriptive-name>
target: <file:function>
technique: <technique>
verdict: approved
date: <YYYY-MM-DD>
votes:
  duplicate_hunter: approve
  skeptic: approve
  conservative: approve
  rust_expert: approve
  greybeard: approve
measurements:
  time: <-X%>
  memory: <-X%>
  allocations: <-X%>
reason: |
  <summary of why approved>
lessons: |
  <pattern learned>
```

**assets/db/<review-name>.yaml** for REJECTED:
```yaml
id: <descriptive-name>
target: <file:function>
technique: <technique>
verdict: rejected
date: <YYYY-MM-DD>
votes:
  duplicate_hunter: <approve|reject>
  skeptic: <approve|reject>
  conservative: <approve|reject>
  rust_expert: <approve|reject>
  greybeard: <approve|reject>
reason: |
  <why rejected - which persona(s) and why>
lessons: |
  <what NOT to do next time>
```

**assets/db/<review-name>.yaml** for DUPLICATE:
```yaml
id: <descriptive-name>
target: <file:function>
technique: <technique>
verdict: duplicate
date: <YYYY-MM-DD>
duplicate_of: <existing entry>
reason: |
  <explanation of duplication>
```

**assets/db/<review-name>.yaml** for BUG_FOUND:
```yaml
id: <descriptive-name>
target: <file:function>
verdict: bug_found
date: <YYYY-MM-DD>
validation_file: <path to validate db entry>
reason: |
  <what bug was found>
lessons: |
  <what was learned>
```

---

## Usage

```
/lading-optimize-review
```
