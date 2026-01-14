---
name: lading:optimize:review
description: Reviews optimization patches for lading using a 5-persona peer review system. Requires unanimous approval backed by benchmarks. Bugs discovered during review are valuable - invoke /lading:optimize:validate to validate them.
---

# Optimization Patch Review

A rigorous 5-persona peer review system for optimization patches in lading. Requires unanimous approval backed by concrete benchmark data. **Duplicate Hunter persona prevents redundant work.**

## CRITICAL: Recording is MANDATORY

**EVERY review outcome MUST be recorded as a beads issue with `opt-review` label.**

## Valuable Outcomes

| Outcome | Value | Action |
|---------|-------|--------|
| **Optimization APPROVED** | Improvement validated | Merge |
| **Optimization REJECTED** | Learned where NOT to optimize | Record lesson |
| **Bug DISCOVERED** | Found correctness issue | Invoke `/lading:optimize:validate` |

**Finding bugs during optimization review is SUCCESS, not failure.**

---

## Phase 0: Pre-flight

Run `/lading:preflight` first. Then check for duplicate work:

```bash
# List previous reviews
bd list --labels=opt-review

# List previous hunts
bd list --labels=opt-hunt
```

**Check for:**
- Same branch name already reviewed -> REJECT as "DUPLICATE"
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

### For payloadtool (end-to-end):

```bash
# Choose a config file (e.g., ci/fingerprints/json/lading.yaml)
CONFIG=ci/fingerprints/json/lading.yaml

# Baseline
git checkout origin/main
cargo build --release --bin payloadtool
hyperfine --warmup 3 --runs 30 --export-json /tmp/old.json \
  "./target/release/payloadtool $CONFIG"
./target/release/payloadtool "$CONFIG" --memory-stats 2>&1 | tee /tmp/old-mem.txt

# Optimized
git checkout -
cargo build --release --bin payloadtool
hyperfine --warmup 3 --runs 30 --export-json /tmp/new.json \
  "./target/release/payloadtool $CONFIG"
./target/release/payloadtool "$CONFIG" --memory-stats 2>&1 | tee /tmp/new-mem.txt
```

### For inner loops (criterion):

```bash
# Baseline
git checkout origin/main
cargo criterion --save-baseline before

# Optimized
git checkout -
cargo criterion --baseline before
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
- [ ] Branch not already in reviews.yaml
- [ ] File + technique combo not already approved
- [ ] No substantially similar optimization exists
- [ ] If duplicate found -> REJECT with "DUPLICATE: see <existing branch>"

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
- [ ] **No bugs introduced** (if bug found -> `/lading:optimize:validate`)
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
| **APPROVED** | 5/5 APPROVE | Merge, record success |
| **REJECTED** | Any REJECT | Record lesson, delete branch |
| **DUPLICATE** | Duplicate Hunter REJECT | Record as DUPLICATE, delete branch |
| **BUG FOUND** | Correctness issue | Invoke `/lading:optimize:validate` |

### When Bug Is Found

If review discovers a bug instead of an optimization:

```
/lading:optimize:validate
```

The validate skill will:
1. Attempt Kani proof (if feasible)
2. Create property test reproducing the bug
3. Verify the fix works
4. Record as a beads issue with `opt-validate` label

Then return here to record the finding as a SUCCESS (bug found).

---

## Phase 6: MANDATORY Recording

Create a beads issue with appropriate labels based on verdict:

For **APPROVED**:
```bash
bd create --type=task --labels=opt-review,verdict-approved \
  --title="Review: <branch> - APPROVED" \
  --description="$(cat <<'EOF'
## Review Verdict: APPROVED

- **Branch**: <branch name>

## Persona Votes
- Duplicate Hunter: APPROVE
- Skeptic: APPROVE
- Conservative: APPROVE
- Rust Expert: APPROVE
- Greybeard: APPROVE

## Measurements
- **Time**: <-X%>
- **Memory**: <-X%>
- **Allocations**: <-X%>

## Reason
<summary of why approved>

## Lesson
<pattern learned>
EOF
)"
```

For **REJECTED**:
```bash
bd create --type=task --labels=opt-review,verdict-rejected \
  --title="Review: <branch> - REJECTED" \
  --description="$(cat <<'EOF'
## Review Verdict: REJECTED

- **Branch**: <branch name>

## Persona Votes
- Duplicate Hunter: <APPROVE|REJECT>
- Skeptic: <APPROVE|REJECT>
- Conservative: <APPROVE|REJECT>
- Rust Expert: <APPROVE|REJECT>
- Greybeard: <APPROVE|REJECT>

## Reason
<why rejected - which persona(s) and why>

## Lesson
<what NOT to do next time>
EOF
)"
```

For **DUPLICATE**:
```bash
bd create --type=task --labels=opt-review,verdict-duplicate \
  --title="Review: <branch> - DUPLICATE" \
  --description="$(cat <<'EOF'
## Review Verdict: DUPLICATE

- **Branch**: <branch name>
- **Duplicate of**: <existing issue ID or branch>

## Reason
<explanation of duplication>
EOF
)"
```

For **BUG_FOUND**:
```bash
bd create --type=task --labels=opt-review,verdict-bug-found \
  --title="Review: <branch> - BUG FOUND" \
  --description="$(cat <<'EOF'
## Review Verdict: BUG_FOUND

- **Branch**: <branch name>
- **Bug Issue**: <issue ID from /lading:optimize:validate>

## Reason
<what bug was found>

## Lesson
<what was learned>
EOF
)"
```

---

## Usage

```
/lading:optimize:review
```
