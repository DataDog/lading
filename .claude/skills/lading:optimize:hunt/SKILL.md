---
name: lading:optimize:hunt
description: Systematic optimization hunter for lading. Finds memory optimizations AND bugs - both are valuable. Run /lading:optimize:validate when bugs are discovered.
---

# Optimization Hunt

Systematically explores the lading codebase, implements optimizations, validates with benchmarks. **Finding bugs is equally valuable as finding optimizations.**

## Valuable Outcomes

| Outcome | Value | Action |
|---------|-------|--------|
| **Optimization works** | Memory/time improved | Submit to `/lading:optimize:review` |
| **Optimization fails** | Learned cold path | Record, next target |
| **Bug discovered** | Found correctness issue | Invoke `/lading:optimize:validate` |

**All three outcomes build institutional knowledge.**

---

## Phase 0: Pre-flight

Run `/lading:preflight` first. Then check what's already been done:

```bash
# List previous hunts
bd list --labels=opt-hunt

# List previous reviews (to check for approved optimizations)
bd list --labels=opt-review
```

**If a target/technique combination exists in either listing, SKIP IT.**

---

## Phase 1: Select Target

### Target Sources

Use profiling data when available:
```bash
# CPU profiling (Mac)
sample <pid> 10 -file /tmp/profile.txt

# Or with samply (if installed)
samply record ./target/release/payloadtool ci/fingerprints/json/lading.yaml

# Memory profiling (use existing fingerprint configs)
./target/release/payloadtool ci/fingerprints/json/lading.yaml --memory-stats
```

**Note:** payloadtool takes a config YAML as its first argument. See `ci/fingerprints/*/lading.yaml` for examples. Config controls `seed`, `maximum_prebuild_cache_size_bytes`, and generator type.

Otherwise, check pending hunt issues or pick from hot subsystems:

| Crate | Hot Paths |
|-------|-----------|
| `lading_payload` | Block generation, cache, payload construction |
| `lading_throttle` | Capacity calculation, rate limiting |
| `lading` | Generators, blackholes, target management |

---

## Phase 2: Analyze Target

### Identify Opportunity Type

| Pattern | Technique | Bug Risk |
|---------|-----------|----------|
| `Vec::new()` + repeated push | `Vec::with_capacity(n)` | None |
| `String::new()` + repeated push | `String::with_capacity(n)` | None |
| `HashMap::new()` hot insert | `HashMap::with_capacity(n)` | None |
| Allocation in hot loop | Move outside loop | Lifetime issues |
| Clone where borrow works | Use reference | Lifetime complexity |
| Large struct by value | Box or reference | Nil/lifetime risk |
| Unbounded growth | Bounded buffer | Semantic change |

**Watch for bugs while analyzing - they're valuable findings.**

### Lading-Specific Concerns

- **Determinism**: Any optimization must preserve deterministic output
- **Pre-computation**: Prefer moving work to initialization over runtime
- **Worst-case behavior**: Optimize for worst-case, not average-case

---

## Phase 3: Implement

```bash
git checkout main && git pull
git checkout -b opt/<crate>-<technique>
```

Make ONE change. Commit:
```bash
git commit -m "opt: <description>

Hypothesis: <expected improvement>
Technique: <prealloc|cache|avoid-clone|etc>
"
```

---

## Phase 4: Benchmark

### For payloadtool (end-to-end):

```bash
# Choose a config file (e.g., ci/fingerprints/json/lading.yaml)
CONFIG=ci/fingerprints/json/lading.yaml

# Baseline
git stash && git checkout main
cargo build --release --bin payloadtool
hyperfine --warmup 3 --export-json /tmp/baseline.json \
  "./target/release/payloadtool $CONFIG"
./target/release/payloadtool "$CONFIG" --memory-stats 2>&1 | tee /tmp/baseline-mem.txt

# Optimized
git checkout - && git stash pop
cargo build --release --bin payloadtool
hyperfine --warmup 3 --export-json /tmp/optimized.json \
  "./target/release/payloadtool $CONFIG"
./target/release/payloadtool "$CONFIG" --memory-stats 2>&1 | tee /tmp/optimized-mem.txt
```

### For inner loops (micro-benchmarks):

```bash
# Baseline
git stash && git checkout main
cargo criterion --save-baseline before

# Optimized
git checkout - && git stash pop
cargo criterion --baseline before
```

### Decision Point

| Result | Action |
|--------|--------|
| Time improved >=5% | Proceed to `/lading:optimize:review` |
| Memory reduced >=10% | Proceed to `/lading:optimize:review` |
| Allocations reduced >=20% | Proceed to `/lading:optimize:review` |
| No change or regression | Record FAILURE, next target |
| **ci/validate fails** | Might be a **BUG** |
| **Determinism broken** | Might be a **BUG** |

---

## Phase 5: Handle Bug Discovery

If during hunting you discover a bug (not an optimization):

### Invoke Correctness Validation
```
/lading:optimize:validate
```

This skill will:
1. Attempt Kani proof first (if feasible)
2. Fall back to property test if Kani fails
3. Verify fix works
4. Record as a beads issue with `opt-validate` label

### After Validation

Return here and record as BUG_FOUND (a success!) using beads:
```bash
bd create --type=task --labels=opt-hunt,result-bug-found \
  --title="Hunt: <target> - BUG FOUND" \
  --description="$(cat <<'EOF'
## Hunt Result: BUG_FOUND

- **Target**: <file:function>
- **Bug Issue**: <bug issue ID from /lading:optimize:validate>
- **Lesson**: <what was learned>

Bug discovery is a SUCCESS - institutional knowledge gained.
EOF
)"
```

Then **continue hunting** - don't stop.

---

## Phase 6: Validate (MANDATORY)

Before proceeding to review, ALL changes must pass:

```bash
ci/validate
```

**No exceptions. If ci/validate fails, fix the issue before continuing.**

### Kani Proofs (When Touching Critical Code)

If your optimization touches `lading_throttle` or `lading_payload`:

```bash
ci/kani lading_throttle  # If throttle was modified
ci/kani lading_payload   # If payload was modified
```

Kani is slow and may not compile complex code. If it fails:
1. Document why Kani couldn't run
2. Ensure comprehensive property tests exist instead

---

## Phase 7: Record & Continue

### MANDATORY: Create beads issue

For **SUCCESS** (optimization validated):
```bash
bd create --type=task --labels=opt-hunt,result-success \
  --title="Hunt: <target> - SUCCESS" \
  --description="$(cat <<'EOF'
## Hunt Result: SUCCESS

- **Target**: <file:function>
- **Branch**: <branch name>
- **Technique**: <preallocation|avoid-clone|cache|etc>

## Measurements
- **Time**: <-X% or ~>
- **Memory**: <-X% or ~>
- **Allocations**: <-X% or ~>

## Lesson
<pattern learned>

Ready for /lading:optimize:review
EOF
)"
```

For **FAILURE** (no improvement measured):
```bash
bd create --type=task --labels=opt-hunt,result-failure \
  --title="Hunt: <target> - FAILURE" \
  --description="$(cat <<'EOF'
## Hunt Result: FAILURE

- **Target**: <file:function>
- **Technique**: <what was tried>

## Lesson
<why it didn't work - cold path? already optimized?>

This is valuable knowledge - we now know NOT to try this again.
EOF
)"
```

### Immediately Continue

```
Target completed -> Back to Phase 1 -> Pick new target -> Never stop
```

---

## Usage

```
/lading:optimize:hunt
```

A 10% combined success rate (optimizations + bugs) is excellent. Most targets are cold paths - that's expected.
