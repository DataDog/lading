---
name: lading-optimize-hunt
description: Systematic optimization hunter for lading. Finds memory optimizations AND bugs - both are valuable. Run /lading-optimize-validate when bugs are discovered.
---

# Optimization Hunt

Systematically explores the lading codebase, implements optimizations, validates with benchmarks. **Finding bugs is equally valuable as finding optimizations.**

## Valuable Outcomes

| Outcome | Value | Action |
|---------|-------|--------|
| **Optimization works** | Memory/time improved | Submit to `/lading-optimize-review` |
| **Optimization fails** | Learned cold path | Record, next target |
| **Bug discovered** | Found correctness issue | Invoke `/lading-optimize-validate` |

**All three outcomes build institutional knowledge.**

---

## Phase 0: Pre-flight

Run `/lading-preflight` first. Then check what's already been done:

```bash
# Check previous hunts
cat .claude/skills/lading-optimize-hunt/assets/db.yaml

# Check previous reviews
cat .claude/skills/lading-optimize-review/assets/db.yaml
```

**If a target/technique combination exists in either db.yaml, SKIP IT.**

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

Make ONE change. After validating with benchmarks (Phase 4), commit using the template in `assets/commit-template.txt`:

```bash
# Example:
git commit -m "opt: buffer reuse in syslog serialization

Replaced per-iteration format!() with reusable Vec<u8> buffer.

Target: lading_payload/src/syslog.rs::Syslog5424::to_bytes
Technique: buffer-reuse

Micro-benchmarks:
  syslog_100MiB: +42.0% throughput (481 -> 683 MiB/s)

Macro-benchmarks (payloadtool):
  Time: -14.5% (8.3 ms -> 7.1 ms)
  Memory: -35.8% (6.17 MiB -> 3.96 MiB)
  Allocations: -49.3% (67,688 -> 34,331)

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>
"
```

**Note:**
- First line must be ≤50 characters (Git best practice)
- Replace `{MODEL}` with the actual Claude model being used (e.g., "Claude Sonnet 4.5", "Claude Opus 4.5").

---

## Phase 4: Benchmark

**Two-stage gate: micro THEN macro. Both must show improvement.**

### CRITICAL: Use Separate Worktree for Baseline

**NEVER use `git stash`/`git checkout` to switch between baseline and optimized.** This causes confusion and errors. Instead, use a separate git worktree:

```bash
# One-time setup: create a baseline worktree (do this once per repo)
git worktree add ../lading-baseline main

# The baseline worktree is at ../lading-baseline
# Your optimization work stays in the current directory
```

### Stage 1: Micro-benchmarks (inner loops)

Use `cargo criterion` for micro-benchmarks. Run in each worktree and compare output:

```bash
# In baseline worktree (../lading-baseline)
cd ../lading-baseline
cargo criterion 2>&1 | tee /tmp/criterion-baseline.log

# In optimization worktree (your current directory)
cd /path/to/your/optimization/branch
cargo criterion 2>&1 | tee /tmp/criterion-optimized.log

# Compare results manually - look for "change:" lines showing improvement/regression
# Example output: "time: [1.2345 ms 1.2456 ms 1.2567 ms] change: [-5.1234% -4.5678% -4.0123%]"
```

**Note:** Criterion automatically compares against the last run in that worktree and reports percentage changes.

#### Micro Decision Point

| Result | Action |
|--------|--------|
| Time improved >=5% | Proceed to Stage 2 |
| No change or regression | Record FAILURE, next target |

**If micro-benchmark shows no improvement, STOP. Move to next target.**

### Stage 2: Macro-benchmarks (end-to-end payloadtool)

Only run this if Stage 1 showed improvement.

```bash
# Choose a config file (e.g., ci/fingerprints/json/lading.yaml)
CONFIG=ci/fingerprints/json/lading.yaml

# In baseline worktree
cd ../lading-baseline
cargo build --release --bin payloadtool
hyperfine --warmup 3 --export-json /tmp/baseline.json \
  "./target/release/payloadtool $CONFIG"
./target/release/payloadtool "$CONFIG" --memory-stats 2>&1 | tee /tmp/baseline-mem.txt

# In optimization worktree
cd /path/to/your/optimization/branch
cargo build --release --bin payloadtool
hyperfine --warmup 3 --export-json /tmp/optimized.json \
  "./target/release/payloadtool $CONFIG"
./target/release/payloadtool "$CONFIG" --memory-stats 2>&1 | tee /tmp/optimized-mem.txt
```

#### Macro Decision Point

| Result | Action |
|--------|--------|
| Time improved >=5% | Proceed to `/lading-optimize-review` |
| Memory reduced >=10% | Proceed to `/lading-optimize-review` |
| Allocations reduced >=20% | Proceed to `/lading-optimize-review` |
| No change or regression | Record FAILURE (micro win, macro loss), next target |
| **ci/validate fails** | Might be a **BUG** |
| **Determinism broken** | Might be a **BUG** |

**All three gates must pass to proceed to review:**
1. Micro-benchmark shows improvement (>=5% time)
2. Macro-benchmark shows improvement (>=5% time OR >=10% memory OR >=20% allocations)
3. ci/validate passes (correctness preserved)

---

## Phase 5: Handle Bug Discovery

If during hunting you discover a bug (not an optimization):

### Invoke Correctness Validation
```
/lading-optimize-validate
```

This skill will:
1. Attempt Kani proof first (if feasible)
2. Fall back to property test if Kani fails
3. Verify fix works
4. Record in its db.yaml

### After Validation

Return here and record as BUG_FOUND in Phase 7, then **continue hunting** - don't stop.

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

### MANDATORY: Update db.yaml

1. Add entry to `assets/db.yaml` index
2. Create detailed file in `assets/db/` directory

**assets/db.yaml entry:**
```yaml
entries:
  - target: <file:function>
    technique: <prealloc|avoid-clone|cache|etc>
    status: <success|failure|bug_found>
    file: assets/db/<target-technique>.yaml
```

**assets/db/<target-technique>.yaml** for SUCCESS:
```yaml
target: <file:function>
technique: <prealloc|avoid-clone|cache|etc>
status: success
date: <YYYY-MM-DD>
branch: <branch name>
measurements:
  time: <-X% or ~>
  memory: <-X% or ~>
  allocations: <-X% or ~>
lessons: |
  <pattern learned>
```

**assets/db/<target-technique>.yaml** for FAILURE:
```yaml
target: <file:function>
technique: <prealloc|avoid-clone|cache|etc>
status: failure
date: <YYYY-MM-DD>
reason: <cold path|already optimized|etc>
lessons: |
  <why it didn't work - this is valuable knowledge>
```

**assets/db/<target-technique>.yaml** for BUG_FOUND:
```yaml
target: <file:function>
technique: <what was being tried>
status: bug_found
date: <YYYY-MM-DD>
bug_description: <what was found>
validation_file: <path to validate db entry>
lessons: |
  <what was learned>
```

### Immediately Continue

```
Target completed -> Back to Phase 1 -> Pick new target -> Never stop
```

---

## Usage

```
/lading-optimize-hunt
```

A 10% combined success rate (optimizations + bugs) is excellent. Most targets are cold paths - that's expected.
