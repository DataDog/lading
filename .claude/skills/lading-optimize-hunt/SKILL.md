---
name: lading-optimize-hunt
description: Systematic optimization hunter for lading. Finds memory optimizations AND bugs - both are valuable. Run /lading-optimize-validate when bugs are discovered.
allowed-tools: Bash(cat:*) Bash(sample:*) Bash(samply:*) Bash(cargo:*) Bash(ci/*:*) Bash(hyperfine:*) Bash(*/payloadtool:*) Bash(tee:*) Read Write Edit Glob Grep Skill
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

**CRITICAL: The target must have an associated criterion benchmark.**

---

## Phase 2: Analyze Target

### Identify Opportunity Type

| Pattern | Technique | Bug Risk |
|---------|-----------|----------|
| `Vec::new()` + repeated push | `Vec::with_capacity(n)` | None |
| `String::new()` + repeated push | `String::with_capacity(n)` | None |
| `FxHashMap::default()` hot insert | `FxHashMap::with_capacity(n)` | None |
| `format!()` in hot loop | `write!()` to reused buffer | Format errors, buffer sizing |
| `&Vec<T>` or `&String` parameter | `&[T]` or `&str` slice | None |
| Allocation in hot loop | Move outside loop | Lifetime issues |
| Repeated temp allocations | Object pool/buffer reuse | Lifetime complexity, state bugs |
| Clone where borrow works | Use reference | Lifetime complexity |
| Hot cross-crate fn call | `#[inline]` attribute | Binary size bloat |
| Intermediate `.collect()` calls | Iterator chains without collect | Off-by-one, logic errors |
| Large struct by value | Box or reference | Nil/lifetime risk |
| Unbounded growth | Bounded buffer | Semantic change |

**Watch for bugs while analyzing - they're valuable findings.**

### Lading-Specific Concerns

- **Determinism**: Any optimization must preserve deterministic output
- **Pre-computation**: Prefer moving work to initialization over runtime
- **Worst-case behavior**: Optimize for worst-case, not average-case

---

## Phase 3: Establish Baseline

**CRITICAL: Capture baseline metrics BEFORE making any code changes.**

### Identify the Benchmark Target

Each `lading_payload` source module has a matching benchmark target. Use `--bench <name>` to run **only** the relevant benchmark instead of the full suite.

| Source module | `--bench` target | Fingerprint config dir |
|---|---|---|
| `apache_common.rs` | `apache_common` | `ci/fingerprints/apache_common/` |
| `ascii.rs` | `ascii` | `ci/fingerprints/ascii/` |
| `block.rs` | `block` | *(none — use json)* |
| `datadog_logs.rs` | `datadog_logs` | `ci/fingerprints/datadog_logs/` |
| `dogstatsd.rs` | `dogstatsd` | `ci/fingerprints/dogstatsd/` |
| `fluent.rs` | `fluent` | `ci/fingerprints/fluent/` |
| `json.rs` | `json` | `ci/fingerprints/json/` |
| `opentelemetry_log.rs` | `opentelemetry_log` | `ci/fingerprints/otel_logs/` |
| `opentelemetry_metric.rs` | `opentelemetry_metric` | `ci/fingerprints/otel_metrics/` |
| `opentelemetry_traces.rs` | `opentelemetry_traces` | `ci/fingerprints/otel_traces/` |
| `splunk_hec.rs` | `splunk_hec` | `ci/fingerprints/splunk_hec/` |
| `syslog.rs` | `syslog` | `ci/fingerprints/syslog/` |
| `trace_agent.rs` | `trace_agent` | `ci/fingerprints/trace_agent_v04/` |

Set these once and use them throughout:

```bash
BENCH=<target>   # e.g. json, syslog, dogstatsd
CONFIG=ci/fingerprints/<config_dir>/lading.yaml
```

### Stage 1: Micro-benchmark Baseline

Run **only** the benchmark for your target:

```bash
cargo criterion --bench "$BENCH" 2>&1 | tee /tmp/criterion-baseline.log
```

**Note:** Criterion stores baseline data automatically for later comparison.

### Stage 2: Macro-benchmark Baseline

Use the matching fingerprint config:

```bash
cargo build --release --bin payloadtool
hyperfine --warmup 3 --runs 10 --export-json /tmp/baseline.json \
  "./target/release/payloadtool $CONFIG"

./target/release/payloadtool "$CONFIG" --memory-stats 2>&1 | tee /tmp/baseline-mem.txt
```

**Baseline captured. Now proceed to implementation.**

---

## Phase 4: Implement

Make ONE change. Keep it focused and minimal.

Before proceeding, ALL changes must pass:

```bash
ci/validate
```

**No exceptions. If ci/validate fails, fix the issue before continuing.**

---

## Phase 5: Re-benchmark and Compare

**Two-stage gate: micro THEN macro. Both must show improvement.**

### Stage 1: Micro-benchmarks (inner loops)

Re-run the **same** benchmark target with your changes:

```bash
cargo criterion --bench "$BENCH" 2>&1 | tee /tmp/criterion-optimized.log
```

Note: Criterion automatically compares against the last run and reports percentage changes.

Compare results manually - look for "change:" lines showing improvement/regression

Example output looks like: `time: [1.2345 ms 1.2456 ms 1.2567 ms] change: [-5.1234% -4.5678% -4.0123%]`

#### Micro Decision Point

| Result | Action |
|--------|--------|
| Time improved >=5% | Proceed to Stage 2 |
| No change or regression | Process to Phase 8 and record FAILURE |

### Stage 2: Macro-benchmarks (end-to-end payloadtool)

Only run this if Stage 1 showed improvement. Use the SAME `$CONFIG` as Phase 3:

```bash
cargo build --release --bin payloadtool
hyperfine --warmup 3 --export-json /tmp/optimized.json \
  "./target/release/payloadtool $CONFIG"
./target/release/payloadtool "$CONFIG" --memory-stats 2>&1 | tee /tmp/optimized-mem.txt
```

#### Macro Decision Point

| Result | Action |
|--------|--------|
| Time improved >=5% | Proceed to Phase 6 |
| Memory reduced >=10% | Proceed to Phase 6 |
| Allocations reduced >=20% | Proceed to Phase 6 |
| No change or regression | Process to Phase 8 and record FAILURE (micro win, macro loss) |
| **ci/validate fails** | Might be a **BUG** |
| **Determinism broken** | Might be a **BUG** |

**All three gates must pass to proceed:**
1. Micro-benchmark shows improvement (>=5% time)
2. Macro-benchmark shows improvement (>=5% time OR >=10% memory OR >=20% allocations)
3. ci/validate passes (correctness preserved)

---

## Phase 6: Handle Bug Discovery

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

Return here and record as BUG_FOUND in Phase 8, then **continue hunting** - don't stop.

---

## Phase 7: Review

Invoke the review process: `/lading-optimize-review`

**Only consider this a valid optimization if the review passes.**

---

## Phase 8: Record the results

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

---

## Usage

```
/lading-optimize-hunt
```

A 10% combined success rate (optimizations + bugs) is excellent. Most targets are cold paths - that's expected.
