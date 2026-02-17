---
name: lading-optimize-hunt
description: Systematic optimization hunter for lading. Finds memory optimizations AND bugs - both are valuable. Run /lading-optimize-validate when bugs are discovered.
allowed-tools: Bash(cat:*) Bash(sample:*) Bash(samply:*) Bash(cargo:*) Bash(ci/*:*) Bash(hyperfine:*) Bash(*/payloadtool:*) Bash(tee:*) Read Write Edit Glob Grep Skill
---

# Optimization Hunt

Systematically explores the lading codebase, implements optimizations, validates with benchmarks. **Finding bugs is equally valuable as finding optimizations.**

## Role: Coordinator and Recorder

Hunt is the **coordinator and recorder** — it finds targets, captures baselines, implements changes, hands off to review, and records all outcomes. 

Hunt does NOT:
- Run post-change benchmarks (review does this)
- Make pass/fail decisions on optimizations (review does this)

Hunt DOES:
- Record all verdicts and outcomes in db.yaml after review returns

---

## Phase 0: Pre-flight

Run `/lading-preflight` first. Then check what's already been done:

```bash
# Check previous hunts (includes review verdicts)
cat .claude/skills/lading-optimize-hunt/assets/db.yaml
```

**If a target/technique combination exists in db.yaml, SKIP IT.**

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

### Bug Discovery During Analysis

If you discover a correctness bug **before implementing any optimization**, invoke validation immediately:

```
/lading-optimize-validate
```

After validation completes, return here and select the next target. Do not attempt an optimization on code with a known bug.

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
PAYLOADTOOL_CONFIG=ci/fingerprints/<config_dir>/lading.yaml
```

### Stage 1: Clear previous benchmarks

Clear any previously captured baselines so stale data cannot contaminate this run.

```bash
rm -f /tmp/criterion-baseline.log /tmp/baseline.json /tmp/baseline-mem.txt
rm -rf target/criterion
```

### Stage 2: Micro-benchmark Baseline

Run **only** the benchmark for your target:

```bash
cargo criterion --bench "$BENCH" 2>&1 | tee /tmp/criterion-baseline.log
```

### Stage 3: Macro-benchmark Baseline

Use the matching fingerprint config:

```bash
cargo build --release --bin payloadtool
hyperfine --warmup 3 --runs 30 --export-json /tmp/baseline.json \
  "./target/release/payloadtool $PAYLOADTOOL_CONFIG"

./target/release/payloadtool "$PAYLOADTOOL_CONFIG" --memory-stats 2>&1 | tee /tmp/baseline-mem.txt
```

**Baseline captured.** These files will be consumed by review:
- `/tmp/criterion-baseline.log` — micro-benchmark baseline
- `/tmp/baseline.json` — macro-benchmark timing baseline
- `/tmp/baseline-mem.txt` — macro-benchmark memory baseline

**CRITICAL: All benchmarks must complete before continuing.**

---

## Phase 4: Implement

Make ONE change. Keep it focused and minimal.

Before proceeding, ALL changes must pass:

```bash
ci/validate
```

**No exceptions. If ci/validate fails, fix the issue before continuing.**

If `ci/validate` repeatedly fails on what appears to be a **pre-existing bug** (not caused by your change), invoke validation:

```
/lading-optimize-validate
```

After validation completes, return here and select the next target.

---

## Phase 5: Hand Off to Review

Invoke the review process:

```
/lading-optimize-review
```

**Review returns a YAML report (as a fenced code block). It does NOT record results or create files. Hunt records everything in Phase 6.**

---

## Phase 6: Recording

After review returns its YAML report (or after a bug is validated), record the result. Every outcome MUST be recorded.

### Step 1: Write the Report

Write review's YAML report **verbatim** to `assets/db/<id>.yaml`. Do not modify, reformat, or add to the report content — it is the authoritative record from review.

### Step 2: Update the Index

Add an entry to `assets/db.yaml` following the format in `assets/index.template.yaml`.

---
