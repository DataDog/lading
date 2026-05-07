---
name: lading-optimize-hunt
description: "Coordinates lading Rust performance optimization attempts: profiles hot paths, captures benchmark baselines (Criterion + hyperfine), implements a single focused change, invokes multi-persona review, and records outcomes in db.yaml. Use when the user wants to optimize lading performance, speed up serialization, reduce allocations, or run the full benchmark-driven optimization loop."
allowed-tools: Bash(cat:*) Bash(cargo:*) Bash(ci/*:*) Bash(hyperfine:*) Bash(*/payloadtool:*) Bash(tee:*) Read Write Edit Glob Grep Skill
---

# Optimization Hunt

Coordinates optimization attempts. Captures baselines, implements changes, hands off to review (which runs post-change benchmarks and judges), and records all verdicts in `.claude/skills/lading-optimize-hunt/assets/db.yaml`.

---

## Phase 0: Pre-flight

Run `/lading-preflight`.

---

## Phase 1: Find Target

Run `/lading-optimize-find-target`.

It returns a YAML block with 6 fields: `pattern`, `technique`, `target`, `file`, `bench`, `fingerprint` - Print it out.

---

## Phase 2: Establish Baseline

**CRITICAL: Capture baseline metrics BEFORE making any code changes.**

### Identify the Benchmark Target

Use the `bench` and `fingerprint` fields from find-target's output — they are repo-relative paths ready to use:

```bash
BENCH=<bench field without extension>   # e.g. from "lading_payload/benches/syslog.rs" use "--bench syslog"
PAYLOADTOOL_CONFIG=<fingerprint field>  # e.g. "ci/fingerprints/syslog/lading.yaml"
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

## Phase 3: Implement

Make ONE change. Keep it focused and minimal.

Before proceeding, ALL changes must pass:

```bash
ci/validate
```

**No exceptions. If ci/validate fails, fix the issue before continuing.**

If `ci/validate` repeatedly fails on a **pre-existing bug** (not caused by your change), document it and stop.

---

## Phase 4: Hand Off to Review

Run `/lading-optimize-review` with the target fields as positional arguments:

```
/lading-optimize-review <bench> <fingerprint> <file> <target> <technique>
```

Where:
- `<bench>` — benchmark name from find-target's `bench` field, without path or extension (e.g. `trace_agent`)
- `<fingerprint>` — repo-relative path from find-target's `fingerprint` field
- `<file>` — repo-relative path from find-target's `file` field
- `<target>` — function name from find-target's `target` field
- `<technique>` — technique from find-target's `technique` field

It returns a YAML report. Print it out.

---

## Phase 5: Recording

After review returns its YAML report, record the result. Every outcome MUST be recorded.

### Step 1: Write the Report

Write review's YAML report **verbatim** to `.claude/skills/lading-optimize-hunt/assets/db/<id>.yaml`. Do not modify, reformat, or add to the report content — it is the authoritative record from review.

### Step 2: Update the Index

Add an entry to `.claude/skills/lading-optimize-hunt/assets/db.yaml` following the format in `.claude/skills/lading-optimize-hunt/assets/index.template.yaml`.

---
