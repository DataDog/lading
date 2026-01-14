---
name: lading:optimize:rescue
description: Salvages optimization work lacking benchmarks. Generates missing evidence, validates claims. Bugs discovered during rescue are valuable - invoke /lading:optimize:validate.
---

# Optimization Rescue

Salvage optimization work done without proper benchmarks. Generate evidence, validate claims, and discover bugs hiding in "optimizations."

## Valuable Outcomes

| Outcome | Value | Action |
|---------|-------|--------|
| **Change validated** | Real improvement proven | KEEP, include in rescued branch |
| **Change invalidated** | No improvement | DISCARD, record lesson |
| **Bug discovered** | Correctness issue found | Invoke `/lading:optimize:validate` |

**Bugs found during rescue are SUCCESS, not failure.**

---

## Phase 0: Pre-flight

Run `/lading:preflight` first.

---

## Phase 1: Audit

```bash
git diff --name-only origin/main...HEAD | grep '\.rs$'
```

For each change, categorize:
- Preallocation (`Vec::with_capacity`, `String::with_capacity`)
- Avoiding clones (borrowing instead of owned)
- Moving allocations out of loops
- Data structure changes
- **Potential bug** (suspicious patterns)

---

## Phase 2: Triage

| Hot Path? | Decision |
|-----------|----------|
| Yes (profiled, top 10%) | INVESTIGATE |
| Warm (suspected hot, 10-25%) | SKEPTICAL |
| Cold (no profile evidence) | LIKELY DISCARD |
| **Looks buggy** | INVESTIGATE for correctness |

### Bug Warning Signs in Rust

| Pattern | Risk |
|---------|------|
| `.unwrap()` or `.expect()` added | Panic path (lading MUST NOT panic) |
| `unsafe` block added | Memory safety risk |
| Changed return type | Semantic change |
| Removed bounds checks | Correctness risk |
| Clone removed without lifetime analysis | Use-after-move risk |
| `mem::transmute` or `mem::forget` | Undefined behavior risk |

---

## Phase 3: Generate Evidence

### For payloadtool (end-to-end):

```bash
# Choose a config file (e.g., ci/fingerprints/json/lading.yaml)
CONFIG=ci/fingerprints/json/lading.yaml

# Baseline
git stash && git checkout origin/main
cargo build --release --bin payloadtool
hyperfine --warmup 3 --runs 30 --export-json /tmp/baseline.json \
  "./target/release/payloadtool $CONFIG"
./target/release/payloadtool "$CONFIG" --memory-stats 2>&1 | tee /tmp/baseline-mem.txt

# Optimized
git checkout - && git stash pop
cargo build --release --bin payloadtool
hyperfine --warmup 3 --runs 30 --export-json /tmp/optimized.json \
  "./target/release/payloadtool $CONFIG"
./target/release/payloadtool "$CONFIG" --memory-stats 2>&1 | tee /tmp/optimized-mem.txt
```

### For inner loops (criterion):

If the crate has criterion benchmarks:
```bash
# Baseline
git stash && git checkout origin/main
cargo criterion --save-baseline before

# Optimized
git checkout - && git stash pop
cargo criterion --baseline before
```

### Create Benchmarks If Missing

If no benchmark exists for the changed code, create one:

```rust
// In lading_payload/benches/<name>.rs
use criterion::{criterion_group, criterion_main, Criterion, Throughput};

fn benchmark_function(c: &mut Criterion) {
    let mut group = c.benchmark_group("function_name");
    group.throughput(Throughput::Bytes(1024));

    group.bench_function("baseline", |b| {
        b.iter(|| {
            function_under_test()
        })
    });

    group.finish();
}

criterion_group!(benches, benchmark_function);
criterion_main!(benches);
```

---

## Phase 4: Validate

### Decision Matrix

| Result | Decision |
|--------|----------|
| Time improved >=5% | KEEP |
| Memory reduced >=10% | KEEP |
| Allocations reduced >=20% | KEEP |
| No significant change | DISCARD |
| Regression | DISCARD |
| **ci/validate fails** | Possible BUG -> `/lading:optimize:validate` |
| **Determinism broken** | Possible BUG -> `/lading:optimize:validate` |
| **Panic path added** | BUG -> `/lading:optimize:validate` |

### Verify Determinism

Determinism is verified via fingerprints. The same config (with fixed seed) must produce identical output:
```bash
CONFIG=ci/fingerprints/json/lading.yaml
./target/release/payloadtool "$CONFIG" --fingerprint > /tmp/run1.txt
./target/release/payloadtool "$CONFIG" --fingerprint > /tmp/run2.txt
diff /tmp/run1.txt /tmp/run2.txt  # Must be identical
```

**Note:** Seed is specified in the config file, not as a CLI flag.

---

## Phase 5: Handle Bug Discovery

If rescue uncovers a bug instead of an optimization:

```
/lading:optimize:validate
```

After validation:
1. Bug recorded as beads issue with `opt-validate` label (via /lading:optimize:validate)
2. Record rescue as BUG_FOUND in beads (see Phase 7)
3. The bug fix becomes part of rescued branch (with tests!)

---

## Phase 6: Reconstruct

```bash
git checkout main
git checkout -b opt/<name>-rescued
```

Apply only:
- **KEEP** changes (validated optimizations with benchmark proof)
- **BUG_FOUND** changes (with tests from /lading:optimize:validate)

Discard everything else.

### Mandatory Before Finishing

```bash
ci/validate
```

**No exceptions. Rescued branch must pass ci/validate.**

---

## Phase 7: Record

Create a beads issue documenting the rescue operation:

```bash
bd create --type=task --labels=opt-rescue \
  --title="Rescue: <original-branch>" \
  --description="$(cat <<'EOF'
## Rescue Summary

- **Original Branch**: <opt/original-branch>
- **Rescued As**: <opt/original-branch-rescued>

## Statistics
- **Audited**: <N> changes
- **Kept**: <N> changes
- **Discarded**: <N> changes
- **Bugs Found**: <N>

## Kept Changes (with measurements)
- `<file>`: <technique> (<delta_time>, <delta_mem>)
- ...

## Discarded Changes
- `<file>`: <reason>
- ...

## Bugs Found
- `<file>`: <bug issue ID>
- ...

## Lesson
<pattern learned>
EOF
)"
```

---

## Usage

```
/lading:optimize:rescue
```
