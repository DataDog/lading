---
name: lading:optimize:review
description: Reviews optimization patches for lading using a 5-persona peer review system. Requires unanimous approval backed by benchmarks. Bugs discovered during review are valuable - invoke /lading:optimize:validate to validate them.
---

# Optimization Review

5-persona review requiring unanimous approval. All outcomes (approved, rejected, bug found) are valuable.

## Workflow

1. `/lading:preflight` → verify environment
2. Check `db.yaml` → reject if branch already reviewed
3. `ci/validate` → must pass (reject immediately if not)
4. Verify benchmarks → see [BENCHMARKING.md](../shared/BENCHMARKING.md)
5. Run 5-persona review → all must approve
6. Record verdict → see [DB_SCHEMA.md](../shared/DB_SCHEMA.md)

## Benchmark Requirements

Use `--runs 30` for hyperfine. If benchmark data is missing → **REJECT**.

No exceptions for "theoretically better" or "obviously an improvement."

## Five-Persona Checklist

### 1. Duplicate Hunter
- [ ] Branch not already reviewed
- [ ] File + technique not already approved
- [ ] If duplicate → REJECT

### 2. Skeptic
- [ ] Benchmarks meet thresholds (5% time OR 10% memory OR 20% allocs)
- [ ] Statistical significance confirmed
- [ ] Not measurement noise

### 3. Conservative
- [ ] `ci/validate` passes
- [ ] Determinism preserved (same seed → same output)
- [ ] No `.unwrap()` added outside tests
- [ ] Property tests exist
- [ ] If bug found → `/lading:optimize:validate`

### 4. Rust Expert
- [ ] No `mod.rs` files
- [ ] `use` statements at file top
- [ ] Pre-computation over runtime work
- [ ] No unnecessary clones in hot paths

### 5. Greybeard
- [ ] Readable without extensive comments
- [ ] Complexity justified by measurements
- [ ] Minimal change, no scope creep

## Decision

| Votes | Verdict |
|-------|---------|
| 5/5 approve | APPROVED → merge |
| Any reject | REJECTED → record lesson |
| Duplicate found | DUPLICATE → reference existing |
| Bug found | BUG_FOUND → `/lading:optimize:validate` |

## Recording

Update `db.yaml` with verdict. See [DB_SCHEMA.md](../shared/DB_SCHEMA.md).

```yaml
entries:
  - branch: <branch>
    verdict: <approved|rejected|duplicate|bug_found>
    file: db/<branch>.yaml
```
