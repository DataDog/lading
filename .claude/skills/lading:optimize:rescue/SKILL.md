---
name: lading:optimize:rescue
description: Salvages optimization work lacking benchmarks. Generates missing evidence, validates claims. Bugs discovered during rescue are valuable - invoke /lading:optimize:validate.
---

# Optimization Rescue

Salvage unbenchmarked optimization work. Generate evidence, validate claims, find hidden bugs.

## Workflow

1. `/lading:preflight` → verify environment
2. Audit changes → `git diff --name-only origin/main...HEAD`
3. Triage → hot path? cold path? suspicious?
4. Benchmark → see [BENCHMARKING.md](../shared/BENCHMARKING.md)
5. Keep/discard each change based on evidence
6. If bug found → `/lading:optimize:validate`
7. Reconstruct branch with only validated changes
8. Record in db.yaml

## Triage

| Evidence | Decision |
|----------|----------|
| Profiled hot path (top 10%) | INVESTIGATE |
| No profile evidence | LIKELY DISCARD |
| Looks buggy | INVESTIGATE for correctness |

### Bug Warning Signs

- `.unwrap()` or `.expect()` added → panic risk
- `unsafe` block → memory safety risk
- Bounds check removed → correctness risk
- Return type changed → semantic change

## Validation

See [BENCHMARKING.md](../shared/BENCHMARKING.md) for commands.

| Result | Decision |
|--------|----------|
| >=5% time OR >=10% memory OR >=20% allocs | KEEP |
| No improvement | DISCARD |
| ci/validate fails | BUG → `/lading:optimize:validate` |
| Determinism broken | BUG → `/lading:optimize:validate` |

## Reconstruct

```bash
git checkout main && git checkout -b opt/<name>-rescued
```

Apply only KEEP and BUG_FOUND (with tests) changes. Run `ci/validate` before finishing.

## Recording

Update `db.yaml`. See [DB_SCHEMA.md](../shared/DB_SCHEMA.md).

```yaml
entries:
  - original_branch: <branch>
    rescued_as: <branch>-rescued
    status: <rescued|partial|bug_found>
    file: db/<name>.yaml
```
