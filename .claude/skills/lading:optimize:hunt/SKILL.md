---
name: lading:optimize:hunt
description: Systematic optimization hunter for lading. Finds memory optimizations AND bugs - both are valuable. Run /lading:optimize:validate when bugs are discovered.
---

# Optimization Hunt

Find optimizations, benchmark them, record results. Bugs discovered during hunting are equally valuable.

## Workflow

1. `/lading:preflight` → verify environment
2. Check `db.yaml` → skip already-attempted target/technique combos
3. Select target → profile or pick from hot paths
4. Implement ONE change on `opt/<crate>-<technique>` branch
5. Benchmark → micro then macro (see [BENCHMARKING.md](../shared/BENCHMARKING.md))
6. `ci/validate` → must pass
7. Commit with results → submit to `/lading:optimize:review`

## Target Selection

Hot paths: `lading_payload` (serialization), `lading_throttle` (rate math), `lading` (generators).

Profile with: `samply record ./target/release/payloadtool ci/fingerprints/<type>/lading.yaml`

## Benchmark Gates

See [BENCHMARKING.md](../shared/BENCHMARKING.md) for commands.

**Pass if ANY threshold met:**
- Time: >=5% faster
- Memory: >=10% reduction
- Allocations: >=20% reduction

**Micro must pass before running macro.** If micro shows no improvement, record failure and move on.

## Bug Discovery

If you find a bug instead of an optimization: `/lading:optimize:validate`

Then record as `bug_found` and continue hunting.

## Commit Format

After benchmarks pass:

```
<Verb> <target> <technique>

<Brief explanation of what and why>

**Micro-benchmark** (cargo criterion):
- <size>: <before> → <after> (<improvement>)

**Macro-benchmark** (payloadtool --memory-stats):
- Memory: <before> → <after> (<-X%>)

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
```

Title examples: `Reuse buffer in X`, `Eliminate allocations in Y`, `Use handle-based storage in Z`

## Recording

Update `db.yaml` in the SAME commit. See [DB_SCHEMA.md](../shared/DB_SCHEMA.md).

```yaml
entries:
  - target: <file:function>
    technique: <technique>
    status: <success|failure|bug_found>
    file: db/<name>.yaml
```

## Lading Rules

- **Determinism**: same seed → same output (always)
- **No panics**: no `.unwrap()` in non-test code
- **Kani**: run `ci/kani <crate>` if touching throttle/payload
