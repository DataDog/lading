# Lading Optimization Skills

Systematic optimization hunting for lading. Combines profiling, benchmarking, and rigorous review to find performance improvements while maintaining lading's strict correctness guarantees.

## Quick Start

```
/lading-preflight
```

Run preflight first. It validates your environment and tells you what to do next.

## Skills Overview

| Skill | Purpose |
|-------|---------|
| `/lading-preflight` | Environment validation - run first every session |
| `/lading-optimize-find-target` | Select and analyze an optimization target |
| `/lading-optimize-hunt` | Baseline, implement, review, and record optimizations |
| `/lading-optimize-review` | 5-persona peer review before merge |
| `/lading-optimize-validate` | Bug validation with Kani proofs or property tests |
| `/lading-optimize-submit` | Git branch, commits, and optional PR for optimizations |
| `/lading-optimize-rescue` | Salvage optimization work lacking benchmarks |

## Workflow

```
preflight --> find-target --> hunt --> [implement] --> review --> submit
                                                         |
                                                         v
                                                [approved / rejected]
                                                         |
                                                         v
                                                   record lesson
```

## Critical Context for LLMs

Before modifying lading code, understand these non-negotiable constraints:

**Determinism**: Same seed must produce identical output. No `thread_rng()`, no HashMap iteration order dependencies, no timing-based decisions.

**No Panics**: Never use `.unwrap()` or `.expect()`. All errors propagate via `Result<T, E>`.

**Performance Patterns**: Pre-allocate when size is known. No allocations in hot paths. Optimize for worst-case, not average-case.

**Validation**: Run `ci/validate` after every change. Use `ci/` scripts, not raw cargo commands.

**Testing**: Property tests (proptest) over unit tests. Kani proofs for critical code (throttle, payload).

See `CLAUDE.md` for complete project guidelines.

## Benchmark Thresholds

| Metric | Significance Threshold |
|--------|------------------------|
| Time | >= 5% improvement |
| Memory | >= 10% reduction |
| Allocations | >= 20% reduction |

Changes below these thresholds are noise, not optimization.

## Tracking Results

Hunt maintains `.claude/skills/lading-optimize-hunt/assets/db.yaml` as the central index with detailed entries in `.claude/skills/lading-optimize-hunt/assets/db/*.yaml`. Review owns the report templates (in `.claude/skills/lading-optimize-review/assets/`) and returns filled-in YAML reports; hunt persists them verbatim to `.claude/skills/lading-optimize-hunt/assets/db/`.

```
.claude/skills/lading-optimize-hunt/
├── SKILL.md      # Instructions
└── assets/
    ├── db.yaml   # Index of all hunts
    └── db/
        └── payload-prealloc.yaml  # Detailed hunt report
```

Query past results:
```bash
grep "prealloc" .claude/skills/lading-optimize-hunt/assets/db.yaml
cat .claude/skills/lading-optimize-hunt/assets/db/payload-prealloc.yaml
```

## Prerequisites

- Rust 1.70+ (1.82+ for Rust 2024 edition)
- cargo-nextest
- hyperfine
- Profiler: samply (recommended), perf, or macOS sample
- payloadtool built with `--memory-stats` support
