# Lading Optimization Skills

Systematic optimization hunting for lading. Combines profiling, benchmarking, and rigorous review to find performance improvements while maintaining lading's strict correctness guarantees.

## Quick Start

```
/lading:preflight
```

Run preflight first. It validates your environment and tells you what to do next.

## Skills Overview

| Skill | Purpose |
|-------|---------|
| `/lading:preflight` | Environment validation - run first every session |
| `/lading:optimize:hunt` | Find optimization targets using profiling data |
| `/lading:optimize:review` | 5-persona peer review before merge |
| `/lading:optimize:validate` | Bug validation with Kani proofs or property tests |
| `/lading:optimize:rescue` | Salvage optimization work lacking benchmarks |

## Workflow

```
preflight --> hunt --> [success] --> review --> [approved] --> merge
                |                       |
                v                       v
           [bug found]             [rejected]
                |                       |
                v                       v
            validate                record lesson
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

## Tracking with Beads

Optimization results are tracked using beads issues with specific labels:

| Label | Purpose |
|-------|---------|
| `opt-hunt` | Hunt outcomes and lessons learned |
| `opt-review` | Review decisions and rationale |
| `opt-rescue` | Salvaged work and evidence |
| `opt-validate` | Bug validations and proofs |

Query past results:
```bash
bd list --labels=opt-hunt      # Previous hunts
bd list --labels=opt-review    # Previous reviews
bd list --labels=opt-rescue    # Previous rescues
bd list --labels=opt-validate  # Bug validations
```

## Prerequisites

- Rust 1.70+ (1.82+ for Rust 2024 edition)
- cargo-nextest
- hyperfine
- Profiler: samply (recommended), perf, or macOS sample
- payloadtool built with `--memory-stats` support
