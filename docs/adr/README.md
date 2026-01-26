# Architecture Decision Records

This directory contains Architecture Decision Records (ADRs) for lading. ADRs
document significant architectural decisions, their context, and consequences.

## What is an ADR?

An ADR captures a single architectural decision along with its context and
consequences. They serve as a historical record of why decisions were made,
helping future contributors understand the reasoning behind the current design.

## Quick Reference

| ADR | Title | Status |
|-----|-------|--------|
| [000](000-template.md) | Template | - |
| [001](001-generator-target-blackhole-architecture.md) | Generator-Target-Blackhole Architecture | Accepted |
| [002](002-precomputation-philosophy.md) | Pre-computation Philosophy | Accepted |
| [003](003-determinism-requirements.md) | Determinism Requirements | Accepted |
| [004](004-no-panic-error-handling.md) | No-Panic Error Handling Policy | Accepted |
| [005](005-performance-first-design.md) | Performance-First Design | Accepted |
| [006](006-testing-strategy.md) | Testing Strategy (Property Tests + Kani) | Accepted |
| [007](007-code-style-and-abstraction.md) | Code Style and Abstraction Rules | Accepted |
| [008](008-dependency-philosophy.md) | Dependency Philosophy | Accepted |

## Knowledge Map

Use this map to find ADRs relevant to your domain of interest.

### Core Architecture

Understanding how lading works:

- **ADR-001**: Generator-Target-Blackhole architecture - The fundamental design
- **ADR-002**: Pre-computation philosophy - Why payloads are pre-built

### Performance & Correctness

Why lading makes the trade-offs it does:

- **ADR-003**: Determinism requirements - Why reproducibility matters
- **ADR-005**: Performance-first design - "Obviously fast" patterns
- **ADR-006**: Testing strategy - Property tests and Kani proofs

### Code Quality & Style

How to write code for lading:

- **ADR-004**: No-panic error handling - Graceful failure always
- **ADR-007**: Code style and abstraction - The "shape rule" and naive style
- **ADR-008**: Dependency philosophy - When to implement vs. import

### Decision Dependencies

```
ADR-001 (Architecture)
    ├── ADR-002 (Pre-computation) ─── ADR-003 (Determinism)
    │                                      │
    │                                      v
    │                              ADR-005 (Performance)
    │                                      │
    │                                      v
    │                              ADR-006 (Testing)
    │
    └── ADR-004 (Error Handling)

ADR-007 (Code Style) ←──────────── ADR-008 (Dependencies)
```

## For AI Agents

When working on lading, consult these ADRs based on the task:

| Task Type | Relevant ADRs |
|-----------|---------------|
| Adding a new generator | 001, 002, 003, 005 |
| Adding a new blackhole | 001, 005 |
| Modifying throttle | 003, 005, 006 |
| Adding a dependency | 008 |
| Writing tests | 006 |
| Error handling | 004 |
| Code style questions | 007 |
| Performance work | 002, 005 |

## Creating New ADRs

1. Copy `000-template.md` to `NNN-title-with-dashes.md`
2. Fill in all sections
3. Update this README with the new ADR
4. Submit for review

ADR numbers are sequential. Use the next available number.

## Superseding ADRs

When a decision changes:

1. Create a new ADR documenting the new decision
2. Update the old ADR's status to "Superseded by ADR-NNN"
3. Reference the old ADR in the new one's "Alternatives Considered"

## Related Documentation

- [AGENTS.md](../../AGENTS.md) - Operational guidelines for AI agents
- [CONTRIBUTING.md](../../CONTRIBUTING.md) - Contribution guidelines
- [ci/](../../ci/) - Validation scripts referenced by ADRs
