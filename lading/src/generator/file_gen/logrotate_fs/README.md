# logrotate_fs specification

Behavioural specification for lading's log-rotation filesystem generator, written in [Allium v3](../../../../../.claude/skills/allium/references/language-reference.md).

## What this spec covers

The `logrotate_fs` component simulates a filesystem of rotating log files. External consumers (log shippers, tail utilities, monitoring agents) interact with it through standard filesystem operations. The spec captures:

- **Log file lifecycle** -- creation, byte accumulation, rotation, unlinking, garbage collection
- **Rotation mechanics** -- peer chains, ordinal shifting, size thresholds
- **File access contracts** -- open/read/close semantics including reads on unlinked files
- **Time-driven accumulation** -- constant and linear load profiles controlling write throughput
- **Filesystem surface** -- what a consumer sees (directory listings, file attributes, data reads)
- **Nine invariants** -- properties the system maintains at all times, distilled from the implementation's property-based test suite

## What this spec excludes

These are implementation concerns, not domain-level behaviour:

| Excluded | Reason |
|----------|--------|
| Block cache / payload generation | Data source mechanism, not rotation logic |
| FUSE protocol details | Transport layer for the filesystem interface |
| Inode allocation | Internal bookkeeping |
| Metrics emission | Observability infrastructure |
| RNG / determinism | Implementation of reproducibility |
| File permissions, uid/gid | OS-level detail, not domain behaviour |

## Relationship to implementation

| Spec entity | Implementation type | Source file |
|-------------|-------------------|-------------|
| `LogFile` | `model::File` | `model.rs` |
| `RotationGroup` | Implicit (group_id + group_names) | `model.rs` |
| `Directory` | `model::Directory` | `model.rs` |
| `FileHandle` | `model::FileHandle` | `model.rs` |
| `DataSource` | `block::Cache` | `lading_payload` crate |
| `LoadProfile` | `model::LoadProfile` | `model.rs` |
| `FileSystemView` surface | `impl Filesystem for LogrotateFS` | `../logrotate_fs.rs` |

## How to use this spec

**Understanding the system**: Read the spec top-to-bottom. Entities define the domain model, rules define behaviour, invariants define what must always be true, and surfaces define what external parties can see and do.

**Checking alignment**: Use the `weed` agent to verify that the implementation still matches this spec after code changes.

**Evolving the spec**: Use the `tend` agent to make targeted changes as requirements change. Do not edit the `.allium` file without re-validating against the implementation.

**Generating tests**: Use the `propagate` skill to generate integration tests from this spec. The nine invariants map directly to property-based test assertions already present in the codebase (`model.rs:1249-1474`).

## Open questions

Two questions are flagged in the spec for future resolution:

1. Whether `missed_bytes` tracking (bytes written but never read before GC) should be a first-class domain concept or remain observability infrastructure
2. Whether the load profile should support additional growth curves beyond constant and linear
