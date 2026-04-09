# logrotate_fs specification

`logrotate_fs.allium` is the behavioural specification for the log-rotation
filesystem generator. When working in this directory or on logrotate_fs code,
follow these guidelines.

## Spec-to-implementation mapping

| Spec section | Implementation file | Lines |
|-------------|-------------------|-------|
| Rules: AccumulateBytes, RotateLogFile | `model.rs` | 656-881 |
| Rules: OpenFile, ReadFile, CloseFile | `model.rs` | 579-609, 975-1028 |
| Rule: GarbageCollect | `model.rs` | 886-911 |
| Invariants (all nine) | `model.rs` | 1249-1474 |
| Surface: FileSystemView | `../logrotate_fs.rs` | 336-531 |
| Config | `../logrotate_fs.rs` | 38-65 |

## When modifying logrotate_fs code

1. Read the spec first to understand what behaviour is specified
2. After making code changes, check alignment between the spec and implementation
3. If behaviour changes, update the spec to match -- do not let them drift

## When modifying the spec

- Use `/allium` for syntax reference
- Use `tend` for targeted spec edits that preserve structure
- Use `weed` to verify spec-to-code alignment after changes
- Use `propagate` to generate tests from new rules or invariants

## Workflow for behavioural changes

1. Update the spec with the intended behavioural change
2. Identify the implementation gap
3. Make the code changes
4. Verify alignment between spec and code
5. Generate or update tests if needed

## Existing test coverage

The implementation has a comprehensive property-based test suite in
`model.rs:1107-1596` that exercises all nine spec invariants. The tests use
proptest to drive the model through randomised operation sequences (Open, Close,
Read, Lookup, GetAttr, Wait) and assert invariant properties after every
operation.
