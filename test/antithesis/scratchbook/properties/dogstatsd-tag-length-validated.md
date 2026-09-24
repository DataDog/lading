---
slug: dogstatsd-tag-length-validated
subsystem: payload/determinism
assertion_type: Always
priority: P2
needs_target: false
needs_sut_fix: none (merged to main, e98e3052)
---

# DogStatsD tag_length.end() <= MIN_TAG_LENGTH is rejected upfront

## Statement

A dogstatsd config with `tag_length.end() <= MIN_TAG_LENGTH` is rejected at construction with a
dedicated error naming the value, not swallowed as `Error::StringGenerate` that silently drops
the message.

## Code evidence trail

- Merged fix `e98e3052` (#1875) — rejects `tag_length.end() <= MIN_TAG_LENGTH` upfront through
  `DogStatsD::new`.
- Root cause: the tags Generator wrapper subtracts 1 from `tag_length.end()` for the ':'
  separator; when `end == MIN_TAG_LENGTH` (3) it produced `Inclusive{3,2}`, rejected by the
  inner `min<=max` check and mis-surfaced as `Error::StringGenerate` (blaming pool generation,
  dropping the message).

## Assertion-type rationale

**Always** (config regression): on every timeline such a config must error clearly through
`DogStatsD::new`. Oracle: proptest/fixture over `tag_length` bounds asserting the dedicated
error path.

## Mode / observability

Such a config errors clearly through `DogStatsD::new` rather than mis-surfacing as a
pool-generation error.

## Mechanism

None (merged to main, `e98e3052`); the property guards against regression.

## needs_sut_fix

None (merged to main). Regression-guard.

## Investigation Log

- [2026-07-24] Merged to main (`e98e3052`, #1875); also branch
  `blt/fix-dogstatsd-tag-length-wrapper-underflow`. This property guards against reintroducing
  the underflow.

## Open Questions

- None specific.
