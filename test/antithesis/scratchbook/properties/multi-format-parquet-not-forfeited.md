---
slug: multi-format-parquet-not-forfeited
subsystem: capture
assertion_type: Always
priority: P1
needs_target: false
needs_sut_fix: reorder multi close/flush/write_metric to finalize parquet first (or best-effort both)
---

# multi format finalizes parquet even if jsonl close errors

## Statement

In multi capture mode a trivial jsonl flush/close error does not skip the parquet footer write;
the important format is never sacrificed to the unimportant one.

## Code evidence trail

- `multi.rs:69` — `self.jsonl.close()?;` runs BEFORE `:71` `self.parquet.close()?;`. If the
  jsonl `close()` errors, the `?` returns before the parquet footer is written.
- `multi.rs:46-48` (`write_metric`) and `:57-58` (`flush`) have the same ordering hazard.

## Assertion-type rationale

**Always**: on every timeline where multi mode finalizes, the parquet footer must be written
regardless of a jsonl error.

## Mode / observability

Inject a jsonl close error; the parquet footer is still written and the parquet file is
readable. Oracle: fault-inject jsonl path + external parquet readability check.

## Mechanism

Reorder `multi.rs` close/flush/write_metric to finalize parquet first (or best-effort both,
aggregating errors) at `multi.rs:46-48,57-58,69-72`.

## needs_sut_fix

Yes.

## Investigation Log

- [2026-07-24] Confirmed the ordering: jsonl (the "unimportant" format) is closed first, so a
  trivial jsonl error forfeits the critical parquet footer.

## Open Questions

- Should `multi::Format::close()` finalize parquet FIRST, or best-effort both and aggregate
  errors?
