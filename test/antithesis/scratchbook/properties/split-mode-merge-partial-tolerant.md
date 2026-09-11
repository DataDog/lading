---
slug: split-mode-merge-partial-tolerant
subsystem: capture
assertion_type: Always
priority: P2
needs_target: true
needs_sut_fix: none in lading proper (LEAD to validate against the deployment merge + per-instance finalize)
---

# Split-mode capture merge tolerates one clean side when the other overruns

## Statement

In split mode, if only the sender (or only the receiver) lading overruns/crashes leaving an
unreadable parquet, the merge outcome and replicate-failure attribution are well-defined (a
clean side's captures are not needlessly discarded by the other's corruption).

## Code evidence trail

- Deployment `capture_file_merge` oblivious merge reads row batches and validates schema; a
  footerless (killed-lading) file fails to read and errors the replicate.
- Digest deployment findings: merge tolerates one-empty/both-empty inputs but NOT a
  corrupt/truncated parquet.

## Assertion-type rationale

**Always** (merge robustness): on every timeline the merge/attribution must be well-defined; a
clean side must not be needlessly lost to the other's corruption.

## Mode / observability

Merge of a truncated file + a clean file behaves per policy (fail attributed to the corrupt
side), not a silent whole-replicate loss when one side captured cleanly. Oracle: split-mode
scenario killing only one side.

## Mechanism

None in lading proper; DEPLOYMENT-DERIVED LEAD to validate against source (the oblivious merge +
lading's per-instance capture finalize). Do not name the deployment.

## needs_sut_fix

None in lading proper (this is a merge-policy question in the deployment layer + lading's
per-instance finalize).

## Investigation Log

- [2026-07-24] Split mode (SMPTNG-721): the sender lading runs with `--no-target` and writes its
  own parquet on a separate pod; the two files are merged. The merge deletes a pre-existing
  parquet before glob-reading to avoid stale/merge confusion.
- [2026-07-24] Marked as a LEAD to validate; not a confirmed lading defect.

## Open Questions

- Does a sender-only overrun fail the whole replicate when the receiver captured cleanly?
