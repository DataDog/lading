---
slug: capture-no-fsync-durability
subsystem: capture
assertion_type: Always
priority: P2
needs_target: false
needs_sut_fix: add File::sync_data/sync_all at flush boundaries, or document maturity = handed-to-OS
---

# Flushed capture lines survive whole-VM termination

## Statement

Capture data reported as flushed reaches the persisted volume durably; a whole-container/VM
`node_termination` does not lose already-flushed jsonl lines.

## Code evidence trail

- `jsonl.rs:53` — `self.writer.flush()?` only calls `BufWriter::flush` -> OS page cache.
- Grep of `lading_capture/src` shows no `sync_all`/`sync_data`/`fsync` anywhere.
- `state_machine.rs:313-318` — flush interval; the maturity guarantee is "handed to the OS", not
  "durable on the volume".

## Assertion-type rationale

**Always** durability invariant: every flushed line must be recoverable after a whole-VM kill.

## Mode / observability

After `node_termination`, the surviving prefix includes all lines that were flushed before the
last maturity boundary. If page-cache writes are lost, flushed lines vanish.

## Mechanism

Add `File::sync_data`/`sync_all` at flush boundaries (or document that maturity =
handed-to-OS, not durable) in the `lading_capture` jsonl/parquet sinks.

## needs_sut_fix

Yes (or an explicit documentation decision).

## Investigation Log

- [2026-07-24] Confirmed no fsync in the capture write path.
- [2026-07-24] Priority hinges on the open question: if Antithesis `node_termination` does not
  preserve OS page cache to the persisted named volume, even flushed lines can be lost (P1);
  otherwise this is closer to theoretical (P2).

## Open Questions

- Does `node_termination` preserve OS page cache to the persisted named volume? Determines P2
  vs P1.
