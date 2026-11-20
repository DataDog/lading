---
slug: unix-stream-write-error-progress
subsystem: generators/throttle
assertion_type: Always
priority: P2
needs_target: true
needs_sut_fix: handle non-BrokenPipe write errors (advance/break/reconnect); count packets per block not per partial write
---

# unix_stream makes progress on non-BrokenPipe write errors

## Statement

On a non-BrokenPipe, non-WouldBlock write error (e.g. ConnectionReset) the unix_stream worker
reconnects or advances rather than busy-looping on the same offset spamming `request_failure`.

## Code evidence trail

- `unix_stream.rs:304-315` — the non-BrokenPipe branch falls through with no offset
  advance/break/reconnect; the inner while loop re-calls `ready()`/`try_write` on the same
  offset, busy-looping and spamming `request_failure`. Only BrokenPipe triggers reconnect.
- `unix_stream.rs:293-295` — `packets_sent` is incremented once per partial write, so one block
  can count as many "packets".

## Assertion-type rationale

**Always** (no-livelock): on every timeline a ConnectionReset receiver must not pin a core or
emit a runaway `request_failure` count.

## Mode / observability

A ConnectionReset receiver does not pin a core or emit a runaway `request_failure` count;
`packets_sent` is not inflated per partial write. Oracle: reset-injecting receiver + CPU/counter
observation.

## Mechanism

Handle non-BrokenPipe write errors (advance/break/reconnect) at `unix_stream.rs:304-315`; count
packets per block, not per partial write.

## needs_sut_fix

Yes.

## Investigation Log

- [2026-07-24] Confirmed: only BrokenPipe triggers reconnect; other write errors fall through
  without advancing, causing a busy loop on the same offset.

## Open Questions

- None specific.
