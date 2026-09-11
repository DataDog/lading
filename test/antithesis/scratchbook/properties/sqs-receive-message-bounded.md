---
slug: sqs-receive-message-bounded
subsystem: blackhole/config
assertion_type: Always
priority: P2
needs_target: false
needs_sut_fix: clamp num_messages to a max (e.g. 10) before the 0..num_messages loop
---

# SQS blackhole bounds ReceiveMessage response size

## Statement

The SQS blackhole caps `max_number_of_messages` so a single target-controlled request cannot
force an enormous allocation and OOM the blackhole.

## Code evidence trail

- `sqs.rs:257-267` — `num_messages = rm.max_number_of_messages` parsed straight from the request
  (a `u32`) with no cap.
- `sqs.rs:362-370` — `generate_receive_message_response` loops `for _ in 0..num_messages`
  building ~450-byte message strings. Real SQS caps at 10; a value up to `u32::MAX` causes an
  enormous String allocation.

## Assertion-type rationale

**Always** (amplification/OOM): on every timeline a single request must not force an unbounded
allocation. Oracle: adversarial-request scenario + memory observation.

## Mode / observability

A request with a huge `max_number_of_messages` produces a bounded response (real SQS caps at
10), not an unbounded String allocation.

## Mechanism

Clamp `num_messages` to a max (e.g. 10) at `sqs.rs:257-267` before the `0..num_messages` loop.

## needs_sut_fix

Yes.

## Investigation Log

- [2026-07-24] Confirmed: the response size is unbounded by a target-controlled `u32`, enabling
  memory/CPU amplification from a single request.

## Open Questions

- None specific.
