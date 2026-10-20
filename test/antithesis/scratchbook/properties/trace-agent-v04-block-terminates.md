---
slug: trace-agent-v04-block-terminates
subsystem: payload/determinism
assertion_type: Always
priority: P0
needs_target: false
needs_sut_fix: cap consecutive rejections (InsufficientBlockSizes) and emit EmptyBlock not a 1-byte block
---

# trace-agent v04 block-cache next_block terminates in bounded time

## Statement

trace-agent v04 block-cache construction terminates quickly even when a serialized trace exceeds
`max_block_size`; `to_bytes` never emits a 1-byte empty msgpack array accepted as a valid block,
and construction is not O(n^2) re-serialization.

## Code evidence trail

- `block.rs`, `trace_agent/v04.rs` — the v04 `next_block` construction path; observed 31h hang
  -> 0.75s after the fix.
- Root cause: `to_bytes` emitted a 1-byte empty msgpack array accepted as a valid block, with
  O(n^2) re-serialization when a single trace exceeds `max_block_size`.

## Assertion-type rationale

**Always**/liveness: on every config construction completes in sub-second. Fuzz property
`trace_agent_v04_cache_fixed_next_block` + a bounded-time startup assertion.

## Mode / observability

Construction completes in sub-second, not hours (observed 31h hang -> 0.75s). An empty result is
`EmptyBlock`, not a 1-byte block.

## Mechanism

Cap consecutive rejections (`InsufficientBlockSizes`) and emit `EmptyBlock` not a 1-byte block
(fix `456e85a3` on backup branch; live on main).

## needs_sut_fix

Yes. Fix `456e85a3` on backup branch; live on main.

## Investigation Log

- [2026-07-24] The general block-cache-hang case is a separate property
  (`block-cache-construction-terminates`); this is the trace_agent-v04-specific variant with a
  dedicated fuzz property.

## Open Questions

- None specific.
