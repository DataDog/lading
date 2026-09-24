---
slug: tcp-connect-loop-shutdown-responsive
subsystem: lifecycle/shutdown
assertion_type: Always
priority: P0
needs_target: true
needs_sut_fix: wrap connect attempt in select! with shutdown_wait
---

# TCP worker observes shutdown while (re)connecting

## Statement

The TCP generator worker responds to the shutdown signal even when the target is unreachable
and it is stuck in the connect/retry loop.

## Code evidence trail

- `tcp.rs:232-251` — the connect branch does `connect().await` -> on error `sleep(1s)` ->
  `continue`, forever.
- `tcp.rs:253-283` — the `tokio::select!` containing `shutdown_wait` is only reached once a
  connection exists.
- `tcp.rs:206-213` — `Tcp::spin()` blocks on `join_next()` for these workers, so graceful
  shutdown hangs (bounded only by the 30s runtime timeout, if reached).

## Assertion-type rationale

**Always** (bounded shutdown): every timeline that signals shutdown must reach exit within the
bound, including the unreachable-target case where the worker is in the pre-select connect loop.

## Mode / observability

With an unreachable target, after the experiment timer fires lading still exits within
`max_shutdown_delay`. Oracle external (timer to exit) in an unreachable-target scenario;
optionally a SUT `reachable!` at the loop's shutdown exit to prove the branch is taken.

## Mechanism

Wrap the connect attempt in `tokio::select!` with `&mut shutdown_wait`, or check shutdown
before the `sleep(1s)`, in the `tcp.rs` connect branch.

## needs_sut_fix

Yes. The connect branch has no shutdown branch; only the post-connection `select!` polls
`shutdown_wait`.

## Investigation Log

- [2026-07-24] Verified against source this turn: the connect happens before the `select!` and
  shutdown is only polled inside it (`tcp.rs:230-283`). Confirmed live on main.
- [2026-07-24] Feeds the `shutdown-completes-bounded` umbrella and the
  `unreachable-target` scenario.

## Open Questions

- None specific; shares the umbrella's open question about whether `Server::spin` completion
  is externally timed out.
