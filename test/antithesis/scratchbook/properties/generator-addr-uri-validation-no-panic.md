---
slug: generator-addr-uri-validation-no-panic
subsystem: no-panic
assertion_type: Unreachable
priority: P1
needs_target: false
needs_sut_fix: return Result errors instead of .expect at tcp/udp/grpc construction
---

# Malformed addr/target_uri is a Result error, not a construction panic

## Statement

An unresolvable/malformed socket address (tcp/udp) or `target_uri` (grpc) yields a clean config
error, not an `.expect` panic at generator construction.

## Code evidence trail

- `tcp.rs:154-159` — `to_socket_addrs().expect("could not convert to socket").next().expect(..)`.
- `udp.rs:164-169` — same pattern.
- `grpc.rs:226-231` — `.expect("target_uri must be valid")`; `:245-247`
  `.expect("target_uri should have an RPC path")`.

## Assertion-type rationale

**Unreachable**: these construction `.expect`s on user-controlled strings must never be reached.
Oracle: malformed-config scenario + panic hook.

## Mode / observability

Config with a bad addr/uri fails startup with an error; the panic hook does not fire.

## Mechanism

Return `Result` errors instead of `.expect` at `tcp.rs:154-159`, `udp.rs:164-169`,
`grpc.rs:226-231,245-247`.

## needs_sut_fix

Yes.

## Investigation Log

- [2026-07-24] Confirmed: several generators `.expect()` on values derived from user strings,
  violating the no-panic ADR (which requires returning the `Result` Error).

## Open Questions

- None specific.
