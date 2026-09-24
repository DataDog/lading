---
slug: no-panic-anywhere
subsystem: no-panic
assertion_type: Unreachable
priority: P0
needs_target: false
needs_sut_fix: none (hook already wired)
---

# lading never panics (panic hook = Unreachable)

## Statement

No panic occurs anywhere in the lading SUT under any config, fault, timing, or shutdown
path; the panic hook must never report "lading panicked".

## Code evidence trail

- `lading/src/antithesis_hooks.rs:13-33` — `init()` installs a panic hook that wraps the
  previous default hook.
- `lading/src/antithesis_hooks.rs:27` — the hook fires `unreachable! "lading panicked"` with
  details `{message, location}` on any panic, then forwards to the previous hook.
- `lading/src/bin/lading.rs:741` — `lading::antithesis_hooks::init()` is called as early as
  possible in `main`, before the tokio runtime is built.
- `Cargo.toml:115,120` — both release and dev profiles set `panic = "abort"`, so every panic
  is a hard, observable process abort.

## Assertion-type rationale

**Unreachable**: a panic is a point in code that must never be reached. The existing panic
hook is the oracle — it is the primary bridge enforcing the no-panic ADR. Because `panic=abort`
turns any hit into a SIGABRT, every reachable panic is an Antithesis-visible `unreachable!`
failure with the message and source location attached.

## Mode / observability

The hook fires on any panic before `panic=abort` SIGABRTs the container. Directly
Antithesis-visible via the reported `unreachable!`. This is an umbrella invariant; individual
panic sites are catalogued as their own properties.

## Mechanism

Umbrella no-panic invariant. SUT-side probe already present in `antithesis_hooks.rs`. No
additional instrumentation is needed for the umbrella — the individual panic sites listed as
separate properties each need their ADR-compliant `Result` conversions.

## needs_sut_fix

None for the hook itself (already wired). The individual panic sites (observer asserts,
`.expect` call sites, OnceCell double-set, etc.) each need their own Result conversions,
tracked as separate properties.

## Investigation Log

- [2026-07-24] Confirmed from the SDK inventory: `lading/src` carries only two real SDK sites,
  both bootstrap/plumbing — `antithesis_hooks.rs` (init + panic hook) and `lading.rs:792`
  (bootstrap `reachable!`). No domain-level assertions live in production source, so the panic
  hook is the sole no-panic oracle inside the SUT.
- [2026-07-24] The three std-library `unreachable!` sites (`lading.rs:347`,
  `splunk_hec/acknowledgements.rs:110`, `otlp/http.rs:227,258`) are core macros, NOT SDK
  instrumentation, but they are still subject to the no-panic ADR and would be caught by the
  hook if hit.

## Open Questions

- Is the `antithesis` feature actually enabled in the scenario Dockerfile so the enabled-arm
  hook compiles in? (Noted unverified in the SDK-instrumentation scan.)
