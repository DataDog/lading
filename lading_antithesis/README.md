# lading-antithesis

A thin facade over the [Antithesis](https://antithesis.com) Rust SDK for the
lading project. It owns the single `antithesis` cargo feature: with the feature
off (the default) every macro is a no-op that elides its arguments unevaluated,
so ordinary lading builds are unaffected; with it on, each macro forwards to the
underlying SDK macro.

Both lading's SUT bootstrap (`lading/src/antithesis_hooks.rs`) and the Antithesis
workload drivers under `antithesis/` reach the SDK through this crate, never
`antithesis_sdk` directly, so an assertion reads identically wherever it lives.

Prefer the numeric macros (`always_gt!`, `always_le!`, ...) over
`always!(x > y, ...)`: a more precise assertion gives Antithesis better
exploration.
