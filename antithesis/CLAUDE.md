# Antithesis directory: agent guide

This directory is the single home for all Antithesis testing of lading. Read
`README.md` for the layout and `AGENTS.md` for the skills that own each part.

## The one rule

**All Antithesis code lives under `antithesis/`.** Do not scatter
Antithesis-specific code, especially `assert_*!` property assertions, across
lading's crates such as `lading/`, `lading_throttle/`, and `lading_payload/`.
The SUT carries exactly one Antithesis touch point: the SDK bootstrap in
`lading/src/antithesis_hooks.rs`, plus the `antithesis` cargo feature that
gates it. That is the saluki model, chosen deliberately. It keeps ordinary
lading builds untouched and keeps everything testable and easy to point to
from one place.

When a property needs to observe the SUT:

- Prefer driving the SUT over the network and asserting on an oracle's view,
  such as the `lading-probe` report or the capture file, the way saluki's
  intake works.
- For a pure, deterministic contract, link the SUT crate as a library from
  `workload-client` and assert on its return value in the driver. Reach for a
  public accessor the SUT already exposes. Do not add SUT code to make a
  private value observable without discussing it first.

Either way, the `assert_*!` calls belong in `workload-client`, never in the SUT.

## Skills

- `antithesis-research` - analyze the codebase.
- `antithesis-setup` - scaffold/adjust `config/`, `Dockerfile`, containers.
- `antithesis-workload` - implement one property at a time as a test command.
- `antithesis-triage` / `antithesis-launch` - review and submit runs. Never
  run `snouty launch` by hand.

## Conventions

- Test commands are Rust binaries in `workload-client/src/bin/`, named with a
  valid Antithesis prefix, injected into `test/v1/main/` by the `Dockerfile`.
- Assertion property names are inline constant string literals, unique across
  the whole project because Antithesis catalogs them statically. Namespace
  them by what they check, e.g. `lading_throttle.divide.aggregate_not_exceeded`.
- All randomness goes through the Antithesis SDK's `antithesis_sdk::random` so
  timelines replay deterministically.
- Draw bounded inputs from a property-specific value menu of boundaries plus
  the configured-limit families the property's code paths care about, not
  arbitrary ranges.
