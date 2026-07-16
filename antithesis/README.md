# Antithesis testing for lading

Everything needed to test lading under [Antithesis](https://antithesis.com) lives
in this one directory: the workload, the scenarios, the purpose-built probe, the
property catalog, and the harness that turns properties into assertions. If it is
Antithesis-specific, it is here. lading's own source carries only a single SDK
bootstrap hook, `lading/src/antithesis_hooks.rs`, and nothing else.

## Layout

| Path | What it is |
|---|---|
| `config/` | The `--config` directory snouty consumes: `docker-compose.yaml`, which defines three containers, plus `lading.yaml`, the SUT config. |
| `Dockerfile` | One file, three runtime targets: the coverage-instrumented SUT `lading`, `lading-probe`, and `workload-client`. |
| `workload-client/` | The test driver crate. Its `src/bin/` holds the test commands baked into the `main` test template. Property assertions live **here**, not in lading. |
| `lading-probe/` | The purpose-built network target/oracle that replaces `integration/ducks`. |
| `test/v1/` | Test templates. Command binaries are compiled from `workload-client` and injected by the Dockerfile. Files prefixed with `helper_` are ignored by Antithesis. |
| `setup-complete.sh` | Emits the Antithesis `setup_complete` lifecycle event once the rig is up. |

## How a property gets tested

Properties come from the `antithesis-research` skill. Each is implemented by the
`antithesis-workload` skill as a test command under `workload-client/src/bin/`,
prefixed with a valid Antithesis command type: `serial_driver_`, `parallel_driver_`,
`first_`, `eventually_`, `finally_`, `anytime_`, or `singleton_driver_`.

The house rule is saluki's: the SUT is only instrumented, never littered with
property assertions. A driver either exercises the SUT over the network and
asserts on what an oracle observed, or, for a pure, deterministic contract like
`Throttle::divide`, links the SUT crate as a library and asserts on its output
directly. Either way the `assert_*!` calls live in the harness.

Implemented so far:

- **`rig-runs-lading-cleanly`**, the P0 baseline: `serial_driver_baseline_clean_run`
  spawns a real `lading` run in a fault-quiet window and asserts it exits 0,
  writes a non-empty capture, and delivers bytes the probe caught. This is the
  control to run first. It also built the probe's byte-counting oracle.
- **`divide-preserves-aggregate-rate`**: `serial_driver_divide_throttle` links
  `lading_throttle` and drives the real `Throttle::divide` across a divisor ×
  capacity value menu, asserting the split preserves aggregate capacity up to
  the integer-division remainder.

## Running

Build images and validate the rig locally before launching:

```sh
cd antithesis/config
docker compose build
snouty validate . --timeout 120
```

`snouty validate` brings the compose up, waits for `setup_complete`, and checks
that the discovered test commands are well-formed. To submit a real run, use the
`antithesis-launch` skill. Do not hand-run `snouty launch`.

See `AGENTS.md` / `CLAUDE.md` for the agent-facing workflow and the skills that
own each part of this directory.
