# Antithesis harness

This directory holds the Antithesis test harness for lading. The design and
rationale live in [ADR-009](../../docs/adr/009-antithesis-test-harness.md).

## Skills

- **Launch** a run with the `antithesis-launch` skill or the
  `test/antithesis/bin/launch.sh` wrapper. Do not hand-type `snouty launch`.
- **Triage** a completed run with `antithesis-triage` (single run) or
  `antithesis-campaign` (launch, wait, triage, and correlate against tickets).
- **Scaffold and grow** the harness with `antithesis-setup`,
  `antithesis-research` (build the property catalog), and `antithesis-workload`
  (implement assertions and test commands).
- **Validate** scaffolding changes quickly with `snouty validate`.

## launch.sh

`test/antithesis/bin/launch.sh <scenario>` builds the scenario's images, renders
the compose with concrete git-SHA tags into `.launch/`, and submits through
`snouty launch` with the cpu, clock, and node fault profile the scenario's
`launch.env` sets. Node faults apply only to `SCENARIO_FAULT_NODES`. See the
script header for env overrides (`DURATION`, `SOURCE`, `WEBHOOK`,
`FORCE_DISABLE_ALL_FAULTS`, `DRY_RUN`, ...).

The `SOURCE` (antithesis.source) defaults to `lading`; confirm the exact string
this tenant expects before relying on tracked runs.

## setup-complete.sh

Antithesis runs no test commands until it receives `setup_complete`. Each
scenario's `workload/setup-complete.sh` emits it once the system under test is
ready; the workload container runs it from its entrypoint.

## Directory layout

- `sink/` — the TCP byte-counting sink oracle crate (`sink`), a workspace member.
  It receives lading's load and owns the "load arrived" assertion. SDK-linked but
  built without coverage instrumentation. Built, linted, and tested from the repo
  root like any other crate.
- `scenarios/general/` — the MVP scenario: `Dockerfile` (three targets: the
  instrumented lading system under test, the sink, the workload), a
  `docker-compose.yaml` snouty consumes as `--config`, `launch.env`, the
  hard-wired `lading.yaml`, and the `workload/` build inputs. Config variation is
  deferred to the workload seam.
- `bin/launch.sh` — the generic launcher shared by every scenario.
