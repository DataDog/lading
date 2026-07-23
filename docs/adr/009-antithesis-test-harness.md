# ADR-009: Antithesis Test Harness Architecture

## Status

**Draft**

## Date

2026-07-23

## Context

lading's integration correctness is currently checked by sheepdog/ducks, under
`integration/`. sheepdog orchestrates: it builds lading and ducks, spawns ducks,
tells it over a gRPC-on-unix-socket control plane to listen on a random port,
templates that port into a lading config, spawns lading, waits for lading to
exit, pulls counters back from ducks, and asserts "lading pushed load" via
`request_count > 10` or `total_bytes > 100_000`.

We want to test lading under Antithesis, a deterministic hypervisor that controls
scheduling and randomness, injects faults, and observes properties through
in-process SDK assertions. This requires deciding how lading is driven and where
the "load arrived" claim lives.

Three constraints shape the decision:

1. **Antithesis is the orchestrator.** docker-compose brings containers up. This
   replaces 'sheepdog'.
2. **lading is its own load driver.** Unlike a typical system under test that
   needs a separate workload container to exercise it, driving load is lading's
   function.
3. **The observer must be independent of the config under test.** In Antithesis
   the lading config is the search space. A `sometimes!` assertion that never
   executes is reported as unsatisfied, so if the claim lived in lading's own
   blackhole, a healthy config that omits a blackhole would fault the harness.

## Decision

Introduce an Antithesis harness under `test/antithesis/`, mirroring
Datadog/saluki's layout, with a "general" MVP scenario of three containers:

- **sink** (oracle): the standalone `test/antithesis/sink/` crate that binds a
  fixed TCP port, counts received bytes, and owns the claim
  `sometimes!(total_bytes > 0, "sink received bytes")` through the
  `lading_antithesis` SDK facade. Not faulted. SDK-linked but built without
  sancov coverage instrumentation.
- **lading** (system under test): the real lading binary, built
  `--features antithesis` with sancov coverage instrumentation and
  `panic="abort"`. lading reads its config once at startup and cannot be
  reconfigured, so its entrypoint blocks on a `ready` sentinel and then boots
  under the per-timeline config the harness sampled to the shared volume, run
  `--no-target --experiment-duration-infinite --prometheus-addr <addr>`
  (`--prometheus-addr` satisfies lading's telemetry requirement). A tcp generator
  points at the sink. Faulted.
- **workload** (driver): emits the Antithesis `setup_complete` signal, then idles
  to host the test commands. lading drives the load itself, so the only command
  today is `first_sample_config`, which samples this timeline's lading config.

In a like manner to saluki we introduce a generic
`test/antithesis/bin/launch.sh`, driven by per-scenario `launch.env`, tags
images by git SHA, builds, renders the compose with concrete tags, and submits
through `snouty launch` with a pinned fault profile. `test/antithesis/CLAUDE.md`
documents the launcher and the Antithesis skills: launch, triage, and
setup/research/workload.

The SDK facade `lading_antithesis`, feature-gated and a no-op when the feature
is off, is the single path both the sink and lading's bootstrap use to reach
`antithesis_sdk`. lading's instrumentation wiring lives in a feature-gated
`lading/src/antithesis_hooks.rs`, referenced from `lading/src/bin/lading.rs`. It
does SDK init plus a panic-reporting hook.

Config variation is a shared mechanism, not per-scenario code. The `harness`
crate (`test/antithesis/harness/`) samples a config by building lading's own
`generator::tcp::Config` from a value menu and serializing it, so the menu cannot
drift from the real schema. Its `first_sample_config` command draws the
structured choices from `AntithesisRng` (the SDK RNG, so each draw is a branch
point Antithesis explores; `thread_rng` under Antithesis is seeded once and does
not branch richly), writes the config plus a `ready` sentinel to a volume shared
with the lading container, and tags the sample with `reachable!` so triage can
count distinct variants. Scenarios reuse `harness` and differ only in wiring. The
MVP menu varies the free axes the TCP sink already catches -- payload variant,
throughput, and parallel connections -- over a fixed TCP transport; transport
variation waits on a multi-protocol sink. Block size is derived from the sampled
rate and connection count (not varied independently) so it stays within the
divided per-connection throttle capacity. The payload `seed` is drawn from system
entropy, not `AntithesisRng`: lading seeds its own PRNG from it and the docs
forbid seeding a userspace RNG from SDK randomness, so payload content is opaque
and effectively fixed across timelines -- acceptable because the sink asserts on
bytes received, not content.

Key sub-decisions:

- **Standalone sink, not lading's blackhole, as the oracle, per constraint 3.**
  The observer is separate and always-on, as ducks was.
- **The sink is SDK-linked but uninstrumented.** sancov coverage and the SDK are
  independent mechanisms. sancov steers exploration by coverage, and that budget
  must target lading, not the oracle. Instrumenting the sink would add its
  branches to the coverage surface and pull the search off the system under test.
  The oracle must be correct by construction: kept minimal and covered by
  ordinary unit and property tests, so it stays a trusted, fixed instrument
  rather than something the fuzzer probes. This matches saluki's `tools-builder`
  stage.
- **Three containers.** `setup_complete` and future config-variation
  live in a dedicated workload container rather than being owned by the faulted
  system under test.
- **Config varied per timeline, sampled from lading's own types.** The shared
  `harness` builds `tcp::Config` and serializes it, drawing the structured
  choices from `AntithesisRng`. Sampling is a post-`setup_complete` `first_`
  command so Antithesis branches each choice per timeline. Unit tests assert
  every sampled config re-deserializes as a valid lading config and that the
  block size never exceeds the divided per-connection throttle capacity, so the
  menu cannot silently drift from the schema or produce a discard-spin config.

## Alternatives Considered

### Assert in lading's own blackhole (self-loop, or lading-generator to lading-blackhole)

Rejected: the claim's reachability would depend on the config under test, and a
config without a blackhole would fault the harness with an unsatisfied
`sometimes!`. The observer must be independent of the search space.

### Reuse ducks as the Antithesis target

Rejected: ducks carries the sheepdog gRPC control plane it no longer needs, and
the goal is to replace ducks, not extend it. The sink fills ducks' structural
role as an independent, always-on receiver, without the orchestration baggage.

### Instrument the sink with sancov

Rejected: coverage should guide exploration into the system under test, not the
oracle. Instrumenting the sink dilutes the coverage signal and spends exploration
budget on harness code. The oracle must be bug-free, but that is achieved by
keeping the sink minimal and testing it conventionally, not by fuzzing it. The
sink still links the SDK to emit its assertion.

### Two containers (fold `setup_complete` into the system under test)

Rejected: the faulted system under test would own the setup signal, and
config-variation test commands would have no home. A dedicated workload
container keeps those concerns separate.

### Sample the config in lading's entrypoint at boot

Rejected. `snouty validate` does not execute `first_` commands, so with the
sentinel approach lading stays blocked and does not boot under validate --
validate still passes on `setup_complete`, exactly as saluki behaves. Sampling in
lading's own entrypoint would make validate boot lading and push, but a boot-time
draw is pre-`setup_complete`, which Antithesis branches less richly than a
post-setup `first_` command. We chose the richer exploration; validate passing
without booting the SUT is acceptable and is what saluki lives with.

## References

- `lading_antithesis/` - SDK facade over `antithesis_sdk`
- `test/antithesis/sink/` - the sink oracle crate
- `test/antithesis/harness/` - shared config-variation crate (`first_sample_config`)
- `test/antithesis/scenarios/general/` - the general scenario (Dockerfile, compose, launcher inputs)
- `integration/sheepdog/`, `integration/ducks/` - the mechanism this replaces
- saluki `test/antithesis/` - pattern source
- ADR-001: Generator-Target-Blackhole Architecture (the sink is an
  out-of-process, blackhole-like oracle)
