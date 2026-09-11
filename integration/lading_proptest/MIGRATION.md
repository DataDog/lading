# Migration: Adaptive Sampling Testing → datadog-agent

This document tracks the migration of e2e property testing for adaptive sampling from the `lading` repo prototype to the `datadog-agent` repo, where it will live alongside the implementation it tests.

## Background

We built `lading_proptest` as a prototype in the lading repo to prove that property-based e2e testing of the Datadog Agent's logs pipeline works. It does — we have passing tests for truncation, multiline aggregation (timestamp, JSON, mixed), and adaptive sampling with a model-based property checker at tolerance=0.

The next phase moves this work to `datadog-agent` where it belongs.

## What We Built (lading_proptest prototype)

### Infrastructure
- **Fake intake server** (Rust, hyper) — accepts agent HTTP log payloads, decompresses gzip/zstd, parses JSON, stores entries in memory for assertion
- **Agent orchestrator** — starts/stops Docker containers or local binaries, manages config, temp dirs, timing
- **Action sequence executor** — write logs, sleep, write more — enables timing-dependent tests
- **Output dumping** — `output.json`, `output_messages.txt`, `summary.txt` per case for human inspection

### Scenarios
- **Truncation** — all 5 log formats, property: agent correctly truncates at `max_message_size_bytes` with `...TRUNCATED...` marker
- **Timestamp multiline** — 3 formats (TimestampPrefixed, Syslog5424, ApacheCommon), property: continuations merged with correct header
- **JSON multiline** — 11 structural variants (flat, nested, deep, arrays, unicode, escapes, etc.), property: compacted JSON preserves all fields. Discovered top-level array limitation mechanically.
- **Mixed multiline** — all formats interleaved, property: format transitions don't break aggregation
- **Adaptive sampling** — model-based credit state machine, variable-length action sequences, tolerance=0 exact matching

### Key Ideas Worth Preserving
1. **UUID markers for correlation** — embed a unique ID in each log line so we can match inputs to outputs. For adaptive sampling, use zero-padded numeric IDs (UUIDs have variable hex/letter mixes that the tokenizer treats as different patterns).
2. **Model-based property testing** — define a simplified model of expected behavior, simulate both model and real system on same inputs, compare. The model is intentionally simpler than the implementation.
3. **Action sequences** — write/sleep/write enables timing-dependent tests (credit refill, burst exhaustion).
4. **Structural diversity** — proptest generates diverse input structures (JSON variants, format mixes, variable sequence lengths) to mechanically discover edge cases.
5. **`summary.txt`** — human-readable test case summary showing the action sequence, line counts, and results. Critical for debugging and demos.
6. **Temp dir preservation** — on failure (always) and on success (with `LADING_KEEP_TEMP=1`), preserve all artifacts for inspection.

### Key Discoveries
- Agent's JSON aggregator doesn't support top-level arrays (`[{...}]`) — discovered mechanically by generating diverse JSON structures
- UUIDs have variable hex digit/letter ratios that the tokenizer classifies as different patterns — discovered when adaptive sampling didn't trigger
- Agent appends `...TRUNCATED...` (15 bytes) to truncated head chunks, splits overflow into separate tail entries
- Aggregated multiline messages use literal `\n` (two chars `\` and `n`), not actual newline byte

## What to Reuse from datadog-agent

The agent repo already has infrastructure we should use instead of rebuilding:

### `test/fakeintake/`
Go-based fake intake server that already handles:
- POST to `/api/v2/logs` and `/v1/input`
- Automatic gzip/zstd decompression
- JSON log payload parsing
- Payload retrieval via `/fakeintake/payloads?endpoint=<route>`
- Payload flushing via `/fakeintake/flushPayloads`

This replaces our Rust intake server. It's more mature and maintained by the agent team.

### `test/regression/`
Existing regression test infrastructure using lading configs. We may be able to integrate with or extend this.

### Agent CI infrastructure
Existing Docker-based test workflows we can hook into.

## What to Build New in datadog-agent

### E2E Test Harness
A test runner (Go or Rust, TBD) that:
1. Starts fakeintake
2. Configures and starts the agent (container or local binary)
3. Executes action sequences (write log files, sleep)
4. Collects payloads from fakeintake
5. Runs property assertions
6. Outputs human-readable results

### Property Framework
Composable property checks against fakeintake payloads. The model-based approach for adaptive sampling, plus structural checks for truncation and multiline.

### Allium Spec Integration (future)
Parse the `.allium` spec to derive:
- State invariants → post-condition assertions after each action
- Behavioral properties → end-to-end property checks
- Operation rules → action sequence generation guidance

This is the long-term goal — when the spec evolves, tests evolve automatically.

## Migration Plan

### Phase 1: Allium Spec Cleanup (current focus)
- Complete the in-progress allium work (pattern isolation, log emission, credit recovery, table ordering)
- Ensure spec is comprehensive and maintained alongside code

### Phase 2: Unit Test Expansion
- Inspect existing 19 unit tests in `sampler_test.go`
- Consider converting to proptest-style (Go equivalent: `rapid` or `gopter`) for broader input coverage
- Add tests for spec properties not currently covered:
  - CreditRecovery
  - Isolation invariant
  - TableOrdered (stable sort)
  - LogEmission contract (tag format/timing)
  - Config validation

### Phase 3: E2E Test Integration
- Build e2e test harness in datadog-agent repo
- Reuse fakeintake, agent container/binary infrastructure
- Port adaptive sampling model-based tests
- Port truncation and multiline tests if valuable
- Wire into CI as a separate slow test suite (nightly or merge-gate)

### Phase 4: Spec-Driven Testing (aspirational)
- Parse allium spec
- Auto-generate property assertions from invariants
- Auto-generate action sequences from operation rules
- Tests evolve when spec evolves

## Architecture in datadog-agent

```
datadog-agent/
  pkg/logs/internal/decoder/preprocessor/
    adaptive_sampler.allium    ← spec (source of truth)
    sampler.go                 ← implementation
    sampler_test.go            ← unit tests (fast, every commit)
    sampler_proptest_test.go   ← property tests (fast, every commit)
  test/
    e2e/                       ← or similar location
      adaptive_sampling/
        model.go               ← credit state machine model
        properties.go          ← property assertions
        scenarios.go           ← action sequence generation
        e2e_test.go            ← test entry points (slow, nightly)
    fakeintake/                ← existing, reused
```

## CI Integration

| Test Type | Speed | When to Run | What it Catches |
|---|---|---|---|
| Unit tests | < 1s | Every commit | Algorithm logic bugs |
| Property tests | < 10s | Every commit | Edge cases in isolated components |
| E2E tests | ~1 min per case | Nightly / merge gate | Pipeline integration, config plumbing, timing, output format |

## Open Questions

- **Go or Rust for e2e harness?** The agent is Go, fakeintake is Go, but our prototype is Rust. Go is probably the right choice for agent-repo integration. The property generation could use `gopter` or `rapid` for Go proptest equivalents.
- **How to handle the slow e2e test speed?** Each case is ~50s (30s warmup + 15s drain + overhead). With FUSE-based log delivery the warmup could potentially be reduced. Alternatively, keep the agent running across cases for scenarios where state leakage is acceptable.
- **Allium spec parsing** — is there an existing parser? If allium is TLA+-like, there may be tooling. If it's custom, we'd need to build a parser.
- **Multi-pattern testing** — the black-box pattern probing approach (burst_size=1, send pairs, observe same/different) is the cleanest way to test the tokenizer without reimplementing it. Should this be a unit test, e2e test, or both?

## References

### lading_proptest docs (this repo)
- `README.md` — overview, running instructions, env vars
- `PROPERTIES.md` — detailed property documentation
- `ADAPTIVE_SAMPLING.md` — adaptive sampling testing strategy and discussion
- `MIGRATION.md` — this document

### Agent repo
- `pkg/logs/internal/decoder/preprocessor/adaptive_sampler.allium` — formal spec
- `pkg/logs/internal/decoder/preprocessor/sampler.go` — implementation
- `pkg/logs/internal/decoder/preprocessor/sampler_test.go` — unit tests
- `test/fakeintake/` — fake intake server
- Branch: `blt/add_pattern_isolation_invariant_to_adaptive_sampling_spec` — spec additions in progress
