---
sut_path: /home/ssm-user/src/lading
commit: 51148899
updated: 2026-07-24T21:28:12Z
external_references:
  - name: the deployment
    why: real shutdown/deploy model (production runner + local dev); how lading is launched, stopped, watchdog-timed, and how captures are collected — establishes which properties are MOOT vs LIVE
  - name: Jira/Confluence (datadoghq.atlassian.net, project SMPTNG)
    why: existing bug tickets and design docs — SMPTNG-725/719/697 (ungraceful-termination telemetry loss, hang-in-spin), SMPTNG-694 (compressed captures), SMPTNG-762/767 (entropy/wall-clock determinism analogs), SMPTNG-390 (traffic recorder)
  - name: whole lading repo
    why: source of truth for every property's evidence line/file; spot-verified against current main
---

# lading Antithesis portfolio evaluation

Source of truth: the property catalog (51 properties across no-panic, lifecycle/shutdown,
capture, generators/throttle, observer, blackhole/config, payload/determinism) plus the
discovery digest (deployment shutdown model, Jira/Confluence leads, git fix-history
regressions, per-subsystem source findings). Nothing here is invented; every claim traces
to a catalog `slug` or a digest finding.

The catalog is a strong bug-hunting portfolio: every property is a checkable invariant whose
violation is a real defect, and many encode LIVE regressions on `main` (the hardening fixes
live only on backup/`blt` branches). The gaps below are not in the *properties* — they are in
the **harness oracles and scenarios** that would actually exercise and observe them, and in a
handful of properties that are structurally vacuous or moot under the Antithesis fault model
as currently configured.

---

## 1. Coverage gaps (properties with no oracle / no scenario today)

Per the SDK-instrumentation inventory, the SUT binary carries only two real SDK sites (facade
init + panic hook at `antithesis_hooks.rs:27`, and a bootstrap `reachable!` at `lading.rs:792`).
All domain invariants live in `test/antithesis/`, and today that is only:

- `sink/main.rs:82` — `sometimes! (total>0)` load-arrival oracle.
- `harness/.../anytime_capture_consistent.rs` — jsonl torn/monotonic + parquet-readable-is-consistent (`always!`), plus `sometimes!` non-vacuity floors (MIN_RECORDS=10).
- `harness/.../first_sample_config.rs:36` — per-timeline config-menu anchor.
- The panic hook (covers the entire `no-panic` subsystem indirectly).

Everything else in the catalog has **no oracle wired**. Concrete gaps, grouped by what is missing:

### 1a. No shutdown-latency / liveness oracle (highest-value gap)
No external timer measures shutdown-signal -> process-exit, and no `reachable!`/`always!`
marks the bounded-exit branch of any connect/retry loop. Consequently these P0 properties are
**completely unchecked**:
- `shutdown-completes-bounded` (umbrella), `tcp-connect-loop-shutdown-responsive`,
  `unix-stream-partial-write-shutdown`, `unix-datagram-connect-loop-shutdown`,
  `grpc-connect-loop-shutdown`, `target-wait-bounded`, `docker-target-discovery-bounded`,
  `capture-finalize-bounded`.
- Needed scenarios (per catalog `scenarios_needed`): **unreachable-target**, **stalled-receiver**.
- Note: several of these connect-loop hangs are confirmed LIVE on main (tcp.rs:230-283 has no
  shutdown branch in the connect leg; unix_datagram.rs:246-264; grpc.rs:287-299;
  unix_stream.rs:281-318 busy-yields; target.rs:212-244 docker discovery has no shutdown/timeout arm).

### 1b. No CPU / livelock observation
`unix-stream-partial-write-shutdown`, `throttle-capacity-no-zerodelivery-livelock`,
`unix-stream-write-error-progress` all hinge on detecting a 100%-CPU busy-spin delivering
~zero bytes. The harness has no CPU/throughput observer. Scenarios **oversized-block-vs-capacity**
and **stalled-receiver** are unimplemented.

### 1c. No rate-fidelity oracle
The sink counts only "bytes > 0", not *rate*. So every rate invariant is unchecked:
`throttle-divide-no-silent-underdelivery`, `linear-ramp-slope-preserved`,
`grpc-honors-throttle`, `unix-throttle-aggregate-consistent`, `divide-by-zero-startup-error`,
`discarded-blocks-counted`. Catalog flags divide/linear-ramp as **pure proptests** provable
against the real Valve with no rig — cheapest win. Scenarios **oversized-block-vs-capacity**,
**linear-ramp-parallel** unimplemented.

### 1d. No determinism oracle
No `always!` on a rolling hash of emitted blocks and no two-seed byte-equality replay.
`payload-determinism-byte-identical` and `no-wall-clock-in-payloads` are unchecked. Scenario
**determinism-replay** (incl. perturbed-clock timestamp equality) unimplemented. Jira leads
SMPTNG-762 (CPU-jitter entropy) / SMPTNG-767 (non-monotonic wall clock) are the SUT-analog bug
classes to hunt.

### 1e. No memory / label-cardinality observation
`error-label-cardinality-bounded` and `sqs-receive-message-bounded` need a memory/key-count
observer + a flapping-target / adversarial-request scenario. None exists.

### 1f. No blackhole-fault scenarios
`datadog-blackhole-accept-resilient`, `blackhole-never-backpressures-target`,
`unix-datagram-blackhole-removes-stale-socket` need accept-fault-injection, slow-capture-drain,
and blackhole-restart-stale-socket scenarios. None exists. (All three are confirmed-live defects:
datadog.rs:207-212 fallible-select wedge; datadog per-point `send().await` before response;
unix_datagram.rs:95 remove_file future never awaited.)

### 1g. No target-lifecycle-race scenario
`observer-pid-reuse-no-panic`, `observer-process-vanish-no-panic`,
`observer-cpu-max-parse-no-panic`, `observer-target-pid-recv-no-panic`,
`observer-transient-read-not-fatal`, `observer-pid-identity-fingerprint` need
target-exits/PID-recycle/malformed-cgroup fault injection. The panic hook would catch the
aborts, but the **races are never induced**, so the properties are vacuously green.

### 1h. No malformed-config scenario
`generator-addr-uri-validation-no-panic`, `block-cache-zero-max-no-panic`,
`config-numeric-fields-validated`, `tcp-rr-listener-no-panic`, `per-generator-semaphore-no-panic`,
`splunk-hec-response-parse-no-panic` need the **malformed-config**, **small-and-zero-block-size**,
**multi-generator-config**, **tcp_rr-threads>1-bind-conflict** scenarios plus a non-JSON blackhole body.

### 1i. Split-mode and durability blind spots
`split-mode-merge-partial-tolerant`, `recorded-traffic-crash-consistency`,
`capture-no-fsync-durability` all depend on open questions about the deployment (persisted
named-volume page-cache retention; zstd framing of compressed captures per SMPTNG-694; oblivious
merge attribution). No scenario **split-mode-partial-kill** / **node-termination-mid-run** targets these.

---

## 2. Moot under SIGKILL (Antithesis `node_termination` cannot exercise these)

Antithesis node_termination = SIGKILL of the whole container (untrappable), and the digest
confirms **the deployment never sends lading SIGTERM** — production runs lading in Container
observer mode and tears down via `docker rm --force` (immediate SIGKILL). So lading's
SIGTERM/SIGINT graceful path is effectively dead code in production, and these graceful-only or
Binary-target-only properties **cannot be violated by the Antithesis SIGKILL fault** as configured:

| Property | Why moot under SIGKILL | Where it is still LIVE |
|---|---|---|
| `sigterm-graceful-drain` | SIGTERM untrappable; SIGKILL skips the graceful arm entirely | Deployment orchestrator-stop path (needs an explicit `kill -TERM` harness step) |
| `target-wait-bounded` | Target is a sibling container force-killed by the runner, not a lading child | Binary-target mode only |
| `target-grace-period-honored` | No SIGTERM delivered to lading; target is disposable | Binary-target mode with a cooperative slow target |
| `orphaned-children-on-signal-death` | Whole-container SIGKILL kills all pids together | Shared-PID-namespace / bare-host deployments |
| `capture-no-fsync-durability` | Only decidable if node_termination preserves page-cache to the persisted volume — **open question**; likely undecidable in-sim | Real VM/instance power-off on a persisted volume |

Action: to test the graceful contract at all, the harness must add an explicit
**sigterm-stop** step (send `kill -TERM` to lading and validate footer-complete parquet + clean
exit). Without it, `sigterm-graceful-drain`, `parquet-footer-on-graceful-exit` (graceful arm),
`target-grace-period-honored`, and `orphaned-children-on-signal-death` are never exercised.

---

## 3. Vacuous without a graceful exit or a target/sink

These pass **trivially (vacuously)** unless the precondition is arranged. They are correct
invariants but give false confidence until the trigger is guaranteed on some timeline.

### 3a. Vacuous without a *graceful* exit
The parquet footer is written *only* in `handle_shutdown -> format.close()`
(state_machine.rs:249-259; parquet.rs:307-324). If no timeline reaches a graceful shutdown,
every parquet-readability assertion is vacuous:
- `parquet-footer-on-graceful-exit` — needs a graceful exit to produce a readable footer at all.
- `capture-finalize-bounded` — the finalize await only runs on the graceful path.
- `lading-completes-and-exits-cleanly` (Sometimes / non-vacuity anchor) — **this is the guard**:
  it asserts the good path is reachable on >=1 timeline. If it never fires, the whole
  graceful/capture family is silently vacuous. Treat its `sometimes!` as a coverage tripwire.
- `capture-write-failure-not-abort` — vacuous unless a capture-write IO fault is actually injected
  (scenario **capture-write-fault**).

The `anytime_capture_consistent` checker already handles this correctly by design: the parquet
arm is `!readable || invariants_hold` (vacuous on unreadable/killed files, which is intended),
and `sometimes! (readable && records>=10)` guards against parquet *never* being finalized.

### 3b. Vacuous without a target/sink receiving load
All `needs_target: true` rate/delivery/observability properties are vacuous if no sink receives
bytes or no target is observed:
- `sink-receives-bytes` is the load-arrival non-vacuity guard for the entire generator/throttle family.
- `throttle-divide-no-silent-underdelivery`, `linear-ramp-slope-preserved`, `grpc-honors-throttle`,
  `unix-throttle-aggregate-consistent`, `divide-by-zero-startup-error`, `discarded-blocks-counted`,
  `throttle-capacity-no-zerodelivery-livelock`, `unix-stream-write-error-progress`,
  `error-label-cardinality-bounded` — all require delivered load to be measurable.
- Observer family (`observer-*`) is vacuous unless a target process exists and is sampled.

Cross-check: `throttle-divide-no-silent-underdelivery` and `linear-ramp-slope-preserved` escape
this vacuity because they are provable as **pure proptests against the real Valve** — no rig, no
target needed. Prioritize those (they need no scenario wiring at all).

---

## 4. Which properties need SUT fixes (live on main)

The catalog `needs_sut_fix` field is authoritative. The hardening fixes referenced in git
history live on backup/`blt` branches, **not main**, so the following are LIVE regressions the
harness should be able to catch (and which need code changes to *pass*):

### P0 SUT fixes — shutdown safety
- `sigterm-graceful-drain` — add `SignalKind::terminate` arm to the `lading.rs:658` select.
- `shutdown-completes-bounded` (umbrella) — shutdown branches on every pre-select connect/retry loop + timeout on `target_child.wait()` and capture-finalize await.
- `tcp-connect-loop-shutdown-responsive` — wrap connect in `select!` with `shutdown_wait` (tcp.rs:230-251).
- `unix-stream-partial-write-shutdown` — shutdown branch on the `blk_offset<blk_max` loop + connect loop (unix_stream.rs:281-318, :248-268); replace bare `yield_now` spin.
- `unix-datagram-connect-loop-shutdown` — shutdown branch (unix_datagram.rs:246-264).
- `docker-target-discovery-bounded` — shutdown arm and/or max-attempts timeout (target.rs:212-244). **This is the launched production observer mode — highest impact.**
- `target-wait-bounded` — timeout+escalate-to-SIGKILL around wait() (target.rs:432, inspector.rs:176).

### P0 SUT fixes — no-panic (abort under panic=abort)
- `capture-write-failure-not-abort` — replace `.expect` on capture start (lading.rs:476/501/526) with Result propagation; plumb mid-run write errors to graceful shutdown. (Partly on backup b7624af2.)
- `observer-pid-reuse-no-panic` — replace `assert!(cur_pid==pid)` at stat.rs:82 with skip-and-continue.
- `observer-process-vanish-no-panic` — yield empty iterator instead of `panic!` at process_descendents.rs:13.
- `observer-cpu-max-parse-no-panic` — bounds-check cpu.max parse / guard zero period.
- `observer-target-pid-recv-no-panic` — handle `recv()` Err/None with a returned error (observer.rs:114-120).
- `block-cache-construction-terminates` — cap consecutive rejections / add time bound (block.rs:625-673); reject sub-floor max_block_size.

### P0 SUT fixes — throttle rate fidelity
- `throttle-divide-no-silent-underdelivery` — divide must shrink block sizing with capacity (lib.rs divide; backup 0868e39c).
- `linear-ramp-slope-preserved` — divide `rate_of_change` by divisor in the Linear divide arm (lib.rs:178-193).
- `grpc-honors-throttle` — honor the throttle Result at grpc.rs:307-318 (backup 944d4be4).
- `throttle-capacity-no-zerodelivery-livelock` — validate `maximum_block_size <= bps/parallel_connections` at construction.

### P1 SUT fixes
- `capture-finalize-bounded` (timeout around lading.rs:713-715), `multi-format-parquet-not-forfeited` (reorder multi.rs to finalize parquet first), `capture-histogram-drops-counted` (backup 39d8ae56), `grpc-connect-loop-shutdown`, `per-generator-semaphore-no-panic` (backup 5f8c375e), `splunk-hec-response-parse-no-panic`, `generator-addr-uri-validation-no-panic`, `discarded-blocks-counted` (backup 73c4805e), `error-label-cardinality-bounded` (backup 32dd4cf6; gRPC still raw), `unix-throttle-aggregate-consistent`, `observer-transient-read-not-fatal` (backup 30b86a71), `tcp-rr-listener-no-panic`, `datadog-blackhole-accept-resilient`, `blackhole-never-backpressures-target`, `unix-datagram-blackhole-removes-stale-socket`, `logrotate-stale-tick-noop` (backup 220850e5), `orphaned-children-on-signal-death`, `target-grace-period-honored`, `block-cache-zero-max-no-panic`.

### P2 SUT fixes
- `capture-drift-no-silent-gap`, `divide-by-zero-startup-error`, `unix-stream-write-error-progress`, `observer-pid-identity-fingerprint`, `sqs-receive-message-bounded`, `config-numeric-fields-validated`, `capture-no-fsync-durability`, `arbitrary-block-nonzero-no-panic`.

### Need NO SUT fix (property/oracle-only; guards against regression)
`no-panic-anywhere` (hook wired), `jsonl-prefix-valid-after-kill`, `parquet-footer-on-graceful-exit`
(pure-graceful path), `payload-determinism-byte-identical`, `no-wall-clock-in-payloads`,
`stable-burst-envelope-bounded` (Kani), `get-available-memory-cgroup-chain` (merged 1085887c),
`dogstatsd-tag-length-validated` (merged e98e3052), `trace-agent-v04-block-terminates` (fix live on main),
`lading-completes-and-exits-cleanly`, `sink-receives-bytes`.

---

## 5. Prioritized action plan (shutdown safety first)

### Phase 0 — Make the graceful/liveness family non-vacuous (do first)
1. **Add a shutdown-latency oracle**: external timer measuring shutdown-signal -> exit, asserting `<= max_shutdown_delay` (30s); plus a `sometimes!`/`reachable!` at every connect-loop bounded-exit branch. Unblocks the entire §1a P0 set.
2. **Add the `unreachable-target` scenario** (generator points at an address/socket/uri/container that never binds). Directly exercises `tcp/udp/unix_stream/unix_datagram/grpc-connect-loop-shutdown` and `docker-target-discovery-bounded` — all confirmed LIVE hangs on main.
3. **Add an explicit `sigterm-stop` harness step** (`kill -TERM lading`) so `sigterm-graceful-drain`, `parquet-footer-on-graceful-exit`, `target-grace-period-honored`, `orphaned-children-on-signal-death` stop being moot (§2). Assert footer-complete parquet + clean exit + no orphans.
4. **Guard non-vacuity**: ensure `lading-completes-and-exits-cleanly` (`sometimes!`) and the parquet `sometimes!(readable && records>=10)` arm actually fire in triage; if not, the whole capture family is silently vacuous.

### Phase 1 — Shutdown-safety SUT fixes (P0, land on main)
Apply the §4 P0 shutdown fixes: SIGTERM handler; shutdown branches on all connect/retry loops; bounded `target_child.wait()`; bounded docker discovery; bounded capture-finalize; capture-write-error -> graceful exit (not abort). Strong deployment lead: SMPTNG-725 "RJO alive but not really" (hang-in-spin after "lading shutdown") and SMPTNG-719/697 (ungraceful-termination telemetry loss) corroborate these as observed-in-production classes.

### Phase 2 — No-panic race scenarios (P0)
Add `target-lifecycle-races` (target exits before PID / vanishes mid-listing / PID recycled) and `capture-write-fault` (disk-full/EIO on a flush tick). Panic hook already the oracle; these scenarios *induce* the races so the observer no-panic quartet + `capture-write-failure-not-abort` stop being vacuous. Land the corresponding stat.rs:82 / process_descendents.rs:13 / observer.rs:114 fixes.

### Phase 3 — Rate fidelity (P0, cheap)
Land `throttle-divide-no-silent-underdelivery` and `linear-ramp-slope-preserved` as **pure proptests against the real Valve** (no rig, no target — no vacuity risk). Then add a sink *rate* oracle + `oversized-block-vs-capacity` and `linear-ramp-parallel` scenarios for `grpc-honors-throttle`, `throttle-capacity-no-zerodelivery-livelock`, `discarded-blocks-counted`.

### Phase 4 — Determinism (P1)
Add `determinism-replay` (two seeded runs, byte-identical at sink or block-cache hash) and a perturbed-clock timestamp-equality check. Hunt the SMPTNG-762/767 entropy/wall-clock classes inside lading. No SUT fix expected; guards the determinism ADR.

### Phase 5 — Blackhole & config faults (P1)
`accept-fault-injection` (datadog wedge, fd exhaustion), `slow-capture-drain` (datadog per-point backpressure), `blackhole-restart-stale-socket` (unix_datagram remove_file), `malformed-config` / `small-and-zero-block-size` / `multi-generator-config` / `tcp_rr-threads>1-bind-conflict`. Land the matching §4 P1 fixes.

### Phase 6 — Durability, split-mode, cardinality (P2, resolve open questions first)
`node-termination-mid-run` (jsonl prefix + parquet consistency — largely covered by the existing checker), `split-mode-partial-kill`, and cardinality/OOM (`error-label-cardinality-bounded`, `sqs-receive-message-bounded`). Blocked on deployment open questions: does node_termination preserve page-cache to the persisted volume? are compressed captures (SMPTNG-694 zstd) a valid prefix when truncated? does a sender-only overrun fail a clean receiver's replicate?

### Open questions to resolve against the deployment before Phase 6
- Does Antithesis ever deliver SIGTERM, or only SIGKILL? (Determines whether §2 items are reachable at all in-sim vs only via an explicit harness `kill -TERM`.)
- Does node_termination preserve OS page-cache to the persisted named volume? (P2 vs P1 for `capture-no-fsync-durability`.)
- Are compressed capture streams (post-SMPTNG-694) a valid decodable prefix after truncation, or undecodable past the last flushed frame? (Reframes `jsonl-prefix-valid-after-kill` / `recorded-traffic-crash-consistency`.)
- Is the unix socket path on a persisted volume? (Impact of `unix-datagram-blackhole-removes-stale-socket`.)
- Is per-connection (undivided) throttle intentional for unix_stream? (`unix-throttle-aggregate-consistent`.)
- Is `tcp_rr` ever run with threads>1? (`tcp-rr-listener-no-panic`.)
