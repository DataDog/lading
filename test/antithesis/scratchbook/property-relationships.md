---
sut_path: /home/ssm-user/src/lading
commit: 51148899
updated: 2026-07-24T21:28:15Z
external_references:
  - name: the deployment (production runner + local dev harness)
    why: real shutdown/deploy model — how lading is launched, stopped (self-exit on experiment timer; SIGKILL via docker rm --force; watchdog (warmup+samples+30)*1.2), and how captures are merged/attributed. Grounds which shutdown/termination properties are LIVE vs MOOT on production paths.
  - name: Jira (project SMPTNG) + Confluence (datadoghq.atlassian.net)
    why: existing bug tickets and design docs — SMPTNG-725 (hang-in-spin after "lading shutdown"), SMPTNG-719/697 (ungraceful-termination telemetry loss), SMPTNG-694 (zstd-only captures / crash-consistency), SMPTNG-390 (blackhole traffic recorder), SMPTNG-762/767 (entropy / wall-clock determinism analogs), SMPTNG-735 Antithesis Triage epic; DST/determinism Confluence pages.
  - name: the whole lading repo (this checkout)
    why: source of truth for every evidence line-number, ADR (determinism / no-panic / pre-computation / performance-first), the existing panic hook (antithesis_hooks.rs:27), the harness under test/antithesis/, and the backup/blt branches where several fixes live but are NOT on main.
---

# Property relationships

Bug-hunting invariant catalog for lading, clustered by **subsystem**, tagged
**graceful / abrupt / any-path** (which termination timeline exercises it), and
annotated with **dominance / overlap** (which property is the umbrella and which
are its subsumed or dependent members). Every slug below appears in the catalog
JSON; nothing is invented here.

Legend
- **Path**: `graceful` = experiment-timer or SIGTERM drain; `abrupt` = SIGKILL /
  node_termination / hard restart; `any` = config-load / steady-state / rate /
  no-panic invariant independent of the termination timeline.
- **Type**: Antithesis assertion type (Unreachable / Always / Sometimes).
- **P**: priority. **main?**: `LIVE` = defect present on `main`; `fix-on-main` =
  merged; `guard` = holds, property guards against regression.

Cross-cutting note used throughout: fixes referenced from git history
(divide stall, linear ramp, gRPC throttle bypass, observer panics, logrotate
stale-tick, per-generator semaphore, capture abort, silent discards, unbounded
error labels, block-cache/trace-agent hangs) live on backup/blt branches, **not
`main`** — they are LIVE regressions here.

---

## 1. no-panic (umbrella over every Unreachable panic site)

**Dominance:** `no-panic-anywhere` is the umbrella. It is not proved directly;
it is the disjunction "no panic-site property is violated on any timeline",
observed through the existing panic hook (`antithesis_hooks.rs:27`,
`unreachable! "lading panicked"`) plus `panic=abort`. Every `Unreachable`
property in **any** subsystem below is a specific witness that would trip this
umbrella. Adding a new panic-site property = narrowing the umbrella, never
replacing it.

| slug | Path | Type | P | main? | Notes / evidence |
|---|---|---|---|---|---|
| `no-panic-anywhere` | any | Unreachable | P0 | guard | Umbrella; hook already wired at `antithesis_hooks.rs:13-33`. |
| `block-cache-zero-max-no-panic` | any (startup) | Unreachable | P1 | LIVE | `random_range(0..0)` at `block.rs:628`; grpc.rs forwards unvalidated. |
| `per-generator-semaphore-no-panic` | any (startup) | Unreachable | P1 | LIVE | static `OnceCell::set` double-set for 2×Http/2×SplunkHec. |
| `splunk-hec-response-parse-no-panic` | any | Unreachable | P1 | LIVE | `.expect` on `from_slice::<HecResponse>` at splunk_hec.rs:371-374. |
| `generator-addr-uri-validation-no-panic` | any (startup) | Unreachable | P1 | LIVE | `.expect` on addr/uri: tcp.rs:154, udp.rs:164, grpc.rs:226-247. |
| `observer-target-pid-recv-no-panic` | any | Unreachable | P0 | LIVE | `.expect("catastrophic failure")` on closed PID channel, observer.rs:114-120. |
| `tcp-rr-listener-no-panic` | any (startup) | Unreachable | P1 | LIVE | expect/panic! in create_listener, tcp_rr.rs:345-346; threads>1 on async task at :179. |
| `logrotate-stale-tick-noop` | any | Unreachable | P1 | LIVE | stale-tick assert in logrotate_fs/model.rs (fix 220850e5 not on main). |
| `arbitrary-block-nonzero-no-panic` | any (fuzz only) | Unreachable | P2 | LIVE | fuzz-feature only; `NonZeroU32` expect at block.rs:119-127. |

**Overlap across subsystems:** the observer panic trio
(`observer-pid-reuse-no-panic`, `observer-process-vanish-no-panic`,
`observer-cpu-max-parse-no-panic`) and `capture-write-failure-not-abort` are
*also* `Unreachable` witnesses of `no-panic-anywhere` but are catalogued under
their owning subsystem (observer / capture) because their oracle and fix live
there. Treat them as members of this umbrella when counting panic coverage.

---

## 2. lifecycle/shutdown (IMMEDIATE PRIORITY)

**Dominance:** `shutdown-completes-bounded` is the P0 liveness **umbrella**:
"once shutdown is signaled, lading exits within `max_shutdown_delay` (30s)". It
is violated by any one of the per-loop hang properties below. `needs_target:
true` on the umbrella; its members are the concrete hang sites to instrument.

Members subsumed by `shutdown-completes-bounded`:

| slug | Path | Type | P | main? | Hang site |
|---|---|---|---|---|---|
| `tcp-connect-loop-shutdown-responsive` | graceful | Always | P0 | LIVE | connect before select, tcp.rs:230-283. |
| `unix-stream-partial-write-shutdown` | graceful | Always | P0 | LIVE | inner write loop + yield_now spin, unix_stream.rs:281-318. |
| `unix-datagram-connect-loop-shutdown` | graceful | Always | P0 | LIVE | connect/retry loop, unix_datagram.rs:246-264. |
| `grpc-connect-loop-shutdown` | graceful | Always | P1 | LIVE | connect loop, grpc.rs:287-299. |
| `target-wait-bounded` | graceful | Always | P0 | LIVE | untimed `wait()`, target.rs:432 / inspector.rs:176. |
| `docker-target-discovery-bounded` | graceful (startup) | Always | P0 | LIVE | unbounded discovery poll, target.rs:212-244 — **production observer path**. |
| `capture-finalize-bounded` | graceful | Always | P1 | LIVE | unbounded `handle.await`, lading.rs:713-715. |

Graceful-contract properties (the entry into that timeline and its cleanup):

| slug | Path | Type | P | main? | Notes |
|---|---|---|---|---|---|
| `sigterm-graceful-drain` | graceful | Always | P0 | LIVE | no `SignalKind::terminate` arm at lading.rs:658. MOOT under node_termination (SIGKILL), LIVE on orchestrator-stop. Confirmed by SMPTNG-719/697. |
| `target-grace-period-honored` | graceful | Always | P1 | LIVE | ~0 grace: JoinSet dropped -> kill_on_drop SIGKILL after flush, lading.rs:690-717. Open Q: is ~0 intentional? |
| `orphaned-children-on-signal-death` | graceful + abrupt | Always | P1 | LIVE | kill_on_drop can't fire on untrapped signal; single-pid not process-group. MOOT under whole-container SIGKILL, LIVE in shared-PID-ns. |
| `lading-completes-and-exits-cleanly` | graceful | Sometimes | P0 | guard | **Non-vacuity anchor**: at least one timeline reaches exit-0 + footer-complete capture. Guards against "every timeline hangs/aborts". |

**Overlap / dependency edges (lifecycle ↔ capture):**
- `parquet-footer-on-graceful-exit` (capture) **depends on** both
  `sigterm-graceful-drain` and `capture-write-failure-not-abort` holding — it is
  the operational payoff of a correct graceful path.
- `capture-finalize-bounded` sits in both clusters: it is a shutdown-liveness
  member *and* a capture-finalization concern; catalogued here because its
  violation is a hang, not a corrupt file.
- Deployment reality (external_reference #1): production stops lading only via
  self-exit or SIGKILL, so `sigterm-graceful-drain` / `orphaned-children` /
  `target-grace-period-honored` / `target-wait-bounded` are **MOOT on the
  production runner path** but LIVE on the orchestrator-stop / Binary-target /
  bare-host paths — keep them, mark the moot-ness, drive them with an explicit
  `kill -TERM` harness step. SMPTNG-725 is the strongest deployment match for the
  hang class.

---

## 3. capture

**Dominance:** two independent umbrellas by timeline.
- **graceful:** `parquet-footer-on-graceful-exit` — every graceful exit yields a
  readable footer-complete parquet. Depends on lifecycle graceful path (§2) and
  on `capture-write-failure-not-abort`.
- **abrupt:** `jsonl-prefix-valid-after-kill` — every SIGKILLed jsonl file is a
  valid strictly-monotonic prefix. Already implemented as the harness checker
  (`anytime_capture_consistent.rs`), so it is the reference oracle other capture
  properties reuse.

| slug | Path | Type | P | main? | Notes |
|---|---|---|---|---|---|
| `capture-write-failure-not-abort` | any (steady) | Unreachable | P0 | LIVE | `.block_on(start()).expect` + panic=abort SIGABRTs mid-flush, lading.rs:476/501/526. Also a `no-panic-anywhere` witness. |
| `parquet-footer-on-graceful-exit` | graceful | Always | P0 | guard/dep | Footer only in close(), parquet.rs:307-324. |
| `jsonl-prefix-valid-after-kill` | abrupt | Always | P1 | guard | Oracle live: `anytime_capture_consistent.rs:44,49`; MIN_RECORDS=10. |
| `capture-no-fsync-durability` | abrupt | Always | P2 | LIVE | no sync_all/sync_data; jsonl.rs:53 flush-only. Open Q: does node_termination preserve page cache to the named volume? (P2↔P1). |
| `multi-format-parquet-not-forfeited` | graceful | Always | P1 | LIVE | jsonl.close()? before parquet.close()?, multi.rs:69-72. |
| `capture-drift-no-silent-gap` | any (stall) | Always | P2 | LIVE | drift loop advances ticks w/o flush, state_machine.rs:219-232; gaps invisible to validator. |
| `capture-histogram-drops-counted` | any (load) | Always | P1 | LIVE | try_send warn-drop, manager.rs:121-133 (counter fix 39d8ae56 not on main). |
| `split-mode-merge-partial-tolerant` | abrupt | Always | P2 | LIVE(lead) | deployment-derived; one clean side must not be lost to the other's corruption. Open Q per digest. |

**Overlap:** `capture-write-failure-not-abort` belongs to both this cluster and
the §1 no-panic umbrella. `capture-histogram-drops-counted` and
`capture-drift-no-silent-gap` are **observability** siblings — loss must be
*counted / visible*, not silent — analogous to `discarded-blocks-counted` in §5.
Note SMPTNG-694: captures are zstd-only now; the "valid prefix" assumption for
`jsonl-prefix-valid-after-kill` must be re-checked against zstd framing (also see
`recorded-traffic-crash-consistency` in §7).

---

## 4. payload/determinism

**Dominance:** `payload-determinism-byte-identical` is the umbrella ADR
invariant (same seed+config -> identical bytes). `no-wall-clock-in-payloads` is
its most likely violation vector (a specific entropy/clock source), so it is a
subsumed member, not a peer. The two block-cache properties are **liveness /
no-panic** members that live here because construction is a payload concern.

| slug | Path | Type | P | main? | Notes |
|---|---|---|---|---|---|
| `payload-determinism-byte-identical` | any | Always | P1 | guard | BTreeMap/BTreeSet, rng-derived timestamps confirmed. Analog leads: SMPTNG-762/767. |
| `no-wall-clock-in-payloads` | any | Always | P2 | guard | Member of the determinism umbrella; replay under perturbed clock. |
| `block-cache-construction-terminates` | any (startup) | Always | P0 | LIVE | no time/iteration cap, block.rs:625-673 (general case live; v04 fixed off-main). |
| `trace-agent-v04-block-terminates` | any (startup) | Always | P0 | fix-on-main | 31h hang -> 0.75s (456e85a3). Specialization of the line above for v04. |
| `dogstatsd-tag-length-validated` | any (startup) | Always | P2 | fix-on-main | e98e3052 (#1875); guards regression. |

**Overlap:** `block-cache-zero-max-no-panic` (§1) is the panic-site sibling of
`block-cache-construction-terminates` — same construction path, one is a hang,
the other an empty-range panic; both reachable via unvalidated
`maximum_block_size` (`grpc.rs:205-223`). `trace-agent-v04-block-terminates` is
**dominated by** `block-cache-construction-terminates` (the general bounded-
construction invariant); keep the v04 one as the regression witness.

---

## 5. generators/throttle

**Dominance:** two umbrellas.
- **rate-fidelity:** aggregate delivered rate == configured `bytes_per_second`.
  Members: `throttle-divide-no-silent-underdelivery`, `linear-ramp-slope-preserved`,
  `grpc-honors-throttle`, `unix-throttle-aggregate-consistent`,
  `divide-by-zero-startup-error`, bounded above by `stable-burst-envelope-bounded`.
- **no-livelock / observability:** a bad config fails fast or delivers nonzero,
  never a 100%-CPU zero-delivery spin, and under-delivery is *counted*. Members:
  `throttle-capacity-no-zerodelivery-livelock` (umbrella here),
  `discarded-blocks-counted`, `unix-stream-write-error-progress`,
  `error-label-cardinality-bounded`.
- **non-vacuity:** `sink-receives-bytes` (Sometimes) proves the whole delivery
  pipeline is not dead on at least one timeline — it dominates as a floor under
  every rate-fidelity member (if it fails, a rate check is vacuous).

| slug | Path | Type | P | main? | Cluster |
|---|---|---|---|---|---|
| `throttle-divide-no-silent-underdelivery` | any | Always | P0 | LIVE | rate-fidelity; delivers at N=1 must deliver at N>1 (0868e39c off-main). |
| `linear-ramp-slope-preserved` | any | Always | P0 | LIVE | rate-fidelity; Linear divide leaves rate_of_change => N× slope (914bb14a off-main). |
| `grpc-honors-throttle` | any | Always | P0 | LIVE | rate-fidelity; `let _ = result;` grpc.rs:307-318 (944d4be4 off-main). |
| `unix-throttle-aggregate-consistent` | any | Always | P1 | LIVE | rate-fidelity; unix gens miss `.divide()`. Open Q: per-connection intentional? |
| `divide-by-zero-startup-error` | any (startup) | Always | P2 | LIVE | rate-fidelity edge; bps<connections => clear error + remainder distributed. |
| `stable-burst-envelope-bounded` | any | Always | P1 | guard | rate-fidelity ceiling; Kani-backed, feature-gated assert (3f4a6bd2 off-main). |
| `throttle-capacity-no-zerodelivery-livelock` | any | Always | P0 | LIVE | no-livelock umbrella; block>capacity busy-discard spin. |
| `discarded-blocks-counted` | any | Always | P1 | LIVE | observability; blocks_discarded counter (73c4805e off-main). |
| `unix-stream-write-error-progress` | any (fault) | Always | P2 | LIVE | no-livelock; non-BrokenPipe reset spins offset, unix_stream.rs:304-315. |
| `error-label-cardinality-bounded` | any (fault) | Always | P1 | LIVE | bounded-memory; io::ErrorKind labels not raw string (32dd4cf6 off-main; gRPC still raw). |
| `sink-receives-bytes` | any | Sometimes | P1 | guard | non-vacuity floor; sink/main.rs:82 already instrumented. |

**Overlap:** `discarded-blocks-counted` (this cluster, observability) mirrors
`capture-histogram-drops-counted` (§3) — same "loss must be counted" principle
across subsystems. `throttle-capacity-no-zerodelivery-livelock`,
`throttle-divide-no-silent-underdelivery`, and `discarded-blocks-counted` form a
tight triangle around one config class (block > per-worker capacity): fail-fast,
don't-silently-drop, and count-if-you-do respectively.

---

## 6. observer

**Dominance:** the panic trio are `Unreachable` members of the §1
`no-panic-anywhere` umbrella but cluster here by fault class = *target lifecycle
race*. `observer-transient-read-not-fatal` is the liveness umbrella (a component
read error must not `?`-propagate to a dead run). `observer-pid-identity-
fingerprint` is the correctness (right-process) invariant that dominates the
PID-reuse pair semantically: `observer-pid-reuse-no-panic` says "don't crash on
reuse", `observer-pid-identity-fingerprint` says "don't silently attach to the
impostor" — the latter is the stronger claim.

| slug | Path | Type | P | main? | Fault class |
|---|---|---|---|---|---|
| `observer-pid-reuse-no-panic` | any | Unreachable | P0 | LIVE | `assert!(cur_pid==pid)` stat.rs:82 (6aa1b1ba off-main). |
| `observer-process-vanish-no-panic` | any | Unreachable | P0 | LIVE | `panic!` process_descendents.rs:13 (7e1d2968 off-main). |
| `observer-cpu-max-parse-no-panic` | any | Unreachable | P0 | LIVE | cpu.max parse bounds/zero-period (6aa1b1ba off-main). |
| `observer-transient-read-not-fatal` | any (fault) | Always | P1 | LIVE | liveness umbrella; best-effort component reads (30b86a71 off-main). |
| `observer-pid-identity-fingerprint` | any | Always | P2 | LIVE | correctness; start_time fingerprint vs PID-reuse impostor. Dominates the reuse pair. |
| `get-available-memory-cgroup-chain` | any | Always | P2 | fix-on-main | 1085887c; walks cgroup v2 ancestors. guards regression. |

**Overlap:** `observer-target-pid-recv-no-panic` is catalogued under §1 (its
subsystem field is `no-panic`) but is functionally an observer property — same
target-lifecycle-race fault class. When building the `target-lifecycle-races`
scenario, drive all four observer panic sites plus the identity fingerprint from
one setup (target exits before PID, vanishes mid-listing, PID recycled).

---

## 7. blackhole/config

**Dominance:** no single umbrella; two overlapping invariant families.
- **never-backpressure-the-target:** `blackhole-never-backpressures-target` is
  the umbrella; `datadog-blackhole-accept-resilient` is a specific failure mode
  (accept-loop wedge) that violates it, and `sqs-receive-message-bounded` is the
  amplification/OOM sibling.
- **restart / crash resilience:** `unix-datagram-blackhole-removes-stale-socket`
  (abrupt-restart) and `recorded-traffic-crash-consistency` (abrupt).
- **validate-early:** `config-numeric-fields-validated` (config load).

| slug | Path | Type | P | main? | Notes |
|---|---|---|---|---|---|
| `blackhole-never-backpressures-target` | any (load) | Always | P1 | LIVE | blocking `send().await` per point before response, datadog.rs:296-299. Umbrella. |
| `datadog-blackhole-accept-resilient` | any (fault) | Always | P1 | LIVE | fallible select arm wedges on accept Err, datadog.rs:207-212. Violates umbrella above. |
| `sqs-receive-message-bounded` | any | Always | P2 | LIVE | uncapped max_number_of_messages, sqs.rs:257-267. Amplification/OOM. |
| `unix-datagram-blackhole-removes-stale-socket` | abrupt (restart) | Always | P1 | LIVE | lazy remove_file future never awaited, unix_datagram.rs:95. Open Q: socket on persisted volume? |
| `recorded-traffic-crash-consistency` | abrupt | Always | P2 | LIVE(lead) | SMPTNG-390 / #1911 / #1895; zstd-framed truncation must be a valid prefix. |
| `config-numeric-fields-validated` | any (startup) | Always | P2 | LIVE | compression_level 1-22, sample_period>0, config.rs:186-198,106-107. |

**Overlap:** `tcp-rr-listener-no-panic` (§1) is a blackhole-startup property by
location but a no-panic property by class. `config-numeric-fields-validated`,
`dogstatsd-tag-length-validated` (§4), and `divide-by-zero-startup-error` (§5)
are all instances of the same **validate-early** ADR principle across three
subsystems — reject at load, not at runtime.

---

## Cross-subsystem dominance summary

Four umbrellas dominate the catalog; most other properties are their members or
their regression witnesses:

1. `no-panic-anywhere` (Unreachable, hook-observed) — dominates **all** panic-site
   properties across no-panic, observer, capture (`capture-write-failure-not-abort`).
2. `shutdown-completes-bounded` (Always, liveness) — dominates every per-loop
   connect/wait/finalize hang in lifecycle/shutdown.
3. `payload-determinism-byte-identical` (Always) — dominates
   `no-wall-clock-in-payloads`; `block-cache-construction-terminates` dominates
   `trace-agent-v04-block-terminates`.
4. Rate-fidelity + no-livelock pair (generators/throttle), floored by the
   `sink-receives-bytes` non-vacuity Sometimes and the
   `lading-completes-and-exits-cleanly` graceful non-vacuity Sometimes.

Observability siblings (`discarded-blocks-counted`,
`capture-histogram-drops-counted`, `capture-drift-no-silent-gap`) and
validate-early siblings (`config-numeric-fields-validated`,
`dogstatsd-tag-length-validated`, `divide-by-zero-startup-error`,
`generator-addr-uri-validation-no-panic`, `block-cache-zero-max-no-panic`) each
encode one ADR principle recurring across subsystems — instrument them together.
