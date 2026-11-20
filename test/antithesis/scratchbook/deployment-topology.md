---
sut_path: /home/ssm-user/src/lading
commit: 51148899
updated: 2026-07-24T21:28:13Z
external_references:
  - name: the deployment (production runner-orchestrator, generic name)
    why: real shutdown/deploy model — RJO launches lading as an ECR container in a
      docker/podman pod, uses --target-container observer mode (not Binary target),
      never sends lading a signal (self-termination on the experiment timer is the
      only graceful stop), tears everything down with `docker rm --force` (SIGKILL),
      and enforces bounded shutdown with a (warmup+samples+30)*1.2 watchdog. Defines
      what "correct shutdown" means and which faults are live vs moot in production.
  - name: Jira (SMPTNG project) + Confluence
    why: existing bug tickets and design docs — SMPTNG-725 "RJO alive but not really"
      (hang-in-spin after "lading shutdown"), SMPTNG-719/697 (ungraceful-termination
      telemetry loss, graceful-termination gap Won't-Fixed), SMPTNG-694 (captures now
      zstd-compressed — revisit the "valid jsonl prefix" assumption), SMPTNG-390 +
      #1911/#1895 (blackhole traffic recorder crash-consistency), SMPTNG-762/767
      (SUT wall-clock/entropy classes mirroring lading's determinism ADR).
  - name: whole lading repo (this checkout)
    why: the SUT source spot-verified against current main; every property below is a
      checkable invariant whose violation is a real lading defect (file:line evidence
      in the catalog).
---

# Antithesis deployment topology and scenarios

Minimal set of Antithesis topologies that cover `catalog.scenarios_needed`. Source
of truth is the property catalog JSON + discovery digest; nothing here is invented.
Each topology lists its containers, its fault profile, and the catalog property
slugs whose violation it would catch. Shutdown/termination safety is the immediate
priority (P0); the rest of the system is catalogued behind it.

Reused container roles (same code, different wiring per topology):

- **sink** — TCP byte-counting oracle, SDK-linked, never faulted. Owns
  `sink-receives-bytes` (`sometimes! total>0`). Already built (`test/antithesis/sink`).
- **lading** — the instrumented SUT (sancov, `panic=abort`; a panic → SIGABRT caught
  as the panic hook's `unreachable! "lading panicked"`). Config chosen per timeline.
- **workload** — driver + checker host. Emits `setup_complete`, writes the sampled
  `lading.yaml` via `first_sample_config`, and runs `anytime_capture_consistent` over
  the capture volume. Never faulted, so it survives to validate.
- **target** — (Binary/observer topologies only) the process lading supervises or
  observes.
- **load-source** — (blackhole topology only) a sender that drives traffic at a
  lading blackhole.

Volumes: `shared` (workload→lading config handoff), `capture` (named volume so the
capture survives a `node_termination` of lading for the checker).

---

## A. `general` — node-fault generator (EXISTS)

**Containers:** sink + lading(`--no-target`, TCP generator) + workload.
**Faults:** `node_termination` (SIGKILL) on `lading` only; cpu/clock jitter on lading.
**Config:** `--experiment-duration-infinite`, `--capture-path /capture/capture.jsonl`,
`--capture-flush-seconds 1`.

Justification: SIGKILL of the whole lading container is the exact Antithesis analogue
of the deployment's `docker rm --force` teardown and of a mid-run watchdog kill. It is
the only fault that is *live* in production (SIGTERM never reaches lading there). The
1s flush + named `capture` volume maximise kill-in-flight opportunities and preserve a
fresh file for the checker.

Proves / guards:
- `node-termination-mid-run` → `jsonl-prefix-valid-after-kill` (torn-record-free,
  monotonic prefix), `capture-no-fsync-durability` (open question: does node kill
  preserve page cache to the named volume?).
- `no-panic-anywhere` umbrella (panic hook), `sink-receives-bytes` non-vacuity.
- Config variation at the workload seam (`first_sample_config` value menu) rides this
  topology with **no new containers** and covers: `small-and-zero-block-size`
  (`block-cache-construction-terminates`, `block-cache-zero-max-no-panic`),
  `multi-generator-config` (`per-generator-semaphore-no-panic`,
  `splunk-hec-response-parse-no-panic`), `malformed-config`
  (`generator-addr-uri-validation-no-panic`, `config-numeric-fields-validated`,
  `dogstatsd-tag-length-validated`), and the throttle/rate scenarios
  (`oversized-block-vs-capacity`, `linear-ramp-parallel`,
  `throttle-divide-no-silent-underdelivery`, `grpc-honors-throttle`,
  `discarded-blocks-counted`, `error-label-cardinality-bounded`) using the sink as
  the delivered-rate oracle.

Note: the throttle rate-fidelity items (`throttle-divide-*`, `linear-ramp-*`,
`stable-burst-envelope-bounded`) are also provable as **pure proptests** against the
real `Valve` with no Antithesis rig — prefer that where possible; use this topology
only for the end-to-end sink-rate check.

---

## B. `graceful-timer` — lading owns the clock, exits 0 (NEW)

**Containers:** sink + lading + workload (same as A).
**Faults:** NO `node_termination` on lading. Clock jitter / cpu contention only
(Antithesis controls time to stress the timer and the drain).
**Config:** finite `--experiment-duration-seconds` + `--warmup-duration-seconds`,
`--max-shutdown-delay 30`, `--capture-format parquet` (matches the deployment's
`system.yaml`), `--capture-path /capture/capture.parquet`.

Justification: this is the *only* graceful stop that exists in the deployment — lading
self-terminates on its experiment timer, drains the maturity window, writes the parquet
footer, exits 0, and RJO copies the capture off before force-killing the pod. Removing
the node fault is deliberate: this scenario exists to prove the happy path is reachable
and bounded, which the SIGKILL scenario (A) can never observe.

Proves / guards:
- `lading-completes-and-exits-cleanly` (Sometimes / non-vacuity: at least one timeline
  reaches exit 0 with a footer-complete capture).
- `parquet-footer-on-graceful-exit` (Always: readable parquet after graceful exit) via
  the checker's parquet arm.
- `shutdown-completes-bounded` (Always: exit within `max_shutdown_delay`; the
  deployment watchdog makes this operational), `capture-finalize-bounded`.
- `determinism-replay` / `payload-determinism-byte-identical` /
  `no-wall-clock-in-payloads`: run two lading instances (or two runs) with the **same
  seed+config**; assert byte-identical bytes at the sink, and identical payload
  timestamps under a perturbed clock. This is where Antithesis clock control earns its
  keep (analogue of SMPTNG-762/767).

---

## C. `unreachable-sink` — connect-loop shutdown responsiveness (NEW)

**Containers:** sink + lading + workload — but a **network partition** between lading
and sink (Antithesis network fault), or sink held down, so the generator's target
address/socket/uri/container never becomes reachable.
**Faults:** network unreachability on the lading→sink edge; finite experiment duration.
**Variants (config only):** tcp / udp / unix_stream / unix_datagram / grpc generators,
and (with the docker socket present) a misnamed `--target-container`.

Justification: confirmed-live on main — the TCP connect loop (`tcp.rs:230-251`), the
unix_datagram (`:246-264`) and grpc (`:287-299`) connect loops, and the container
discovery loop (`target.rs:212-244`) all lack a shutdown branch. With an unreachable
peer the worker spins `connect → sleep → continue` forever and never reaches the
`select!` that polls shutdown, so the experiment timer's expiry cannot end the run. The
oracle is external: measure shutdown-signal → process-exit ≤ `max_shutdown_delay`.

Proves / guards: `shutdown-completes-bounded` (umbrella) and its per-loop children
`tcp-connect-loop-shutdown-responsive`, `unix-datagram-connect-loop-shutdown`,
`grpc-connect-loop-shutdown`, `docker-target-discovery-bounded`.

---

## D. `stalled-receiver` — backpressure/busy-spin under a slow peer (NEW)

**Containers:** sink + lading + workload, with the **sink modified to accept then
never read** (fills the socket buffer) and to respond slowly on HTTP paths.
**Faults:** none required beyond the slow-receiver behaviour; finite duration; observe
CPU.

Justification: `unix_stream.rs:281-318` busy-spins on `WouldBlock` via `yield_now` with
no shutdown branch (the source comment admits "if the read side has hung up we will
never know"); http acquires all connection permits on a slow target. Assert bounded
shutdown **and** no core pegged at 100%.

Proves / guards: `unix-stream-partial-write-shutdown`, `unix-stream-write-error-progress`,
`throttle-capacity-no-zerodelivery-livelock`, and the http backpressure lead.

---

## E. `sigterm-stop` — orchestrator-stop graceful path (NEW, P0)

**Containers:** sink + lading + workload; lading and workload share a PID namespace
(or workload has a docker exec seam) so the workload can `kill -TERM` lading mid-run.
**Faults:** an explicit `kill -TERM` step from the workload (Antithesis delivers only
SIGKILL via node_termination, so SIGTERM must be an explicit harness action).

Justification: main has **no `SignalKind::terminate` arm** (`lading.rs:658` only
`ctrl_c`), so today a SIGTERM kills lading non-gracefully (exit 143, no drain, no
footer, no child reap). This is MOOT on the production path (RJO never signals lading)
but is the canonical orchestrator-stop model and is directly tied to SMPTNG-719/697/725.
Only an explicit `kill -TERM` exercises it.

Proves / guards: `sigterm-graceful-drain` (readable parquet + non-abort exit after
SIGTERM), `parquet-footer-on-graceful-exit`, `orphaned-children-on-signal-death`
(no reparented target/inspector grandchildren afterward), `target-grace-period-honored`.

---

## F. `binary-target` — process supervisor + observer no-panic (NEW, P0)

**Containers:** sink + lading + **target** + workload. lading runs **without**
`--no-target`: config spawns the `target` process (`target.binary`) so lading is the
supervisor/observer. The `target` container image ships a small program with selectable
behaviours (value-menu): normal, exits-immediately, exits-before-sending-PID,
ignores-SIGTERM, forks-a-grandchild, rapid-exit-then-PID-reuse.
**Faults:** `node_termination` on lading; the target-behaviour selector is the primary
stressor; clock jitter.

Justification: the observer/Binary-target defects (untimed `target_child.wait()` at
`target.rs:432`, the `expect("catastrophic failure")` on the PID channel at
`observer.rs:114-120`, `assert!(cur_pid==pid)` at `stat.rs:82`, the `panic!` at
`process_descendents.rs:13`, cpu.max parse, kill_on_drop-on-signal orphaning) are only
reachable when lading actually supervises a process. Production uses observer mode
against a sibling container and does NOT exercise these, so this topology is where they
must be hunted. Each malicious target behaviour is one row of the value menu.

Proves / guards: `target-wait-bounded`, `observer-target-pid-recv-no-panic`,
`observer-pid-reuse-no-panic`, `observer-process-vanish-no-panic`,
`observer-cpu-max-parse-no-panic`, `observer-transient-read-not-fatal`,
`observer-pid-identity-fingerprint`, `orphaned-children-on-signal-death`,
`target-grace-period-honored`. (Covers `target-lifecycle-races` from
`scenarios_needed`.)

---

## G. `blackhole` — lading as sink under fault (NEW)

**Containers:** **load-source** (a sender — a second lading generator or a minimal
traffic client) + lading configured as a **blackhole** + workload.
**Faults:** fd exhaustion / transient `accept()` errors on the blackhole; a stale
unix_datagram socket left on a persisted volume then a blackhole restart; a throttled
capture-manager drain (slow-capture fault).

Justification: confirmed-live — the datadog blackhole's fallible `Ok((stream,_)) =
accept()` select arm (`datadog.rs:209`) wedges permanently on the first accept error
(silently backpressuring the target); the unix_datagram blackhole never awaits its
`remove_file` future (`unix_datagram.rs:95`) so a stale socket makes `bind` fail
EADDRINUSE on restart; the datadog record path blocks the HTTP response on a full
bounded capture channel. The oracle is the load-source's request success/latency (the
blackhole must never backpressure it) plus a bind-succeeds assertion on restart.

Proves / guards: `datadog-blackhole-accept-resilient`,
`unix-datagram-blackhole-removes-stale-socket`, `blackhole-never-backpressures-target`,
`sqs-receive-message-bounded`, `tcp-rr-listener-no-panic` (with `threads>1` and a
pre-bound data port — the `tcp_rr-threads>1-bind-conflict` scenario),
`recorded-traffic-crash-consistency`.

---

## H. `capture-write-fault` — capture IO error is a clean exit, not SIGABRT (NEW)

**Containers:** sink + lading + workload (as A), with a **disk-full / EIO fault
injected on the `capture` volume** on a flush tick.
**Faults:** IO fault on the capture path; finite duration.

Justification: `lading.rs:476/501/526` start the capture manager with
`.block_on(start()).expect(...)` under `panic=abort`, so a flush-tick IO error today
SIGABRTs the whole run mid-flush and skips `format.close()`, leaving an unreadable
parquet. Assert: the panic hook stays silent AND the parquet is footer-complete/readable
(clean fatal exit, not abort).

Proves / guards: `capture-write-failure-not-abort`, `capture-finalize-bounded`,
`multi-format-parquet-not-forfeited`, `capture-histogram-drops-counted`,
`capture-drift-no-silent-gap` (with a >60s scheduling-starvation fault).

---

## I. `split-mode` — two-instance capture merge under partial kill (NEW, P2)

**Containers:** sink + **lading-sender** (`--no-target`) + **lading-receiver**
(`--target-container` observer) + target + workload (runs the oblivious merge).
**Faults:** `node_termination` on exactly ONE lading instance.

Justification: mirrors the deployment's split mode where sender and receiver run on
separate pods and their parquet files are merged obliviously; a truncated/footerless
file from a killed side fails the merge. Assert the clean side's captures are not
needlessly discarded and failure attribution is well-defined (open question from the
digest).

Proves / guards: `split-mode-merge-partial-tolerant`.

---

## Coverage of `scenarios_needed`

| scenarios_needed item | topology |
| --- | --- |
| unreachable-target | C |
| stalled-receiver | D |
| multi-generator-config | A (config seam) |
| small-and-zero-block-size | A (config seam) |
| oversized-block-vs-capacity | A (config seam) + proptest |
| linear-ramp-parallel | A (config seam) + proptest |
| determinism-replay | B |
| node-termination-mid-run | A |
| sigterm-stop | E |
| capture-write-fault | H |
| target-lifecycle-races | F |
| blackhole-restart-stale-socket | G |
| accept-fault-injection | G |
| slow-capture-drain | G / H |
| malformed-config | A (config seam) |
| split-mode-partial-kill | I |
| tcp_rr-threads>1-bind-conflict | G |

Minimality note: A and B share an identical container graph and differ only in fault
profile + duration; C/D/E/H are A's container graph plus one fault or one modified
peer. Only F (adds `target`), G (adds `load-source`, lading-as-blackhole), and I (two
lading instances) introduce genuinely new topology. Build order should follow priority:
E and F are P0 shutdown/no-panic and should land right after the existing A.
