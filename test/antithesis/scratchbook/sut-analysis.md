---
sut_path: /home/ssm-user/src/lading
commit: 51148899
updated: 2026-07-24T21:28:16Z
external_references:
  - name: the deployment (production/local runner + orchestrator)
    why: source of truth for the real shutdown/deploy model (how lading is launched, stopped, and torn down; what "correct shutdown" means operationally; the watchdog/cancel/kill paths that determine capture completeness)
  - name: Jira / Confluence (datadoghq.atlassian.net, SMPTNG/SMP projects)
    why: existing bug tickets and design docs corroborating termination/telemetry-loss classes (SMPTNG-725, -719, -697, -694, -390, -762, -767) and the determinism/DST testing philosophy
  - name: the whole lading repo (this checkout)
    why: primary code source of truth; all line references below are into this tree and must be re-verified against source, not invented
---

# lading SUT analysis (shutdown-safety emphasis)

lading is a deterministic load generator arranged as **generator -> target -> blackhole**, plus a
passive **observer** and a **capture** subsystem. It is engineered around four ADRs: determinism
(same seed => byte-identical load; no wall-clock in payloads; ordered maps not `HashMap`),
no-panic (`Result`, no `unwrap`/`expect` on reachable paths), pre-computation (payloads prebuilt
into a block cache), and performance-first. `panic = "abort"` is set for both release and dev
profiles (`Cargo.toml:115,120`), so **any reachable panic is a whole-process `SIGABRT`** and the
Antithesis panic hook (`antithesis_hooks.rs:27`, `unreachable!("lading panicked")`) reports it
before the abort.

This document maps the architecture, state management, concurrency model, and failure-prone areas
across ALL subsystems, prioritising shutdown/termination safety (P0). It is synthesised from the
property catalog + discovery digest and spot-verified against current `main`. Line numbers are
leads; re-verify against source.

---

## 1. Architecture and process topology

```
                   lading process (tokio multi-thread runtime)
  ┌───────────────────────────────────────────────────────────────────────┐
  │  main()  bin/lading.rs                                                  │
  │   ├─ antithesis_hooks::init()      :741  (SDK init + panic hook)        │
  │   ├─ reachable! "lading completed bootstrap"  :792                      │
  │   ├─ build config, construct block caches (lading_payload)              │
  │   ├─ tokio runtime; inner_main:                                         │
  │   │    ├─ lading_signal broadcast (shutdown) -- register/broadcast      │
  │   │    ├─ Target (spawn Binary  OR  observe Container via docker.sock)  │
  │   │    ├─ Observer  (per-pid procfs/cgroup sampler)                     │
  │   │    ├─ Inspector (optional child process)                            │
  │   │    ├─ Generators[] (Vec)  -- tcp/udp/unix_stream/unix_datagram/     │
  │   │    │                          grpc/http/splunk_hec/file_gen/...     │
  │   │    ├─ Blackholes[] (Vec)  -- tcp/udp/unix/http/datadog/sqs/otlp/... │
  │   │    ├─ Capture manager (jsonl | parquet | multi)                     │
  │   │    └─ experiment_sequence: warmup timer -> experiment timer -> exit │
  │   └─ runtime.shutdown_timeout(max_shutdown_delay=30s)  :813             │
  └───────────────────────────────────────────────────────────────────────┘
```

Key config facts: `config.generator` and the blackhole list are **`Vec`s** (`config.rs:101`) —
multiple generators/blackholes of the same kind are allowed (only duplicate IDs are rejected).
This is the root of several latent defects (per-generator process-global statics).

**Deployment model (the deployment; verify against its source).** Production launches lading as a
container in Container/observer mode (`--target-container target`, docker.sock bind-mounted) — lading
**observes** a sibling target container and does **not** spawn/reap it. lading "owns the clock":
it self-terminates on its own experiment timer; the orchestrator never signals it. Teardown is
`docker rm --force` (immediate SIGKILL). A watchdog of `ceil((warmup + samples + 30) * 1.2)s`
force-ends overruns. **Correct shutdown operationally = lading self-exits 0 within
`max_shutdown_delay` (30s) of experiment end with a footer-complete parquet at
`/captures/captures.parquet`.** Any hard kill before self-exit => footer-less, unreadable parquet =>
total capture loss for that replicate (this is why bounded shutdown is P0). Consequences:
 - Binary-target defects (untimed `target_child.wait()`, `kill_on_drop` orphaning) are **masked on
   the production observer path** but LIVE for Binary-target mode / local dev / bare-host.
 - The SIGTERM/ctrl_c graceful path is effectively dead code in this deployment, but a plain
   `docker stop`/orchestrator-stop on other paths would hit the missing-SIGTERM-handler defect.
 - Jira corroboration: SMPTNG-725 ("RJO alive but not really" — hang-in-spin after "lading
   shutdown"), SMPTNG-719/697 (ungraceful-termination telemetry loss), SMPTNG-694 (captures now
   zstd-compressed => a truncated stream may be undecodable past the last flushed frame — revisit
   the "valid jsonl prefix" assumption against the on-disk format).

---

## 2. State management

- **Config** (`config.rs`) is the single validate-early gate. Gaps live here: numeric fields
  (`compression_level` 1-22 `:186-198`; `sample_period_milliseconds > 0` `:106-107`) are not
  range-checked and only fail at runtime.
- **Block cache** (`lading_payload/block.rs`) is the precomputed, immutable payload state. Built
  once at startup from `(seed, config)`; a `Cache::Fixed` indexes `blocks[idx % len]`. An empty
  cache would panic on modulo, but `InsufficientBlockSizes` (`block.rs:676-680`) prevents an empty
  Fixed cache from being constructed. Determinism holds by construction: `BTreeMap`/`BTreeSet`
  ordering, `FxHashMap` used index-agnostically, rng-derived (not wall-clock) timestamps.
- **Capture accumulator** (`lading_capture/accumulator.rs`) is a per-series ring of the last
  `INTERVALS = 60` ticks. `flush()` emits only tick `current_tick - 60`; the most recent <=60 ticks
  live only in memory and reach disk exclusively via `drain_and_write` at graceful shutdown
  (`state_machine.rs:326-346`). `fetch_index == tick` and `time_ms = start_ms + tick*1000`, giving a
  global bijection and strict per-series monotonicity — this is why a killed jsonl file is a valid
  parseable prefix.
- **Throttle valve state** (`lading_throttle`) holds per-worker capacity; `divide(N)` splits
  capacity across parallel workers (see §5).

---

## 3. Concurrency model (tokio tasks + lading_signal)

- **Runtime.** A multi-thread tokio runtime. Every subsystem is a spawned task or a `JoinSet`.
- **Shutdown fan-out = `lading_signal`.** A broadcast primitive: workers register a
  `shutdown_watcher`, the driver calls `signal_and_wait` to broadcast and await acknowledgement.
  Each worker is expected to poll its watcher inside its main `tokio::select!`. **The central
  fragility class:** workers that block *before* reaching their `select!` (connect/retry loops,
  partial-write loops, `child.wait()`, capture finalize) never observe the broadcast and cannot be
  shut down within the bound. There is a `Lagged` tripwire on the broadcast (`lib.rs:235/268`) worth
  watching.
- **Generators** run as a `JoinSet`; `Server::spin()` blocks on `join_next()`. One un-shuttable
  worker hangs the whole spin, bounded only by `runtime.shutdown_timeout(30s)` — *if that backstop
  is even reached* (see §4 capture-finalize hang, which precedes it).
- **Target/Observer/Inspector** are tasks holding tokio `Child` handles with `kill_on_drop(true)`.
  `kill_on_drop` fires only on normal `Drop`, never on untrapped-signal death.
- **Capture manager** runs on `spawn_blocking` (`.block_on(start())`), receiving samples over a
  bounded `mpsc::channel(10_000)` (`manager.rs:302`). Histogram record uses `try_send` +
  warn-and-drop (`manager.rs:121-133`) — samples silently lost when full.
- **Antithesis coupling.** Antithesis controls scheduling, timing, and faults, and injects
  `node_termination` = **SIGKILL of the whole container** (untrappable; only persisted named-volume
  artifacts survive). It can also perturb the clock (relevant to throttle burst envelope and any
  wall-clock leak). Only two SDK sites live in `lading/src` (panic hook + a bootstrap `reachable!`);
  ALL domain invariants live in `test/antithesis/` (sink byte oracle, capture-consistency checker).

---

## 4. Failure-prone areas — SHUTDOWN / TERMINATION SAFETY (P0)

This is the immediate priority. The umbrella invariant: **once shutdown is signaled (experiment
timer or SIGTERM), lading exits within `max_shutdown_delay` (30s) and never hangs in `Server::spin`
or capture-finalize.** Violation => watchdog SIGKILL => unreadable parquet => total capture loss.

### 4.1 No SIGTERM handler (P0)
`main` select traps only `signal::ctrl_c()` (SIGINT) at `lading.rs:658`; there is **no
`tokio::signal::unix` `SignalKind::terminate` arm**. A plain SIGTERM => default disposition => exit
143 with NO graceful path (no broadcast, no capture finalize, no child reap). *Fix:* add a
`SignalKind::terminate` arm alongside ctrl_c that triggers `shutdown_broadcast`. Corroborated by
SMPTNG-719/697.

### 4.2 Pre-`select` connect/retry loops ignore shutdown (P0/P1)
Each of these loops does `connect -> on error sleep -> continue` and only reaches the shutdown-aware
`select!` after a connection exists. With an unreachable target they spin forever:
 - **tcp** connect branch `tcp.rs:230-251` (sleep 1s) — P0.
 - **unix_datagram** connect loop `unix_datagram.rs:246-264` (sleep 1s) — P0.
 - **grpc** connect loop `grpc.rs:287-299` (sleep 100ms) — P1.
 - **unix_stream** connect loop `unix_stream.rs:248-268` — P0.
*Fix:* wrap the connect attempt in `select!` with `&mut shutdown_wait`, or check shutdown before the
sleep. *Oracle:* unreachable-target scenario + external exit timer.

### 4.3 unix_stream partial-write busy-spin (P0)
Inner `while blk_offset < blk_max` loop (`unix_stream.rs:281-318`) has no shutdown branch and
busy-spins on `yield_now()` when `try_write` returns `WouldBlock` (comment admits "if the read side
has hung up we will never know and will keep attempting to write"). A slow/stalled receiver =>
un-shuttable + 100% CPU. Related: non-`BrokenPipe`/non-`WouldBlock` write errors (e.g.
`ConnectionReset`) neither advance nor reconnect (`:304-315`) — busy-loop spamming `request_failure`;
`packets_sent` counted per partial write. *Fix:* add a shutdown branch and bounded/awaited readiness;
handle non-BrokenPipe errors (advance/break/reconnect).

### 4.4 Container-target discovery loop unbounded (P0 — production path)
`watch_container` (`target.rs:212-244`) polls for the named container with only a found-break +
`sleep(1s)`, no shutdown arm, no max-attempts. A misnamed/never-started target => `target_running`
never signals => `experiment_sequence` blocks at `target_running_watcher.recv()`
(`lading.rs:632-641`) => experiment timer never starts => lading never self-terminates. **This is
the launched production mode**, so high impact. *Fix:* add a `shutdown.recv()` arm and/or
max-attempts timeout.

### 4.5 Untimed `child.wait()` and ~0 target grace (P0/P1 — Binary mode)
 - `target_child.wait()` (`target.rs:432`) and inspector wait (`inspector.rs:176`) are bare
   `.await` — a SIGTERM-ignoring child hangs lading, relying solely on external JoinSet-abort /
   `shutdown_timeout`. *Fix:* wrap in a timeout that escalates to SIGKILL.
 - Target grace period is effectively ~0: `inner_main` returns after capture flush without joining
   `tsrv_joinset`; dropping it aborts the target task mid-`wait()`, and `kill_on_drop` SIGKILLs the
   child within ms of the SIGTERM (`lading.rs:690-717`). The "give the child a chance to clean up"
   comment is not honored. *Confirm intended grace semantics before fixing.*
 - Orphaned children on signal death: `kill_on_drop` can't fire on untrapped-signal death; graceful
   path signals only the direct child pid, not the process group (`target.rs:430-431`,
   `inspector.rs:175-176`) => grandchildren leak. Moot under whole-container SIGKILL; live in
   shared-PID-namespace / bare-host.

### 4.6 Capture-finalize await unbounded (P1)
`let _ = handle.await;` (`lading.rs:713-715`) has no timeout and runs **before**
`runtime.shutdown_timeout` (`:813`). A stalled parquet footer write (slow/full disk) hangs lading
with no backstop, because the 30s timeout is only reached after this await returns. *Fix:* wrap in a
timeout so `shutdown_timeout` remains the backstop.

### 4.7 Non-vacuity anchors
 - `lading-completes-and-exits-cleanly` (Sometimes/P0): at least one timeline reaches clean self-exit
   0 with a footer-complete capture — guards against a regression that makes EVERY timeline hang/abort.
 - Stale comment at `lading.rs:785-787` ("divide by two") is wrong — no divide-by-two exists; the
   full `max_shutdown_delay` goes only to `shutdown_timeout`.

---

## 5. Failure-prone areas — Generators & throttle (rate fidelity, no-panic, livelock)

### 5.1 throttle `divide()` breaks aggregate rate (P0)
`divide(N)` shrinks per-worker capacity to `R/N` but NOT the block a worker draws. A block sized
`R/N < block <= R` is accepted at `parallel_connections=1` but rejected with `Capacity` by every
worker at N>1 => **silent zero delivery** (only a `debug!` log). *Fix:* shrink block sizing
consistently, or validate `maximum_block_size <= bytes_per_second/parallel_connections` upfront.
Pure-proptest-provable against the real Valve (demonstrated on backup branch `0868e39c`, live on main).

### 5.2 Linear throttle ramp compression (P0)
`divide` splits capacities but passes `rate_of_change` unchanged (`lib.rs` Linear arm), so N workers
ramp at aggregate `N*rate` — the ramp reaches max in `1/N` the intended time. *Fix:* divide
`rate_of_change` by the divisor. Provable as a pure proptest measuring aggregate slope.

### 5.3 Capacity-livelock / zero-delivery busy loop (P0)
When block byte size exceeds per-worker capacity, `wait_for` returns `Capacity` immediately (no
wait); tcp/udp/unix discard + loop => hot 100% CPU delivering ~zero bytes (0.31.2 busy-discard
class). *Fix:* validate `maximum_block_size <= bytes_per_second/parallel_connections` post-divide at
construction.

### 5.4 gRPC ignores the throttle result (P0)
`grpc.rs:307-318` does `let _ = result;` and sends regardless => neither honors discards nor the
configured rate (over-delivers). Also serializes requests (RTT-bound). *Fix:* honor the Result
(discard + count), matching tcp/udp.

### 5.5 unix generators don't `.divide()` (P1)
`unix_stream.rs:161-163` and `unix_datagram.rs:186-188` give each worker a full-rate throttle =>
aggregate `N * bytes_per_second` (vs tcp/udp aggregate ~=bps). Cross-generator inconsistency for the
same config key. *Confirm intended per-connection semantics (unix_stream doc says "per connection").*

### 5.6 Integer-division truncation / DivisionByZero (P2)
`bps` not divisible by `N` drops up to `N-1` bytes/interval; `bps < N` returns `DivisionByZero` and
fails to start with a surprising error. *Fix:* distribute remainder; surface a clear validation error.

### 5.7 Observability of under-delivery (P1)
tcp/udp/grpc must count `blocks_discarded` (currently `debug!` only) and bound error-label
cardinality to `io::ErrorKind` (raw `err.to_string()` embeds addresses => unbounded capture-key
growth, ADR-005 OOM class; gRPC tonic error still raw). Both landed on backup branches, live on main.

### 5.8 Generator no-panic sites (P1)
 - Two `Http` (or two `SplunkHec`) generators panic on the second `new()` via a process-global
   `static CONNECTION_SEMAPHORE: OnceCell` double-`set` (`http.rs:37,187-189`;
   `splunk_hec.rs:51,243-245`). Also shares one process-wide limit. *Fix:* per-instance
   `Arc<Semaphore>`; make hot-path `.expect("semaphore closed")` stop the worker gracefully.
 - Splunk-HEC response parse: `serde_json::from_slice::<HecResponse>(...).expect(...)`
   (`splunk_hec.rs:371-374`) panics a **detached, untracked** task on any non-JSON body.
 - Malformed addr/uri: `.expect` at construction (`tcp.rs:154-159`, `udp.rs:164-169`,
   `grpc.rs:226-247`) instead of a `Result` error.
 - http backpressure: `CONNECTION_SEMAPHORE.acquire().await` sits outside the `select!` and there is
   no per-request timeout; shutdown does `acquire_many(all)` => a stalled target hangs shutdown
   (`http.rs:229-297`). Splunk-HEC wraps requests in a 1s timeout and is more resilient.

### 5.9 sink non-vacuity (P1)
`sink/main.rs:82` `sometimes! (total>0)` guards against a whole config class delivering nothing
(divide stall, capacity livelock, throttle bypass).

---

## 6. Failure-prone areas — Payload / determinism (no-panic, termination, reproducibility)

- **Block-cache construction can hang forever (P0):** `construct_block_cache_inner`
  (`block.rs:625-673`) has no time/iteration cap. If `max_block_size < serializer minimum viable
  block`, every attempt is `EmptyBlock`, `bytes_remaining` never decreases, `min_block_size` stays
  `< max_block_size`, so neither exit fires. *Fix:* cap consecutive rejections + time bound => return
  `InsufficientBlockSizes`; reject `max_block_size` below a serializer floor. trace_agent v04 variant
  fixed on backup `456e85a3` (31h hang -> 0.75s); general case live on main.
- **`maximum_block_size == 0` => `random_range(0..0)` panic (P1):** `block.rs:628`; the guard
  `:214-220` misses 0. Reachable via `grpc.rs:205-223` forwarding unvalidated `as_u128()`. *Fix:*
  lower-bound check.
- **Determinism holds (P1 regression guard):** byte output is a pure function of `(seed, config)`;
  `BTreeMap`/`BTreeSet`, index-agnostic `FxHashMap`, rng-derived timestamps
  (`trace_agent/v04.rs:326,384`); `Instant` feeds only progress logging. Property guards against a
  regression introducing wall-clock/entropy (analog to SMPTNG-762 CPU-jitter entropy / SMPTNG-767
  non-monotonic wall clock).
- **No wall-clock in payloads (P2):** trace_agent/otel/templated_json timestamps are seed-derived and
  monotone; must survive a backward clock step under Antithesis clock control.
- **Fuzz `Arbitrary for Block` (P2):** `total_bytes == 0` panics on `NonZeroU32::new().expect`
  (`block.rs:119-127`); only under the `arbitrary` feature. *Fix:* return
  `Err(arbitrary::Error::IncorrectFormat)`.
- **dogstatsd tag_length (P2, merged e98e3052):** `tag_length.end() <= MIN_TAG_LENGTH` now rejected
  upfront through `DogStatsD::new`; property guards regression.

---

## 7. Failure-prone areas — Capture (crash-consistency, durability, no-abort)

- **Capture write error aborts the whole run (P0):** `start()` propagates via `next(event)?`
  (`manager.rs:409`) and the caller does `.block_on(start()).expect(...)` (`lading.rs:476/501/526`)
  under `panic=abort` => a transient flush IO error (disk full/EIO) SIGABRTs the run, skips
  `format.close()`, leaves a footer-less unreadable parquet. *Fix:* propagate to
  `Error::CaptureManager` so BufWriters flush on Drop; plumb mid-run errors to graceful shutdown.
  (Startup fix on backup `b7624af2`; mid-run still a follow-up.)
- **Parquet footer only at graceful close (P0):** `state_machine.rs:249-259` -> `parquet.rs:307-324`.
  With no SIGTERM handler, SIGTERM/SIGKILL skip `close()` => unreadable by design. The graceful-exit
  invariant (`parquet-footer-on-graceful-exit`) depends on §4.1 + capture-write-not-abort.
- **jsonl valid prefix (P1, holds by construction):** the harness checker
  (`anytime_capture_consistent.rs:44,49`) asserts `torn_before_final==0` and monotonic
  fetch_index/time on any truncated prefix; `MIN_RECORDS=10` non-vacuity floor. NOTE SMPTNG-694:
  captures are now zstd-compressed — re-verify a truncated stream is still a valid prefix.
- **60s maturity loss + no fsync (P2):** the last <=60 ticks live only in memory until graceful
  drain; no `sync_all`/`sync_data` anywhere in `lading_capture` (`jsonl.rs:53` BufWriter flush only)
  => flushed-but-unsynced lines can vanish on whole-VM kill if page cache isn't preserved to the
  named volume. Open question: does node_termination preserve page cache?
- **multi close order forfeits parquet (P1):** `multi.rs:69` `jsonl.close()?` runs before `:71`
  `parquet.close()?`; a trivial jsonl error skips the important parquet footer. *Fix:* finalize
  parquet first / best-effort both.
- **Drift correction silent gaps (P2):** `state_machine.rs:219-232` advances multiple ticks without
  interleaved flush; `advance_tick` overwrites unflushed ring slots (`accumulator.rs:466-481`) =>
  invisible fetch_index gaps under >60s starvation (passes the strict-monotonic validator).
- **Histogram drops silent (P1):** full channel => `try_send` warn-and-drop (`manager.rs:121-133`);
  add a bounded-label `capture_histogram_samples_dropped` counter in the registry (backup `39d8ae56`).
- **Capture no-panic sites (P0 via abort):** `accumulator.rs:527,231`, `manager.rs:236`, repeated
  `state_machine.rs:315/342/395/411/428` `.expect("format must be present...")` — each aborts the run.
- **Split-mode merge (P2, deployment lead):** the oblivious merge tolerates empty but not a
  truncated/footerless parquet — a corrupt side errors the whole replicate. Confirm a clean side's
  captures aren't needlessly lost when only one side overruns.

---

## 8. Failure-prone areas — Observer (no-panic under target races)

`panic=abort` makes each of these a hard SIGABRT (all live on main; fixes only on backup):
- **PID reuse assert (P0):** `stat.rs:82` `assert!(cur_pid == pid)` fires on a recycled/mismatched
  PID. *Fix:* skip-and-continue (backup `6aa1b1ba`).
- **Process vanish mid-listing (P0):** `process_descendents.rs:13` `panic!` when `Process::new`
  fails. *Fix:* yield empty iterator (backup `7e1d2968`).
- **Malformed cpu.max (P0):** unchecked index / zero-period divide in `parse_allowed_cores`. *Fix:*
  bounds-check + guard (backup `6aa1b1ba`).
- **Target PID never arrives (P0):** `observer.rs:114-120` `.expect("catastrophic failure")` on
  `recv()` `Err(Closed)` when a Binary target fails to spawn / instant-exits (`target.rs:395-396`).
  Only partly masked as a join-error log. *Fix:* return an error.
- **Transient read kills the run (P1):** a single procfs/cgroup/wss read error `?`-propagates and
  terminates the experiment. *Fix:* best-effort log+skip (backup `30b86a71`).
- **PID-reuse impostor metrics (P2):** `kill(pid,0)` check then `AsyncPidFd::from_pid` is a TOCTOU;
  a recycled PID silently yields plausible-but-wrong metrics. *Fix:* capture + validate a
  proc start_time identity fingerprint (`target.rs:302-332`).
- **cgroup memory limit (P2, merged 1085887c):** `get_available_memory` now walks the v2 ancestor
  chain returning the tightest `memory.max`; property guards regression via the injectable reader.

---

## 9. Failure-prone areas — Blackholes & config (never-backpressure, no-panic, amplification)

- **unix_datagram stale-socket bind fail (P1):** `unix_datagram.rs:95`
  `let _res = tokio::fs::remove_file(...).map_err(...)` builds a lazy `MapErr` future that is
  **never awaited** — the stale socket is never removed, so bind fails `EADDRINUSE` after a
  hard-kill restart. *Fix:* `.await` the remove. (Impact depends on whether the socket path is on a
  persisted volume.)
- **datadog accept-loop wedge (P1):** `datadog.rs:207-212` uses a fallible `Ok((stream,_)) =
  listener.accept()` select arm with no `else` — on an accept `Err` (EMFILE/ECONNABORTED under fault)
  the branch is disabled and select blocks on shutdown forever; the loop stops accepting while
  appearing alive => backpressures the target. *Fix:* `match` + continue, like `common.rs:89-96`.
- **datadog capture-channel backpressure (P1):** `handle_v2_protobuf` awaits a bounded
  `send().await` per metric point (`:398,412,416`) before building the response (`:296-299`) — a slow
  capture drain stalls the target. *Fix:* `try_send` + count-on-drop.
- **tcp_rr bind panic (P1):** `create_listener` uses `expect`/`panic!` (`tcp_rr.rs:345-346`), and the
  `threads>1` thread-0 prebuild runs in the async `run()` (`:179`) — a pre-bound data port SIGABRTs
  the main task. *Fix:* return `Error::Bind`. (Confirm threads>1 is used.)
- **SQS amplification (P2):** `max_number_of_messages` (u32, target-controlled) drives an unbounded
  `0..num_messages` allocation loop (`sqs.rs:257-267,362-370`); real SQS caps at 10. *Fix:* clamp.
- **Config validation gaps (P2):** `compression_level` 1-22 and `sample_period_milliseconds > 0`
  unchecked at load (`config.rs:186-198,106-107`).
- **Accept-error inconsistency (systemic):** `common.rs` (http/sqs/splunk_hec/otlp) log+continue
  (resilient); tcp/udp/unix_stream propagate+die (target loses sink); datadog wedges (worst). Worth a
  single policy decision.
- **Recorded-traffic crash-consistency (P2, lead):** blackhole traffic recorder (SMPTNG-390, #1911,
  #1895) — verify zstd framing so a SIGKILL-truncated stream is a valid decodable prefix.

---

## 10. Cross-cutting: no-panic umbrella + non-instrumented paths

- **`no-panic-anywhere` (P0):** the panic hook (`antithesis_hooks.rs:27`) is the oracle for EVERY
  panic site above; `panic=abort` makes each hit a hard observable abort. The individual panic-site
  properties (§5.8, §6, §8, §9) are the ADR-compliant `Result` conversions needed.
- **No SDK assertion instruments the shutdown path inside `lading/src`** — the P0 shutdown
  invariants are currently observable only indirectly (panic hook + external capture-consistency
  checker + sink oracle). Adding external exit-timer oracles (shutdown-signal -> exit <= 30s) is the
  highest-leverage harness gap.
- Three std `unreachable!` sites (`lading.rs:347`, `splunk_hec/acknowledgements.rs:110`,
  `otlp/http.rs:227,258`) are guards, not SDK assertions, but are still subject to the no-panic ADR.

---

## 11. Priority scenario map (what to drive under Antithesis)

| Scenario | Drives | Priority |
|---|---|---|
| unreachable-target (addr/socket/uri/container never binds) | §4.2, §4.4 connect-loop hangs; exit <= 30s | P0 |
| stalled-receiver (accepts, never reads) | §4.3 unix_stream spin; §5.8 http backpressure | P0 |
| multi-generator config (two Http / two SplunkHec) | §5.8 OnceCell double-set panic | P1 |
| small-and-zero block size | §6 block-cache hang / `random_range` panic | P0 |
| oversized-block-vs-capacity (N>1) | §5.1/§5.3 divide stall + livelock; blocks_discarded>0 | P0 |
| linear-ramp-parallel | §5.2 aggregate slope == rate_of_change | P0 |
| determinism-replay (+perturbed clock) | §6 byte-identical + monotone timestamps | P1 |
| node-termination mid-run (SIGKILL) | §7 jsonl prefix valid; readable parquet consistent | P0 |
| sigterm-stop | §4.1 graceful drain, footer written, no orphans | P0 |
| capture-write-fault (disk full/EIO) | §7 clean fatal exit, footer written, no SIGABRT | P0 |
| target-lifecycle-races (exit-before-PID, vanish, PID reuse) | §8 observer degrades, no impostor metrics | P0 |
| blackhole-restart-stale-socket | §9 unix_datagram bind succeeds | P1 |
| accept-fault-injection (fd exhaustion) | §9 datadog keeps serving | P1 |
| slow-capture-drain | §4.6 finalize bound; §9 datadog response latency bound | P1 |
| malformed-config (addr/uri/level/period/tag_length) | clean startup errors, no panic | P1 |
| split-mode-partial-kill | §7 merge/attribution well-defined | P2 |
| tcp_rr threads>1 bind conflict | §9 Error::Bind not panic | P1 |

---

## 12. Standing open questions (validate against source / the deployment)

1. Does the deployment ever deliver SIGTERM, or only whole-container SIGKILL? (Determines whether §4.1
   is exercised outside an explicit `kill -TERM`.)
2. Does the parquet writer finalize a readable footer on `flush_seconds:60`, or only at graceful
   close? If only at close, every non-graceful stop loses ALL data, not just the last 60s.
3. Are captures now zstd-compressed (SMPTNG-694) such that a truncated stream is undecodable past the
   last frame — invalidating the "valid jsonl prefix" assumption?
4. Does `node_termination` preserve OS page cache to the persisted named volume (§7 no-fsync — P2 vs P1)?
5. Are the unix socket paths on a persisted named volume (§9 stale-socket impact)?
6. Is the undivided per-connection throttle intentional for unix_stream (§5.5)?
7. Is the ~0 target grace period intentional, or should `max_shutdown_delay` gate it (§4.5)?
8. Is `tcp_rr` ever configured with `threads>1` in the harness/deployment (§9)?

> All backup/blt-branch hardening fixes referenced above (divide stall, linear ramp, grpc throttle,
> observer panic trio, logrotate stale-tick, per-generator semaphore, capture abort/histogram drops,
> silent discards, unbounded labels) are **NOT on `main`** — they are live regressions here.
