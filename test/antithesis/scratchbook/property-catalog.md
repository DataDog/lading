---
sut_path: /home/ssm-user/src/lading
commit: 51148899
updated: 2026-07-24T21:28:18Z
external_references:
  - name: the deployment (production runner + local dev orchestrator)
    why: real shutdown/deploy model — lading "owns the clock", is torn down by SIGKILL (docker rm --force), watchdog=(warmup+samples+30)*1.2, parquet-only captures, container target-observer mode; defines the operational meaning of "correct shutdown"
  - name: Jira (project SMPTNG/SMP) + Confluence (datadoghq.atlassian.net)
    why: existing bug tickets and design docs — SMPTNG-725 hang-in-spin, SMPTNG-719/697 ungraceful-termination telemetry loss, SMPTNG-694 zstd captures, SMPTNG-390 traffic recorder, SMPTNG-762/767 entropy/wall-clock SUT-analog classes
  - name: the whole lading repo (this checkout)
    why: source of truth for every property — spot-verified against current main; hardening fixes referenced in git history live only on backup/blt branches, NOT main
---

# lading bug-hunting property catalog

Every property below is a **checkable invariant whose violation is a real lading defect**. The
oracle is either the existing panic hook (`antithesis_hooks.rs:27` reports `unreachable! "lading
panicked"`, then `panic=abort` SIGABRTs the container), an external harness checker, or a
feature-gated SUT-side `assert_always!`/`reachable!`. Prioritize where Antithesis is strongest:
timing, concurrency, partial-failure, no-panic, determinism.

**SHUTDOWN / TERMINATION SAFETY is the immediate priority (P0).** The full system is catalogued
below it.

> Regression note: the hardening fixes referenced in git history live only on `backup/20260720/*`
> and `blt/*` branches, **NOT on main**. So the divide stall, linear-ramp compression, gRPC
> throttle bypass, observer PID/vanish/cpu.max/transient panics, logrotate stale-tick panic,
> per-generator semaphore panic, capture abort, silent discards, and unbounded error labels are
> **live regressions in main** — not merely history.

Assertion types: **Unreachable** for specific panic/abort sites and validation guards;
**Always** for bounded-latency / rate-fidelity / crash-consistency / observability invariants that
must hold on every timeline; **Sometimes** for non-vacuity liveness.

---

## Overview by subsystem and priority

### lifecycle / shutdown (P0 first — immediate priority)

| Priority | Property | Assertion | Needs target | One-line invariant |
|---|---|---|---|---|
| P0 | `sigterm-graceful-drain` | Always | no | SIGTERM runs the same graceful drain/finalize path as the experiment timer |
| P0 | `shutdown-completes-bounded` | Always | yes | lading exits within `max_shutdown_delay` once shutdown is signaled |
| P0 | `tcp-connect-loop-shutdown-responsive` | Always | yes | TCP worker observes shutdown while (re)connecting to an unreachable target |
| P0 | `unix-stream-partial-write-shutdown` | Always | yes | unix_stream partial-write loop is shutdown-aware, not a busy-spin |
| P0 | `unix-datagram-connect-loop-shutdown` | Always | yes | unix_datagram connect loop observes shutdown |
| P0 | `target-wait-bounded` | Always | yes | post-SIGTERM target/inspector wait is time-bounded (SIGKILL escalation) |
| P0 | `docker-target-discovery-bounded` | Always | yes | container-target discovery is bounded + shutdown-aware (production observer path) |
| P0 | `lading-completes-and-exits-cleanly` | Sometimes | no | happy-path self-termination reaches exit 0 with readable capture (non-vacuity) |
| P1 | `grpc-connect-loop-shutdown` | Always | yes | gRPC connect loop observes shutdown |
| P1 | `capture-finalize-bounded` | Always | no | capture-finalize await is time-bounded |
| P1 | `orphaned-children-on-signal-death` | Always | yes | no orphaned target/inspector children on signal-driven death |
| P1 | `target-grace-period-honored` | Always | yes | cooperative slow target gets its full post-SIGTERM cleanup window |

### capture (lading_capture)

| Priority | Property | Assertion | Needs target | One-line invariant |
|---|---|---|---|---|
| P0 | `capture-write-failure-not-abort` | Unreachable | no | a capture write error is a clean fatal exit, not a `panic=abort` SIGABRT |
| P0 | `parquet-footer-on-graceful-exit` | Always | no | every graceful exit yields a readable footer-complete parquet |
| P1 | `jsonl-prefix-valid-after-kill` | Always | no | a SIGKILLed jsonl capture is always a valid parseable prefix |
| P1 | `multi-format-parquet-not-forfeited` | Always | no | multi mode finalizes parquet even if jsonl close errors |
| P1 | `capture-histogram-drops-counted` | Always | no | dropped histogram samples are counted, not silently lost |
| P2 | `capture-no-fsync-durability` | Always | no | flushed capture lines survive whole-VM termination |
| P2 | `capture-drift-no-silent-gap` | Always | no | drift correction does not silently drop unflushed intervals |
| P2 | `split-mode-merge-partial-tolerant` | Always | yes | split-mode merge tolerates one clean side when the other overruns |

### payload / determinism (lading_payload)

| Priority | Property | Assertion | Needs target | One-line invariant |
|---|---|---|---|---|
| P0 | `block-cache-construction-terminates` | Always | no | block-cache construction always terminates in bounded time |
| P0 | `trace-agent-v04-block-terminates` | Always | no | trace-agent v04 `next_block` terminates fast; no 1-byte empty block |
| P1 | `payload-determinism-byte-identical` | Always | no | same seed + config yields byte-identical load |
| P2 | `no-wall-clock-in-payloads` | Always | no | payload timestamps are seed-derived and monotonic, never wall-clock |
| P2 | `dogstatsd-tag-length-validated` | Always | no | `tag_length.end() <= MIN_TAG_LENGTH` is rejected upfront (merged to main) |

### generators / throttle

| Priority | Property | Assertion | Needs target | One-line invariant |
|---|---|---|---|---|
| P0 | `throttle-divide-no-silent-underdelivery` | Always | yes | a config delivering at N=1 still delivers at N>1 |
| P0 | `linear-ramp-slope-preserved` | Always | yes | Linear aggregate ramp slope == configured `rate_of_change`, not N× |
| P0 | `throttle-capacity-no-zerodelivery-livelock` | Always | yes | oversized block never becomes a zero-delivery busy loop |
| P0 | `grpc-honors-throttle` | Always | yes | gRPC honors throttle rejections and configured rate |
| P1 | `stable-burst-envelope-bounded` | Always | no | stable throttle never exceeds its per-interval burst envelope |
| P1 | `unix-throttle-aggregate-consistent` | Always | yes | unix_stream/unix_datagram aggregate rate matches configured bps |
| P1 | `discarded-blocks-counted` | Always | yes | under-delivery is observable via a `blocks_discarded` counter |
| P1 | `error-label-cardinality-bounded` | Always | yes | generator error-metric label cardinality is bounded (io::ErrorKind) |
| P1 | `sink-receives-bytes` | Sometimes | yes | the sink receives nonzero load (delivery non-vacuity) |
| P2 | `divide-by-zero-startup-error` | Always | yes | `bps < parallel_connections` fails clearly, no DivisionByZero surprise |
| P2 | `unix-stream-write-error-progress` | Always | yes | unix_stream makes progress on non-BrokenPipe write errors |

### observer

| Priority | Property | Assertion | Needs target | One-line invariant |
|---|---|---|---|---|
| P0 | `observer-pid-reuse-no-panic` | Unreachable | yes | never aborts on PID reuse / mismatch (`assert!(cur_pid==pid)`) |
| P0 | `observer-process-vanish-no-panic` | Unreachable | yes | never panics when a target vanishes mid-listing |
| P0 | `observer-cpu-max-parse-no-panic` | Unreachable | yes | never panics on malformed/truncated `cpu.max` |
| P1 | `observer-transient-read-not-fatal` | Always | yes | a transient procfs/cgroup/wss read error does not kill the run |
| P2 | `observer-pid-identity-fingerprint` | Always | yes | samples the identified target, not a PID-reuse impostor |
| P2 | `get-available-memory-cgroup-chain` | Always | no | memory limit reflects tightest cgroup v2 ancestor (merged to main) |

### no-panic (umbrella + specific sites)

| Priority | Property | Assertion | Needs target | One-line invariant |
|---|---|---|---|---|
| P0 | `no-panic-anywhere` | Unreachable | no | lading never panics anywhere (panic hook = `Unreachable`) |
| P0 | `observer-target-pid-recv-no-panic` | Unreachable | yes | observer returns error, not `.expect`, when target PID never arrives |
| P1 | `block-cache-zero-max-no-panic` | Unreachable | no | `maximum_block_size` of 0 is rejected, not a `random_range` panic |
| P1 | `per-generator-semaphore-no-panic` | Unreachable | yes | two HTTP (or two Splunk-HEC) generators coexist without OnceCell panic |
| P1 | `splunk-hec-response-parse-no-panic` | Unreachable | yes | Splunk-HEC response parse never panics on a non-HecResponse body |
| P1 | `generator-addr-uri-validation-no-panic` | Unreachable | no | malformed addr/target_uri is a Result error, not a construction panic |
| P1 | `tcp-rr-listener-no-panic` | Unreachable | no | tcp_rr blackhole returns Error::Bind, not a panic, on bind failure |
| P1 | `logrotate-stale-tick-noop` | Unreachable | no | logrotate_fs treats a stale tick as a no-op, never a panic |
| P2 | `arbitrary-block-nonzero-no-panic` | Unreachable | no | fuzz Arbitrary Block handles `total_bytes==0` without `.expect` panic |

### blackhole / config

| Priority | Property | Assertion | Needs target | One-line invariant |
|---|---|---|---|---|
| P1 | `unix-datagram-blackhole-removes-stale-socket` | Always | yes | blackhole removes a stale socket before bind (currently lazy future) |
| P1 | `datadog-blackhole-accept-resilient` | Always | yes | Datadog blackhole keeps accepting after a transient accept error |
| P1 | `blackhole-never-backpressures-target` | Always | yes | blackhole responds regardless of capture-channel saturation |
| P2 | `sqs-receive-message-bounded` | Always | no | SQS blackhole bounds `ReceiveMessage` response size |
| P2 | `config-numeric-fields-validated` | Always | no | numeric config fields validated at load, not at runtime failure |
| P2 | `recorded-traffic-crash-consistency` | Always | yes | blackhole-recorded traffic files are crash-consistent |

---

# Properties — shutdown / termination safety (P0 FIRST)

## sigterm-graceful-drain — SIGTERM finalizes capture like the experiment timer
- **Subsystem:** lifecycle/shutdown · **Priority:** P0 · **Assertion:** Always · **Needs target:** no
- **Statement:** When lading receives SIGTERM it runs the same graceful path as experiment-timer
  self-termination: broadcast shutdown, drain the capture maturity window, write the parquet/multi
  footer, exit non-abnormally.
- **Observable:** After a SIGTERM the on-disk parquet/multi capture is readable (footer present) and
  lading exits 143/0, not abort. MOOT under Antithesis `node_termination` (SIGKILL is untrappable)
  but LIVE on the deployment's orchestrator-stop path.
- **Mechanism:** Always invariant on the graceful contract. Best oracle is external: send SIGTERM
  mid-run, then validate the capture file is footer-complete and the process exited without abort.
  Lead confirmed by deployment tickets SMPTNG-719/697 (ungraceful-termination telemetry loss).
- **Needs SUT fix:** Add a `tokio::signal::unix` `SignalKind::terminate` arm in the `lading.rs` main
  `select!` (alongside `ctrl_c` at :658) that triggers `shutdown_broadcast`.
- **Evidence:** `lading.rs:658` only `ctrl_c`; no `SignalKind::terminate` per grep. Digest lifecycle finding 1.
- **Open questions:** Does Antithesis ever deliver SIGTERM (vs only SIGKILL `node_termination`)? If
  not, this is exercised only by an explicit harness `kill -TERM` step.

## shutdown-completes-bounded — lading exits within max_shutdown_delay after shutdown is signaled
- **Subsystem:** lifecycle/shutdown · **Priority:** P0 · **Assertion:** Always · **Needs target:** yes
- **Statement:** Once the experiment timer fires (or a shutdown signal is broadcast) lading
  terminates within `max_shutdown_delay`; it never hangs in `Server::spin` waiting on a worker that
  cannot observe shutdown.
- **Observable:** Wall time from shutdown-broadcast to process exit `<= max_shutdown_delay` (30s).
  Violation = overrun → deployment watchdog SIGKILL → unreadable parquet → total capture loss for
  the replicate.
- **Mechanism:** Umbrella liveness for shutdown hangs. Oracle: external timer measuring
  shutdown-signal → exit, or Antithesis liveness "eventually exits". Deployment-confirmed lead:
  SMPTNG-725 "RJO alive but not really" hang-in-spin after "lading shutdown".
- **Needs SUT fix:** Add shutdown branches to every pre-select connect/retry loop
  (tcp/udp/unix_stream/unix_datagram/grpc) and a timeout to `target_child.wait()` and the
  capture-finalize await; see per-loop properties.
- **Evidence:** Digest generators findings 1–4; lifecycle findings 2,6,7.

## tcp-connect-loop-shutdown-responsive — TCP worker observes shutdown while (re)connecting
- **Subsystem:** lifecycle/shutdown · **Priority:** P0 · **Assertion:** Always · **Needs target:** yes
- **Statement:** The TCP generator worker responds to the shutdown signal even when the target is
  unreachable and it is stuck in the connect/retry loop.
- **Observable:** With an unreachable target, after the experiment timer fires lading still exits
  within `max_shutdown_delay`. Confirmed live: the connect branch (`tcp.rs:230-251`) does
  `connect → sleep(1s) → continue` and only the post-connection `select!` polls `shutdown_wait`.
- **Mechanism:** Bounded shutdown. Oracle external (timer to exit) in an unreachable-target
  scenario; optionally a SUT `reachable!` at the loop's shutdown exit to prove the branch is taken.
- **Needs SUT fix:** Wrap the connect attempt in `tokio::select!` with `&mut shutdown_wait`, or check
  shutdown before the `sleep(1s)`, in the `tcp.rs` connect branch.
- **Evidence:** `tcp.rs:230-283` (connect before select, shutdown only inside select).

## unix-stream-partial-write-shutdown — unix_stream partial-write loop is shutdown-aware and not a busy-spin
- **Subsystem:** lifecycle/shutdown · **Priority:** P0 · **Assertion:** Always · **Needs target:** yes
- **Statement:** When the receiver's socket buffer is full, the unix_stream inner partial-write loop
  neither busy-spins on `WouldBlock` at 100% CPU nor ignores shutdown; it yields to shutdown and
  bounds its retry.
- **Observable:** With a stalled/slow unix receiver, lading exits within `max_shutdown_delay` and
  does not peg a core. Code comment admits "if the read side has hung up we will never know and will
  keep attempting to write."
- **Mechanism:** Bounded shutdown + no livelock. Oracle: slow-receiver scenario + external exit timer
  and CPU observation.
- **Needs SUT fix:** Add a shutdown branch to the `while blk_offset < blk_max` loop
  (`unix_stream.rs:281-318`) and to the connect loop (`:248-268`); replace the bare `yield_now` spin
  with a bounded/awaited readiness.
- **Evidence:** `unix_stream.rs:281-318` inner loop, `yield_now` at `:302`; connect loop `:248-268`.

## unix-datagram-connect-loop-shutdown — unix_datagram connect loop observes shutdown
- **Subsystem:** lifecycle/shutdown · **Priority:** P0 · **Assertion:** Always · **Needs target:** yes
- **Statement:** The unix_datagram initial connect loop polls shutdown; if the socket path never
  appears the worker still shuts down within `max_shutdown_delay`.
- **Observable:** Missing-socket-path scenario: lading exits within bound rather than spinning
  `connect → sleep(1s)` forever.
- **Mechanism:** Always. External exit-timer oracle in a never-bound-socket scenario.
- **Needs SUT fix:** Add a shutdown branch to the `unix_datagram.rs:246-264` connect/retry loop.
- **Evidence:** `unix_datagram.rs:246-264`.

## target-wait-bounded — post-SIGTERM target/inspector wait is time-bounded
- **Subsystem:** lifecycle/shutdown · **Priority:** P0 · **Assertion:** Always · **Needs target:** yes
- **Statement:** After lading SIGTERMs a Binary target (or inspector), the `wait()` for the child to
  exit is bounded (does not depend solely on external JoinSet-abort/runtime timeout); a
  SIGTERM-ignoring target does not hang lading.
- **Observable:** With a target that ignores SIGTERM, lading still exits within `max_shutdown_delay`.
  MOOT on the deployment's container-observer path (target is a sibling container force-killed by the
  runner), LIVE for Binary-target mode.
- **Mechanism:** Locally-bounded reap. Oracle: SIGTERM-ignoring-target scenario + exit timer.
- **Needs SUT fix:** Wrap `target_child.wait()` (`target.rs:432`) and inspector wait
  (`inspector.rs:176`) in a timeout that escalates to SIGKILL before returning.
- **Evidence:** `target.rs:432`, `inspector.rs:176` bare `.await`.

## docker-target-discovery-bounded — container-target discovery is bounded and shutdown-aware
- **Subsystem:** lifecycle/shutdown · **Priority:** P0 · **Assertion:** Always · **Needs target:** yes
- **Statement:** In container/observer mode, the target-container discovery poll loop either finds
  the container, times out with an error, or responds to shutdown; it never spins forever so the
  experiment timer can start.
- **Observable:** With a misnamed/never-started target container, lading either errors out bounded or
  self-terminates on its timer; it does not block `experiment_sequence` at
  `target_running_watcher.recv()` forever. This is the production observer path, so high impact.
- **Mechanism:** Bounded startup. Oracle: wrong `--target-container` name scenario; assert lading
  exits (error or timer) within the watchdog. Strong deployment lead (this is the launched production
  mode).
- **Needs SUT fix:** Add a `shutdown.recv()` arm and/or a max-attempts timeout to the
  `target.rs:212-244` `watch_container` loop.
- **Evidence:** `target.rs:212-244` (only found-break + `sleep(1s)`, no shutdown/timeout);
  `lading.rs:632-641` experiment timer gated on `target_running`.

## lading-completes-and-exits-cleanly — lading self-terminates on its experiment timer and exits 0 (non-vacuity)
- **Subsystem:** lifecycle/shutdown · **Priority:** P0 · **Assertion:** Sometimes · **Needs target:** no
- **Statement:** On the happy path lading owns the clock: it reaches experiment end, drains capture,
  and exits 0 with a readable capture at least once.
- **Observable:** A run reaches clean self-termination with exit 0 and a footer-complete capture.
  `reachable!` anchor for shutdown coverage.
- **Mechanism:** Sometimes (liveness non-vacuity): the good shutdown path must be reachable on at
  least one timeline, guarding against a regression that makes every timeline hang/abort. Oracle:
  external exit-code + capture-readable check; SUT `reachable!` at clean exit.
- **Needs SUT fix:** none.
- **Evidence:** `lading.rs` `experiment_sequence` + graceful path.

## grpc-connect-loop-shutdown — gRPC connect loop observes shutdown
- **Subsystem:** lifecycle/shutdown · **Priority:** P1 · **Assertion:** Always · **Needs target:** yes
- **Statement:** The gRPC generator's initial connect loop polls shutdown; a never-available target
  does not wedge the generator.
- **Observable:** Never-up target: lading exits within bound rather than spinning
  `connect → sleep(100ms)`.
- **Mechanism:** Always. External exit-timer oracle.
- **Needs SUT fix:** Add a shutdown branch to the `grpc.rs:287-299` connect loop.
- **Evidence:** `grpc.rs:287-299`.

## capture-finalize-bounded — capture finalize await is time-bounded
- **Subsystem:** lifecycle/shutdown · **Priority:** P1 · **Assertion:** Always · **Needs target:** no
- **Statement:** The capture-manager join await during shutdown is bounded; a stalled parquet footer
  write (slow volume, disk-full) cannot make lading hang before `runtime.shutdown_timeout` can act.
- **Observable:** With a slow/stalled capture volume, lading still exits within `max_shutdown_delay`.
- **Mechanism:** Always. Oracle: slow-disk fault scenario + exit timer.
- **Needs SUT fix:** Add a timeout around `let _ = handle.await;` (`lading.rs:713-715`) so
  `runtime.shutdown_timeout` remains the backstop.
- **Evidence:** `lading.rs:713-715` unbounded await before `shutdown_timeout` at `:813`.

## orphaned-children-on-signal-death — no orphaned target/inspector children on signal-driven death
- **Subsystem:** lifecycle/shutdown · **Priority:** P1 · **Assertion:** Always · **Needs target:** yes
- **Statement:** lading reaps its Binary-target and inspector children (and their process groups)
  even when killed by a signal, rather than relying solely on `kill_on_drop` which cannot fire on
  untrapped-signal death.
- **Observable:** After a SIGTERM (or crash) no orphaned target/inspector grandchildren remain
  reparented to init. MOOT under Antithesis whole-container SIGKILL (all pids die together); LIVE in
  shared-PID-namespace / bare-host deployments.
- **Mechanism:** No-leak. Oracle: SIGTERM scenario in a shared PID namespace checking for surviving
  children.
- **Needs SUT fix:** Install a SIGTERM handler (see `sigterm-graceful-drain`) and send signals to the
  process group, not just the direct child pid (`target.rs:430-431`, `inspector.rs:175-176`).
- **Evidence:** `target.rs:392,430-431`; `inspector.rs:146,175-176`.

## target-grace-period-honored — a cooperative slow target gets its full post-SIGTERM cleanup window
- **Subsystem:** lifecycle/shutdown · **Priority:** P1 · **Assertion:** Always · **Needs target:** yes
- **Statement:** On graceful shutdown the target receives SIGTERM and is given up to
  `max_shutdown_delay` to clean up/flush before SIGKILL, rather than being SIGKILLed within
  milliseconds when capture flush returns.
- **Observable:** A cooperative-but-slow target completes its cleanup/artifact flush before being
  killed. Currently `inner_main` returns after capture flush, dropping `tsrv_joinset` and aborting
  `target_child.wait` → `kill_on_drop` SIGKILL.
- **Mechanism:** Grace contract. Oracle: SIGTERM-then-slow-cleanup target scenario checking the
  target's artifacts are complete.
- **Needs SUT fix:** Join the target task with a `max_shutdown_delay`-bounded wait after signaling,
  instead of dropping the JoinSet immediately (`lading.rs:690-717`).
- **Evidence:** `lading.rs:690-717`; `target.rs:392,425-433`.
- **Open questions:** Is the ~0 grace intentional, or is `max_shutdown_delay` meant to gate the target
  cleanup window?

---

# Properties — capture (lading_capture)

## capture-write-failure-not-abort — a capture write error is a clean fatal exit, not a process abort
- **Subsystem:** capture · **Priority:** P0 · **Assertion:** Unreachable · **Needs target:** no
- **Statement:** A transient capture write/flush error (disk full, EIO) causes a clean non-zero exit
  with the parquet footer flushed, not a `panic=abort` that SIGABRTs the whole run mid-flush and
  leaves an unreadable file.
- **Observable:** On an injected capture-write IO error the panic hook must NOT fire and the parquet
  must be readable. Currently `.block_on(start()).expect(...)` + `panic=abort` turns any flush-tick
  error into a SIGABRT that skips `format.close()`.
- **Mechanism:** Unreachable on the abort site. Oracle: IO-fault-injection scenario + panic hook +
  external parquet-readability check. Partly landed on backup branch (`b7624af2`), not on main.
- **Needs SUT fix:** Replace the `.expect` on capture start (`lading.rs:476/501/526`) with Result
  propagation to `Error::CaptureManager` so BufWriters flush on Drop; plumb mid-run write errors to
  graceful shutdown.
- **Evidence:** `lading.rs:476/501/526` `.expect`; `manager.rs:409` `next(event)?`; `Cargo.toml`
  `panic=abort`.

## parquet-footer-on-graceful-exit — graceful exit always yields a readable parquet capture
- **Subsystem:** capture · **Priority:** P0 · **Assertion:** Always · **Needs target:** no
- **Statement:** After any graceful termination (experiment timer or SIGTERM) the parquet/multi
  capture file has a finalized footer and is fully readable.
- **Observable:** An external reader opens `/captures/captures.parquet` post-exit and parses all row
  groups. This is the operational invariant the deployment depends on
  (watchdog/cancel/kill → unreadable → capture loss).
- **Mechanism:** Always. Oracle is the existing `anytime_capture_consistent` checker's parquet arm
  (readable ⇒ internally consistent) plus a post-graceful-exit readability assertion.
- **Needs SUT fix:** none on the pure graceful path; depends on `sigterm-graceful-drain` and
  `capture-write-failure-not-abort` holding.
- **Evidence:** `parquet.rs:307-324` footer only in `close()`; `state_machine.rs:249-259` close only
  on `ShutdownSignaled`.

## jsonl-prefix-valid-after-kill — a SIGKILLed jsonl capture is always a valid parseable prefix
- **Subsystem:** capture · **Priority:** P1 · **Assertion:** Always · **Needs target:** no
- **Statement:** Under abrupt kill (`node_termination`/SIGKILL) any surviving jsonl capture parses as
  a valid prefix with no torn final record and strictly-increasing per-series `fetch_index`/`time`.
- **Observable:** `anytime_capture_consistent.rs` `always!(torn_before_final==0)` and
  `always!(invariants_hold)`. Loss of the last `<=60s` maturity window is tolerated by design; a
  torn/reordered record is a defect.
- **Mechanism:** Always. Oracle already implemented (harness checker, `MIN_RECORDS=10` non-vacuity
  floor).
- **Needs SUT fix:** none (holds by construction); property guards against regressions in the
  accumulator flush ordering.
- **Evidence:** `anytime_capture_consistent.rs:44,49`; `accumulator.rs:492-496` monotonic flush.

## multi-format-parquet-not-forfeited — multi format finalizes parquet even if jsonl close errors
- **Subsystem:** capture · **Priority:** P1 · **Assertion:** Always · **Needs target:** no
- **Statement:** In multi capture mode a trivial jsonl flush/close error does not skip the parquet
  footer write; the important format is never sacrificed to the unimportant one.
- **Observable:** Inject a jsonl close error; the parquet footer is still written and the parquet file
  is readable.
- **Mechanism:** Always. Oracle: fault-inject jsonl path + external parquet readability check.
- **Needs SUT fix:** Reorder `multi.rs` close/flush/write_metric to finalize parquet first (or
  best-effort both, aggregating errors) at `multi.rs:46-48,57-58,69-72`.
- **Evidence:** `multi.rs:69` `jsonl.close()?` before `:71` `parquet.close()?`.

## capture-histogram-drops-counted — dropped histogram samples are counted, not silently lost
- **Subsystem:** capture · **Priority:** P1 · **Assertion:** Always · **Needs target:** no
- **Statement:** When the capture channel is full or the recorder is uninitialized, dropped
  latency/histogram samples increment a bounded-label counter held in the registry, so tail-biased
  sample loss is observable.
- **Observable:** Under high recording load a run delivering incomplete histograms shows a nonzero
  `capture_histogram_samples_dropped` rather than looking healthy.
- **Mechanism:** Observability. Oracle: saturate the channel, assert dropped count > 0 when samples
  were lost.
- **Needs SUT fix:** Add a `capture_histogram_samples_dropped` counter (bounded reason label, in
  registry not the sample channel) at `manager.rs:121-133`. Landed on backup branch (`39d8ae56`), not
  main.
- **Evidence:** `manager.rs:302` `channel(10_000)`; `:121-133` `try_send` + warn-drop.

## capture-no-fsync-durability — flushed capture lines survive whole-VM termination
- **Subsystem:** capture · **Priority:** P2 · **Assertion:** Always · **Needs target:** no
- **Statement:** Capture data reported as flushed reaches the persisted volume durably; a
  whole-container/VM `node_termination` does not lose already-flushed jsonl lines.
- **Observable:** After `node_termination`, the surviving prefix includes all lines that were flushed
  before the last maturity boundary. If page-cache writes are lost, flushed lines vanish.
- **Mechanism:** Durability invariant. Oracle: `node_termination` scenario comparing pre-kill flush
  count to post-kill parsed count.
- **Needs SUT fix:** Add `File::sync_data`/`sync_all` at flush boundaries (or document that maturity =
  handed-to-OS, not durable) in the `lading_capture` jsonl/parquet sinks.
- **Evidence:** No `sync_all`/`sync_data` in `lading_capture` per grep; `jsonl.rs:53`
  `BufWriter::flush` only.
- **Open questions:** Does `node_termination` preserve OS page cache to the persisted named volume?
  Determines P2 vs P1.

## capture-drift-no-silent-gap — drift correction does not silently drop unflushed intervals
- **Subsystem:** capture · **Priority:** P2 · **Assertion:** Always · **Needs target:** no
- **Statement:** When the flush-tick advances multiple ticks after a scheduling stall, the
  accumulator does not overwrite unflushed ring slots, so the capture has no invisible `fetch_index`
  gaps.
- **Observable:** Under >60s scheduling starvation the capture retains all intervals (no large
  `fetch_index` gap). Gaps pass the current strict-monotonic validator, so loss is invisible today.
- **Mechanism:** Always. Oracle: induce scheduling starvation (Antithesis) + validator extended to
  detect gaps.
- **Needs SUT fix:** Flush between `advance_tick` iterations in `state_machine.rs:229-231` drift loop;
  optionally have `validate` flag large `fetch_index` gaps.
- **Evidence:** `state_machine.rs:219-232` drift loop; `accumulator.rs:466-481` overwrite.

## split-mode-merge-partial-tolerant — split-mode capture merge tolerates one clean side when the other overruns
- **Subsystem:** capture · **Priority:** P2 · **Assertion:** Always · **Needs target:** yes
- **Statement:** In split mode, if only the sender (or only the receiver) lading overruns/crashes
  leaving an unreadable parquet, the merge outcome and replicate-failure attribution are
  well-defined (a clean side's captures are not needlessly discarded by the other's corruption).
- **Observable:** Merge of a truncated file + a clean file behaves per policy (fail attributed to the
  corrupt side), not a silent whole-replicate loss when one side captured cleanly.
- **Mechanism:** Merge robustness. Oracle: split-mode scenario killing only one side.
  DEPLOYMENT-DERIVED LEAD (validate against source; do not name the deployment).
- **Needs SUT fix:** none in lading proper; LEAD to validate against the deployment's oblivious merge
  + lading's per-instance capture finalize.
- **Evidence:** Digest deployment findings; `capture_file_merge` oblivious merge.
- **Open questions:** Does a sender-only overrun fail the whole replicate when the receiver captured
  cleanly?

---

# Properties — payload / determinism (lading_payload)

## block-cache-construction-terminates — block-cache construction always terminates in bounded time
- **Subsystem:** payload/determinism · **Priority:** P0 · **Assertion:** Always · **Needs target:** no
- **Statement:** `construct_block_cache_inner` terminates (with blocks or `InsufficientBlockSizes`)
  in bounded time for every config; it never spins forever when `max_block_size` is below the
  serializer's minimum viable block.
- **Observable:** Startup completes (or errors) within a bounded time even with a tiny
  `maximum_block_size`. Confirmed live: the loop has no time/iteration cap; on repeated `EmptyBlock`
  `min_block_size` stays `< max_block_size` so neither exit fires.
- **Mechanism:** Liveness (bounded startup). Oracle: small-`maximum_block_size` scenario + external
  startup-time bound; SUT `reachable!` at the bounded-exit.
- **Needs SUT fix:** Cap consecutive rejections / add a time bound in `block.rs:625-673` and return
  `InsufficientBlockSizes`; reject `max_block_size` below a serializer-reported floor. (trace_agent
  v04 variant fixed on backup branch `456e85a3`; general case still live on main.)
- **Evidence:** `block.rs:625-673` (no cap; `min_block_size=0.25*block_size`).

## trace-agent-v04-block-terminates — trace-agent v04 block-cache next_block terminates in bounded time
- **Subsystem:** payload/determinism · **Priority:** P0 · **Assertion:** Always · **Needs target:** no
- **Statement:** trace-agent v04 block-cache construction terminates quickly even when a serialized
  trace exceeds `max_block_size`; `to_bytes` never emits a 1-byte empty msgpack array accepted as a
  valid block, and construction is not O(n²) re-serialization.
- **Observable:** Construction completes in sub-second, not hours (observed 31h hang → 0.75s). An
  empty result is `EmptyBlock`, not a 1-byte block.
- **Mechanism:** Liveness. Fuzz property `trace_agent_v04_cache_fixed_next_block` + bounded-time
  startup assertion.
- **Needs SUT fix:** Cap consecutive rejections (`InsufficientBlockSizes`) and emit `EmptyBlock` not a
  1-byte block (fix `456e85a3` on backup branch; live on main).
- **Evidence:** `block.rs`, `trace_agent/v04.rs`; digest `456e85a3`.

## payload-determinism-byte-identical — same seed + config yields byte-identical load
- **Subsystem:** payload/determinism · **Priority:** P1 · **Assertion:** Always · **Needs target:** no
- **Statement:** For a fixed seed and config, the sequence of bytes lading emits is identical across
  runs; no wall-clock, HashMap iteration order, or hidden entropy feeds payload bytes.
- **Observable:** Two runs with identical seed/config produce identical byte streams at the sink (or
  identical block-cache hashes). Violation breaks the determinism ADR and Antithesis reproducibility.
- **Mechanism:** Determinism. Oracle: byte-equality across two seeded runs, or a SUT `always!` on a
  rolling hash of emitted blocks. Lead analog: SMPTNG-762 (CPU-jitter entropy) / SMPTNG-767
  (non-monotonic wall clock) are SUT bugs whose class must not exist in lading.
- **Needs SUT fix:** none expected (BTreeMap/BTreeSet ordering, rng-derived timestamps confirmed);
  property guards against regressions introducing wall-clock/entropy.
- **Evidence:** Digest payload finding: BTreeMap/BTreeSet, FxHashMap index-agnostic, rng-derived
  timestamps.

## no-wall-clock-in-payloads — payload timestamps are seed-derived and monotonic, never wall-clock
- **Subsystem:** payload/determinism · **Priority:** P2 · **Assertion:** Always · **Needs target:** no
- **Statement:** Timestamps embedded in generated payloads (trace_agent, otel, templated_json) are
  derived from the rng/config, never from the system wall clock, and are monotone within a stream.
- **Observable:** Payload timestamps are reproducible across runs and unaffected by a backward clock
  step. Violation mirrors SMPTNG-767's out-of-order/duplicate timestamp class.
- **Mechanism:** Always. Oracle: replay under a perturbed clock (Antithesis clock control) and compare
  payload timestamps for equality/monotonicity.
- **Needs SUT fix:** none currently; guards against regressions.
- **Evidence:** `trace_agent/v04.rs:326,384` rng timestamps; `block.rs` Instant feeds only progress
  logging.

## dogstatsd-tag-length-validated — DogStatsD tag_length.end() <= MIN_TAG_LENGTH is rejected upfront
- **Subsystem:** payload/determinism · **Priority:** P2 · **Assertion:** Always · **Needs target:** no
- **Statement:** A dogstatsd config with `tag_length.end() <= MIN_TAG_LENGTH` is rejected at
  construction with a dedicated error naming the value, not swallowed as `Error::StringGenerate`
  which silently drops the message.
- **Observable:** Such a config errors clearly through `DogStatsD::new` rather than mis-surfacing as a
  pool-generation error.
- **Mechanism:** Config regression. Oracle: proptest/fixture over `tag_length` bounds asserting the
  dedicated error path.
- **Needs SUT fix:** none (merged to main, `e98e3052`); property guards against regression.
- **Evidence:** `e98e3052` (#1875).

---

# Properties — generators / throttle

## throttle-divide-no-silent-underdelivery — a config that delivers at N=1 still delivers at N>1
- **Subsystem:** generators/throttle · **Priority:** P0 · **Assertion:** Always · **Needs target:** yes
- **Statement:** `throttle.divide` shrinks per-worker capacity consistently with the block size a
  worker draws, so a block accepted at `parallel_connections=1` is not rejected by every worker
  (`Capacity`) at N>1, yielding silent zero delivery.
- **Observable:** For `bytes_per_second/N < block <= bytes_per_second` the aggregate delivered bytes
  at N>1 are nonzero and match the N=1 rate; today every worker discards. Silent (only a `debug!`
  log).
- **Mechanism:** Rate fidelity. Provable as a pure proptest against the real `Valve` (no rig);
  Antithesis oracle: sink byte counter vs configured rate.
- **Needs SUT fix:** `divide` must shrink block sizing consistently with capacity, or generators must
  validate `maximum_block_size <= bytes_per_second/parallel_connections` upfront. Demonstrated on
  backup branch (`0868e39c`); live on main.
- **Evidence:** `lib.rs` divide `capacity/divisor`; `tcp.rs:273-276` discard+loop; digest throttle
  finding.

## linear-ramp-slope-preserved — Linear throttle aggregate ramp slope equals configured rate_of_change
- **Subsystem:** generators/throttle · **Priority:** P0 · **Assertion:** Always · **Needs target:** yes
- **Statement:** With a Linear throttle and `parallel_connections>1`, the aggregate ramp slope across
  workers equals the single configured `rate_of_change`, not N times it.
- **Observable:** Aggregate delivered-rate ramp reaches max in the configured time, not 1/N of it.
  Confirmed live: `divide()` divides capacities but passes rate unchanged (`rate_of_change: rate`) so
  N workers each ramp at full rate.
- **Mechanism:** Rate fidelity. Pure proptest measuring aggregate slope; or Antithesis oracle
  sampling sink rate over the warmup ramp.
- **Needs SUT fix:** Divide `rate_of_change` by divisor in the `lib.rs` Linear divide arm (the
  call-site comment claims preservation but aggregate is N*rate).
- **Evidence:** `lib.rs` Linear divide arm: `rate_of_change: rate` unchanged; digest `914bb14a`.

## throttle-capacity-no-zerodelivery-livelock — a block larger than throttle capacity never becomes a zero-delivery busy loop
- **Subsystem:** generators/throttle · **Priority:** P0 · **Assertion:** Always · **Needs target:** yes
- **Statement:** No config leaves a generator discarding every block (block > per-worker capacity) in
  a hot loop at ~100% CPU delivering ~zero bytes; such a config is rejected at startup.
- **Observable:** A run either delivers a nonzero rate or fails fast at startup; it never burns a core
  delivering nothing (0.31.2 busy-discard livelock class).
- **Mechanism:** No-livelock. Oracle: oversized-block scenario + CPU/throughput observation; or
  startup-error assertion. Related to `throttle-divide-no-silent-underdelivery`.
- **Needs SUT fix:** Validate `maximum_block_size <= bytes_per_second/parallel_connections`
  (post-divide) at construction, returning a clear error instead of a runtime discard/spin.
- **Evidence:** `common.rs:147-157`; `stable.rs:151-156` Capacity no-wait; tcp/udp/unix discard+loop.

## grpc-honors-throttle — gRPC generator honors throttle rejections and configured rate
- **Subsystem:** generators/throttle · **Priority:** P0 · **Assertion:** Always · **Needs target:** yes
- **Statement:** The gRPC generator discards+counts throttle-rejected blocks (like tcp/udp) and does
  not send a block regardless of the throttle result; delivered rate does not exceed configured.
- **Observable:** gRPC delivered rate `<= bytes_per_second`; on a `Capacity` error the block is not
  sent. Currently `let _ = result;` ignores the outcome and sends anyway.
- **Mechanism:** Rate fidelity. Oracle: sink byte-rate vs configured under a gRPC scenario.
- **Needs SUT fix:** Honor the throttle `Result` in `grpc.rs:307-318` (discard + count
  `blocks_discarded` on rejection). Landed on backup branch (`944d4be4`); live on main.
- **Evidence:** `grpc.rs:307-318` (`let _ = result;`).

## stable-burst-envelope-bounded — stable throttle never exceeds its per-interval burst envelope
- **Subsystem:** generators/throttle · **Priority:** P1 · **Assertion:** Always · **Needs target:** no
- **Statement:** In the real async `wait_for` path under an adversarial clock, the stable throttle
  grants at most `maximum_capacity` at `timeout=0` (no over-delivery) and at most
  `(MAX_ROLLED_INTERVALS+1)×` with rolled capacity.
- **Observable:** Granted capacity per interval stays within the proven envelope; a
  clock-perturbation-induced over-grant is a defect.
- **Mechanism:** Rate safety. SUT-side feature-gated assertion under an adversarial clock (Antithesis
  controls time) + proptests.
- **Needs SUT fix:** none if Kani proofs hold; add feature-gated `assert_always!` in `stable.rs`
  (landed on backup branch `3f4a6bd2`) to catch regressions.
- **Evidence:** `stable.rs`; digest `3f4a6bd2`.

## unix-throttle-aggregate-consistent — unix_stream/unix_datagram aggregate rate matches configured bytes_per_second
- **Subsystem:** generators/throttle · **Priority:** P1 · **Assertion:** Always · **Needs target:** yes
- **Statement:** unix_stream and unix_datagram divide the throttle across `parallel_connections` so
  aggregate delivery approximates `bytes_per_second`, consistent with tcp/udp, rather than delivering
  `parallel_connections ×` the rate.
- **Observable:** Aggregate delivered bytes for unix generators at N>1 approximate `bytes_per_second`,
  not N× it. Cross-generator inconsistency: same config key means aggregate for tcp/udp but
  per-connection for unix.
- **Mechanism:** Rate fidelity. Oracle: sink byte-rate vs configured.
- **Needs SUT fix:** Add `.divide(worker_count)` in `unix_stream.rs:161-163` and
  `unix_datagram.rs:186-188` (or document per-connection semantics deliberately).
- **Evidence:** `unix_stream.rs:161-163`, `unix_datagram.rs:186-188` (no divide) vs `tcp.rs`.
- **Open questions:** Is undivided per-connection throttle intentional for unix_stream?

## discarded-blocks-counted — under-delivery is observable: discarded blocks are counted
- **Subsystem:** generators/throttle · **Priority:** P1 · **Assertion:** Always · **Needs target:** yes
- **Statement:** tcp/udp/grpc generators count throttle-rejected/discarded blocks (`blocks_discarded`)
  so a run delivering ~zero bytes is distinguishable from a healthy flat-metrics run.
- **Observable:** A zero-byte-delivery run surfaces a nonzero `blocks_discarded` rather than only a
  `debug!` log. Distinguishes silent under-delivery.
- **Mechanism:** Observability. Oracle: oversized-block scenario asserting
  `delivered==0 ⇒ blocks_discarded>0`.
- **Needs SUT fix:** Add `blocks_discarded` counters in `tcp.rs`/`udp.rs` (`73c4805e` on backup
  branch) and `grpc.rs`; live on main.
- **Evidence:** `tcp.rs:273-276` `debug!` only; digest `73c4805e`.

## error-label-cardinality-bounded — generator error-metric label cardinality is bounded
- **Subsystem:** generators/throttle · **Priority:** P1 · **Assertion:** Always · **Needs target:** yes
- **Statement:** `connection_failure`/`request_failure` error labels are drawn from a finite set
  (`io::ErrorKind`), not raw `err.to_string()`; a flapping target cannot grow capture memory without
  bound.
- **Observable:** Distinct error-label values stay bounded regardless of failure diversity; capture
  accumulator key count does not grow unboundedly (ADR-005 OOM class).
- **Mechanism:** Bounded cardinality / memory. Oracle: flapping-target scenario + capture key-count /
  memory observation.
- **Needs SUT fix:** Map errors to `io::ErrorKind` for labels in `tcp.rs`/`udp.rs` (`32dd4cf6` on
  backup branch); gRPC tonic error still raw (follow-up). Live on main.
- **Evidence:** `tcp.rs`/`udp.rs` raw `err.to_string()` labels; digest `32dd4cf6`.

## sink-receives-bytes — the sink receives load (load-arrival non-vacuity)
- **Subsystem:** generators/throttle · **Priority:** P1 · **Assertion:** Sometimes · **Needs target:** yes
- **Statement:** Across a run the sink container receives a nonzero number of bytes from lading
  generators.
- **Observable:** `sink/main.rs:82` `sometimes!(total>0)` "sink received bytes". Guards against a
  whole-config class delivering nothing (divide stall, capacity livelock, throttle bypass).
- **Mechanism:** Non-vacuity of delivery on at least one timeline. Oracle already present in the
  never-faulted sink container.
- **Needs SUT fix:** none (already instrumented).
- **Evidence:** `sink/main.rs:69-83`.

## divide-by-zero-startup-error — bytes_per_second < parallel_connections fails with a clear error
- **Subsystem:** generators/throttle · **Priority:** P2 · **Assertion:** Always · **Needs target:** yes
- **Statement:** A config where `bytes_per_second` divided by `parallel_connections` rounds to zero
  produces a clear startup validation error, and integer-division truncation does not silently drop
  the remainder rate.
- **Observable:** Small-bps/high-connection config errors clearly at startup; aggregate delivered rate
  is within one interval-quantum of configured (no systematic under-delivery of up to N-1
  bytes/interval).
- **Mechanism:** Always. Oracle: startup-error assertion + sink rate check.
- **Needs SUT fix:** Distribute the division remainder across workers, and surface `DivisionByZero` as
  a config validation error naming `bytes_per_second`/`parallel_connections`.
- **Evidence:** `lib.rs` divide `DivisionByZero`; `tcp.rs:168-173` integer divide truncation.

## unix-stream-write-error-progress — unix_stream makes progress on non-BrokenPipe write errors
- **Subsystem:** generators/throttle · **Priority:** P2 · **Assertion:** Always · **Needs target:** yes
- **Statement:** On a non-BrokenPipe, non-WouldBlock write error (e.g. `ConnectionReset`) the
  unix_stream worker reconnects or advances rather than busy-looping on the same offset spamming
  `request_failure`.
- **Observable:** A `ConnectionReset` receiver does not pin a core or emit a runaway `request_failure`
  count; `packets_sent` is not inflated per partial write.
- **Mechanism:** No-livelock. Oracle: reset-injecting receiver + CPU/counter observation.
- **Needs SUT fix:** Handle non-BrokenPipe write errors (advance/break/reconnect) at
  `unix_stream.rs:304-315`; count packets per block not per partial write.
- **Evidence:** `unix_stream.rs:304-315,293-295`.

---

# Properties — observer

## observer-pid-reuse-no-panic — observer never aborts on PID reuse / mismatch
- **Subsystem:** observer · **Priority:** P0 · **Assertion:** Unreachable · **Needs target:** yes
- **Statement:** The stat sampler degrades (skips the stale sample) instead of asserting
  `cur_pid==pid` when a recycled/mismatched PID is read.
- **Observable:** Target exit + PID recycle races do not SIGABRT the run. Confirmed live:
  `assert!(cur_pid == pid)` at `stat.rs:82`.
- **Mechanism:** Unreachable (the assert must never fire). Oracle: rapid target-exit/PID-reuse
  scenario + panic hook.
- **Needs SUT fix:** Replace the `assert!` at `stat.rs:82` with a skip-and-continue (fix `6aa1b1ba`
  on backup branch; live on main).
- **Evidence:** `stat.rs:82` `assert!(cur_pid == pid)`.

## observer-process-vanish-no-panic — observer never panics when a target vanishes mid-listing
- **Subsystem:** observer · **Priority:** P0 · **Assertion:** Unreachable · **Needs target:** yes
- **Statement:** `ProcessDescendantsIterator` degrades to zero descendants when `Process::new` fails,
  rather than panicking, when a target exits before descendant listing.
- **Observable:** A target exiting mid-listing does not crash the run.
- **Mechanism:** Unreachable. Oracle: target-exits-during-sampling scenario + panic hook.
- **Needs SUT fix:** Yield an empty iterator instead of `panic!` at `process_descendents.rs:13` (fix
  `7e1d2968` on backup branch; live on main).
- **Evidence:** `process_descendents.rs:13` `panic!`.

## observer-cpu-max-parse-no-panic — observer never panics on malformed/truncated cpu.max
- **Subsystem:** observer · **Priority:** P0 · **Assertion:** Unreachable · **Needs target:** yes
- **Statement:** `parse_allowed_cores` is bounds-checked (no index panic, guards a zero period) so a
  malformed/truncated cgroup `cpu.max` degrades rather than aborting.
- **Observable:** A truncated `cpu.max` read (fault-injected) does not SIGABRT the run.
- **Mechanism:** Unreachable. Oracle: malformed-`cpu.max` fault + panic hook.
- **Needs SUT fix:** Bounds-check parsing / guard zero period (fix `6aa1b1ba` on backup branch; live
  on main).
- **Evidence:** `stat.rs` `cpu.max` parse; digest `6aa1b1ba`.

## observer-transient-read-not-fatal — a transient observer read error does not kill the run
- **Subsystem:** observer · **Priority:** P1 · **Assertion:** Always · **Needs target:** yes
- **Statement:** A single transient procfs/cgroup/wss read error is best-effort (log + skip that
  component's sample); it does not `?`-propagate and terminate the whole experiment.
- **Observable:** An injected transient read error yields a skipped sample + warning, not a dead run.
  Only persistent problems show as repeated warnings + absent metrics.
- **Mechanism:** Liveness. Oracle: transient-read-fault scenario asserting the run continues to
  self-termination.
- **Needs SUT fix:** Treat component reads as best-effort in `observer/linux.rs` `sample()` (fix
  `30b86a71` on backup branch; live on main).
- **Evidence:** `observer/linux.rs`; digest `30b86a71`.

## observer-pid-identity-fingerprint — observer samples the identified target, not a PID-reuse impostor
- **Subsystem:** observer · **Priority:** P2 · **Assertion:** Always · **Needs target:** yes
- **Statement:** lading reports target-exit iff the process identified at startup exits; after a
  watched PID is recycled, the observer/pidfd path does not silently attach to an unrelated process
  and emit wrong metrics.
- **Observable:** Metrics attributed to the target correspond to the original process identity; a
  recycled PID does not produce plausible-but-wrong metrics with no error.
- **Mechanism:** Metric integrity. Oracle: PID-reuse scenario checking metric identity. LEAD (TOCTOU,
  `PID_MAX` small).
- **Needs SUT fix:** Capture a start-time/identity fingerprint (proc `start_time`) and validate it in
  the pidfd/sampler paths (`target.rs:302-332`, observer sampler).
- **Evidence:** `target.rs:302-332,246-257`; observer/linux sampler.

## get-available-memory-cgroup-chain — memory limit reflects the tightest cgroup v2 ancestor limit
- **Subsystem:** observer · **Priority:** P2 · **Assertion:** Always · **Needs target:** no
- **Statement:** `get_available_memory` walks the cgroup v2 ancestor chain and returns the minimum
  `memory.max`, matching kernel hierarchical enforcement, rather than reading `max` from a namespaced
  root and believing it has `u64::MAX`.
- **Observable:** Reported available memory equals the effective container limit, not `u64::MAX`, in a
  cgroup-namespaced container.
- **Mechanism:** Accuracy. Oracle: deterministic test with synthetic cgroup files (injectable reader).
- **Needs SUT fix:** none (merged to main, `1085887c`); property guards against regression via the
  injectable `get_available_memory_with` reader.
- **Evidence:** `1085887c`; digest regression finding.

---

# Properties — no-panic (umbrella + specific sites)

## no-panic-anywhere — lading never panics (panic hook = Unreachable)
- **Subsystem:** no-panic · **Priority:** P0 · **Assertion:** Unreachable · **Needs target:** no
- **Statement:** No panic occurs anywhere in the lading SUT under any config, fault, timing, or
  shutdown path; the panic hook must never report "lading panicked".
- **Observable:** `antithesis_hooks.rs:27` panic hook fires `unreachable!` "lading panicked"
  `{message,location}` on any panic before `panic=abort` SIGABRTs the container. Directly
  Antithesis-visible.
- **Mechanism:** Umbrella no-panic invariant. Assertion type Unreachable because a panic is a point in
  the code that must never be reached; the existing panic hook is the oracle. `panic=abort` makes
  every hit a hard, observable process abort. SUT-side probe already present.
- **Needs SUT fix:** none (hook already wired); individual panic sites listed as separate properties
  need the ADR-compliant Result conversions.
- **Evidence:** `antithesis_hooks.rs:13-33` panic hook; `Cargo.toml:115,120` `panic=abort`.

## observer-target-pid-recv-no-panic — observer returns an error, not a panic, when the target PID never arrives
- **Subsystem:** no-panic · **Priority:** P0 · **Assertion:** Unreachable · **Needs target:** yes
- **Statement:** If a Binary target fails to spawn or exits before sending its PID, the observer
  returns an error instead of `.expect("catastrophic failure")` panicking on the closed channel.
- **Observable:** A bad target path / instant-exit target does not panic the observer task. Currently
  the `recv()` `Err(Closed)` hits `.expect`.
- **Mechanism:** Unreachable. Oracle: instant-exit/bad-path target scenario + panic hook (currently
  only partly masked as a join-error log).
- **Needs SUT fix:** Handle `recv()` `Err` and `None` with a returned error at `observer.rs:114-120`.
- **Evidence:** `observer.rs:114-120`; `target.rs:395-396`.

## block-cache-zero-max-no-panic — maximum_block_size of 0 is rejected, not a random_range panic
- **Subsystem:** no-panic · **Priority:** P1 · **Assertion:** Unreachable · **Needs target:** no
- **Statement:** A `maximum_block_size` that resolves to 0 is rejected at validation; block-cache
  construction never calls `rng.random_range` on an empty `0..0` range.
- **Observable:** Config with `maximum_block_size '0 B'` produces a clean startup error, not a panic.
  Currently reachable via `grpc.rs` forwarding unvalidated `as_u128()`.
- **Mechanism:** Unreachable (the empty-range `random_range` must never be reached). Oracle:
  zero-block-size scenario + panic hook.
- **Needs SUT fix:** Add a lower-bound (`>=1`, or `>=` serializer floor) check on `maximum_block_size`
  in `block.rs:214-220` and at generator call sites (e.g. `grpc.rs:205-223`).
- **Evidence:** `block.rs:628` `random_range(min..max)`; `:214-220` guard misses 0.

## per-generator-semaphore-no-panic — two HTTP (or two Splunk-HEC) generators coexist without panic
- **Subsystem:** no-panic · **Priority:** P1 · **Assertion:** Unreachable · **Needs target:** yes
- **Statement:** Each HTTP/Splunk-HEC generator instance owns its own connection semaphore;
  configuring two such generators does not panic on a process-wide `OnceCell::set` and gives
  independent connection limits.
- **Observable:** A config with two `Http` generators starts without the second `new()` panicking;
  per-generator concurrency limits are independent. `config.generator` is a `Vec` so this config is
  allowed.
- **Mechanism:** Unreachable (OnceCell double-set panic must never be reached). Oracle:
  two-http-generator scenario + panic hook.
- **Needs SUT fix:** Replace the static `CONNECTION_SEMAPHORE` OnceCell with a per-instance
  `Arc<Semaphore>` in `http.rs:37/187-189` and `splunk_hec.rs:51/243-245`; make the hot-path
  `.expect("semaphore closed")` stop the worker gracefully. Landed on backup branch (`5f8c375e`);
  live on main.
- **Evidence:** `http.rs:37,187-189`; `splunk_hec.rs:51,243-245`; `config.rs:101` `Vec`.

## splunk-hec-response-parse-no-panic — Splunk-HEC response parsing never panics on a non-HecResponse body
- **Subsystem:** no-panic · **Priority:** P1 · **Assertion:** Unreachable · **Needs target:** yes
- **Statement:** The Splunk-HEC generator's spawned request task handles a non-HecResponse body
  (empty, HTML error page, "ok") without panicking via `.expect` on serde_json parse.
- **Observable:** A blackhole/target returning a non-JSON body does not abort the detached task.
  `panic=abort` makes this whole-process fatal.
- **Mechanism:** Unreachable. Oracle: blackhole returning a plain-text body + panic hook.
- **Needs SUT fix:** Replace `serde_json::from_slice::<HecResponse>(...).expect(...)` at
  `splunk_hec.rs:371-374` with error handling; also track the detached task's handle.
- **Evidence:** `splunk_hec.rs:371-374`.

## generator-addr-uri-validation-no-panic — malformed addr/target_uri is a Result error, not a construction panic
- **Subsystem:** no-panic · **Priority:** P1 · **Assertion:** Unreachable · **Needs target:** no
- **Statement:** An unresolvable/malformed socket address (tcp/udp) or `target_uri` (grpc) yields a
  clean config error, not an `.expect` panic at generator construction.
- **Observable:** Config with a bad addr/uri fails startup with an error; the panic hook does not
  fire.
- **Mechanism:** Unreachable. Oracle: malformed-config scenario + panic hook.
- **Needs SUT fix:** Return `Result` errors instead of `.expect` at `tcp.rs:154-159`,
  `udp.rs:164-169`, `grpc.rs:226-231,245-247`.
- **Evidence:** `tcp.rs:154-159`; `udp.rs:164-169`; `grpc.rs:226-247`.

## tcp-rr-listener-no-panic — tcp_rr blackhole returns an error, not a panic, on bind failure
- **Subsystem:** no-panic · **Priority:** P1 · **Assertion:** Unreachable · **Needs target:** no
- **Statement:** tcp_rr blackhole listener setup failures (address in use, stale bind) return
  `Error::Bind` rather than panicking, including the `threads>1` thread-0 prebuild path on the main
  async task.
- **Observable:** A pre-bound data port yields a clean error, not a SIGABRT. Currently
  `create_listener` uses `expect()`/`panic!` (`tcp_rr.rs:345-346`) and thread-0 prebuild runs in
  async `run()` at `:179`.
- **Mechanism:** Unreachable. Oracle: pre-bound-port scenario with `threads>1` + panic hook.
- **Needs SUT fix:** Return `Result` from `create_listener` (`tcp_rr.rs:313-346`) instead of
  `expect`/`panic`.
- **Evidence:** `tcp_rr.rs:345-346,179`.
- **Open questions:** Is tcp_rr ever configured with `threads>1`?

## logrotate-stale-tick-noop — logrotate_fs treats a stale tick as a no-op, never a panic
- **Subsystem:** no-panic · **Priority:** P1 · **Assertion:** Unreachable · **Needs target:** no
- **Statement:** `Model::advance_time` treats a tick below current model time as a no-op early return
  (a benign FUSE scheduling reorder), never asserting/panicking.
- **Observable:** Reordered FUSE ops presenting a stale tick do not crash the logrotate_fs generator.
  No clock fault needed to trigger.
- **Mechanism:** Unreachable (the stale-tick assert must never fire). Oracle: concurrent FUSE op
  scenario + panic hook.
- **Needs SUT fix:** Make `advance_time` early-return on a stale tick in `logrotate_fs/model.rs` (fix
  `220850e5` on backup branch; live on main).
- **Evidence:** `logrotate_fs/model.rs`; digest `220850e5`.

## arbitrary-block-nonzero-no-panic — fuzz Arbitrary Block construction handles zero total_bytes without panic
- **Subsystem:** no-panic · **Priority:** P2 · **Assertion:** Unreachable · **Needs target:** no
- **Statement:** The `arbitrary::Arbitrary` impl for `Block` handles a generated `total_bytes` of 0
  without `.expect` panicking, so a fuzz run is not aborted by benign input.
- **Observable:** A fuzz input yielding `total_bytes==0` is rejected gracefully (`arbitrary::Error`)
  not a `NonZeroU32` expect panic. Only compiled under the `arbitrary` feature (fuzz harness), so no
  production impact.
- **Mechanism:** Unreachable within the fuzz harness. Oracle: `cargo fuzz run`.
- **Needs SUT fix:** Return `Err(arbitrary::Error::IncorrectFormat)` instead of `.expect` at
  `block.rs:119-127`.
- **Evidence:** `block.rs:119-127`.

---

# Properties — blackhole / config

## unix-datagram-blackhole-removes-stale-socket — unix_datagram blackhole removes a stale socket before bind
- **Subsystem:** blackhole/config · **Priority:** P1 · **Assertion:** Always · **Needs target:** yes
- **Statement:** The unix_datagram blackhole actually removes a leftover socket file before binding,
  so it starts cleanly after a hard-kill restart instead of failing bind with `EADDRINUSE`.
- **Observable:** After a `node_termination` leaving a stale socket, the blackhole binds successfully
  and the target keeps its sink. Confirmed live:
  `let _res = tokio::fs::remove_file(...).map_err(...)` builds a lazy future that is never awaited.
- **Mechanism:** Restart resilience. Oracle: restart-with-stale-socket scenario asserting bind
  succeeds. LEAD: impact depends on whether the socket path is on a persisted volume.
- **Needs SUT fix:** Await the `remove_file` future in `unix_datagram.rs:95` (drop the lazy
  `TryFutureExt::map_err` binding).
- **Evidence:** `unix_datagram.rs:95` lazy future.
- **Open questions:** Is the unix socket path on a persisted named volume in the deployment?

## datadog-blackhole-accept-resilient — Datadog blackhole keeps accepting after a transient accept error
- **Subsystem:** blackhole/config · **Priority:** P1 · **Assertion:** Always · **Needs target:** yes
- **Statement:** The Datadog blackhole's accept loop logs-and-continues on an `accept()` error and
  never wedges (silently ceasing to accept while appearing alive), so it never backpressures the
  target.
- **Observable:** Under fd exhaustion / transient accept errors the blackhole keeps serving new
  connections. Confirmed live: `Ok((stream,_addr)) = listener.accept()` fallible select arm with no
  else disables the branch on `Err`, then blocks on shutdown forever.
- **Mechanism:** Blackhole never-backpressure. Oracle: fd-exhaustion fault scenario asserting the
  target's connections still succeed.
- **Needs SUT fix:** Match `accept()` and continue on `Err` (as `common.rs:89-96` does) at
  `datadog.rs:209`.
- **Evidence:** `datadog.rs:207-212` fallible select arm.

## blackhole-never-backpressures-target — blackhole responds to the target regardless of capture-channel saturation
- **Subsystem:** blackhole/config · **Priority:** P1 · **Assertion:** Always · **Needs target:** yes
- **Statement:** A blackhole does not block its HTTP response to the target on a full bounded capture
  channel; a slow/saturated capture manager cannot stall the target.
- **Observable:** With a slow capture drain, target request latency stays bounded; the Datadog
  blackhole does not `await send().await` per metric point before responding.
- **Mechanism:** Never-backpressure. Oracle: slow-capture-drain fault scenario measuring target
  response latency.
- **Needs SUT fix:** Use `try_send` + count-on-drop (consistent with the manager histogram path)
  instead of blocking `send().await` in `datadog.rs` `handle_v2_protobuf` (398,412,416) before the
  response is built.
- **Evidence:** `datadog.rs:296-299` response after per-point awaits; `lib.rs:53-56` bounded send.

## sqs-receive-message-bounded — SQS blackhole bounds ReceiveMessage response size
- **Subsystem:** blackhole/config · **Priority:** P2 · **Assertion:** Always · **Needs target:** no
- **Statement:** The SQS blackhole caps `max_number_of_messages` so a single target-controlled request
  cannot force an enormous allocation and OOM the blackhole.
- **Observable:** A request with a huge `max_number_of_messages` produces a bounded response (real SQS
  caps at 10), not an unbounded String allocation.
- **Mechanism:** Amplification/OOM. Oracle: adversarial-request scenario + memory observation.
- **Needs SUT fix:** Clamp `num_messages` to a max (e.g. 10) at `sqs.rs:257-267` before the
  `0..num_messages` loop.
- **Evidence:** `sqs.rs:362-370,257-267`.

## config-numeric-fields-validated — numeric config fields are validated at load, not deferred to runtime failure
- **Subsystem:** blackhole/config · **Priority:** P2 · **Assertion:** Always · **Needs target:** no
- **Statement:** parquet/zstd `compression_level` (1-22) and `sample_period_milliseconds` (>0) are
  range-checked at config load, so an out-of-range value fails early rather than corrupting a capture
  write or driving a tight sampling loop.
- **Observable:** An out-of-range `compression_level` or a zero `sample_period` is rejected at startup
  (the documented crash-early location), not surfaced as a mid-run zstd error / busy loop.
- **Mechanism:** Validate-early. Oracle: out-of-range-config scenario asserting clean startup error.
- **Needs SUT fix:** Add range checks in `config.rs:186-198` (`compression_level`) and `:106-107`
  (`sample_period_milliseconds > 0`).
- **Evidence:** `config.rs:186-198,106-107`.

## recorded-traffic-crash-consistency — blackhole-recorded traffic files are crash-consistent
- **Subsystem:** blackhole/config · **Priority:** P2 · **Assertion:** Always · **Needs target:** yes
- **Statement:** The blackhole traffic recorder writes files that, when compressed, remain decodable
  up to the last flushed frame after an abrupt kill, and are deterministic for a fixed input.
- **Observable:** A recorder file from a SIGKILLed blackhole decodes to a valid prefix (analogous to
  jsonl), not undecodable-past-last-frame corruption.
- **Mechanism:** Crash-consistency. Oracle: `node_termination` scenario + external decode check. LEAD.
- **Needs SUT fix:** none identified; LEAD to validate zstd framing / flush boundaries of the recorder
  (SMPTNG-390 / record-policy work #1911/#1895).
- **Evidence:** Digest Atlassian SMPTNG-390; commits #1911,#1895.
- **Open questions:** Are recorded-traffic files zstd-framed such that a truncated stream is a valid
  prefix?

---

# Scenarios needed (harness config-variation + fault menu)

- **unreachable-target:** generator points at an address/socket/uri/container that never
  binds/appears; experiment timer fires; assert lading exits within `max_shutdown_delay` (drives
  tcp/udp/unix/grpc connect-loop hangs and docker-target-discovery hang).
- **stalled-receiver:** blackhole/sink accepts but never reads (full socket buffer) and slow HTTP
  responses; assert bounded shutdown + no 100% CPU busy-spin (unix_stream partial-write, http acquire
  backpressure).
- **multi-generator-config:** two Http generators and two SplunkHec generators in one config; assert
  no OnceCell double-set panic and independent connection limits.
- **small-and-zero-block-size:** `maximum_block_size` tiny and `'0 B'` with dogstatsd/otel/trace_agent;
  assert bounded block-cache construction and no `random_range` panic.
- **oversized-block-vs-capacity:** `maximum_block_size` in `(bps/N, bps]` with `parallel_connections>1`;
  assert nonzero delivery, no zero-delivery busy loop, `blocks_discarded>0` on true rejection (divide
  stall / capacity livelock).
- **linear-ramp-parallel:** Linear throttle + `parallel_connections>1`; sample aggregate delivered
  rate over warmup; assert slope == configured `rate_of_change`.
- **determinism-replay:** same seed+config run twice; assert byte-identical load at the sink (and
  under a perturbed clock, identical payload timestamps).
- **node-termination-mid-run:** SIGKILL the whole lading container mid-experiment; assert jsonl
  surviving prefix validates and any readable parquet is internally consistent (capture
  crash-consistency).
- **sigterm-stop:** send SIGTERM to lading (deployment orchestrator-stop path); assert graceful drain,
  parquet footer written, no orphaned children.
- **capture-write-fault:** inject disk-full/EIO on a capture flush tick; assert clean fatal exit with
  finalized parquet, no SIGABRT (panic hook silent).
- **target-lifecycle-races:** target that exits before sending its PID, vanishes mid-listing, or whose
  PID is recycled; assert observer degrades (no `assert!`/`expect` panic) and does not attribute
  impostor metrics.
- **blackhole-restart-stale-socket:** leave a stale unix_datagram socket then restart the blackhole;
  assert bind succeeds.
- **accept-fault-injection:** fd exhaustion / transient accept errors at the datadog (and other)
  blackholes; assert the blackhole keeps serving the target (no wedge).
- **slow-capture-drain:** throttle the capture manager drain; assert the datadog blackhole responds to
  the target within a bounded latency (no channel-full backpressure).
- **malformed-config:** bad addr/target_uri, out-of-range `compression_level`, `sample_period` 0,
  dogstatsd `tag_length.end()<=MIN_TAG_LENGTH`; assert clean startup errors, no panic.
- **split-mode-partial-kill:** two-instance split (sender `--no-target` + receiver observer); kill only
  one side; assert merge/attribution is well-defined and a clean side's captures are not needlessly
  lost.
- **tcp_rr-threads>1-bind-conflict:** pre-bind the data port with `num_threads>1`; assert
  `Error::Bind`, no panic on the main async task.
