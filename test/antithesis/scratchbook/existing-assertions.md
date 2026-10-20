---
sut_path: /home/ssm-user/src/lading
commit: 51148899
updated: 2026-07-24T21:28:09Z
external_references:
  - name: the deployment (production runner + local dev harness)
    why: real shutdown/deploy model — how lading is launched, stopped, and how capture completeness is defined operationally; the source of truth for which lading termination paths are actually exercised.
  - name: Jira / Confluence (datadoghq.atlassian.net, project SMPTNG)
    why: existing bug tickets and design docs (SMPTNG-725 hang-in-spin, SMPTNG-719/697 ungraceful-termination telemetry loss, SMPTNG-694 compressed captures, SMPTNG-390 blackhole recorder, determinism/DST docs).
  - name: the whole lading repo (this checkout)
    why: the SUT itself — generators, throttle, capture, observer, target/inspector lifecycle, blackholes, and the lading_antithesis facade over antithesis_sdk.
---

# Existing Antithesis SDK assertions

Source of truth: the discovery-digest "Antithesis SDK instrumentation inventory" area
plus the property catalog. This file inventories every Antithesis SDK assertion that
**already exists** in the tree at commit `51148899`. It does not propose new ones.

## Summary

- The lading **SUT binary** (`lading/src`) carries **only bootstrap/plumbing probes** —
  there are **NO domain-level `always!`/`sometimes!`/`reachable!` invariants** in the
  production source beyond one bootstrap `reachable!` and the panic hook.
- All **domain invariants live in the workload** (`test/antithesis/`), not in the SUT:
  the sink load-arrival oracle and the capture crash-consistency checker.
- The primary no-panic enforcement is indirect: the panic hook reports any SUT panic as
  an `unreachable!` violation before `panic = "abort"` aborts the container.

## SUT-side sites (`lading/src`) — bootstrap / plumbing only

| Location | Kind | Message | Notes |
|---|---|---|---|
| `lading/src/antithesis_hooks.rs:27` | `unreachable!` (panic hook) | `"lading panicked"` details `{message, location}` | **The** no-panic ADR bridge. `set_hook` wraps the default hook, downcasts the payload to `&str`/`String`, reports to Antithesis, then forwards to the previous hook. Any reachable panic in lading = an Antithesis-visible `unreachable!` failure. `panic = "abort"` (Cargo.toml:115,120) makes every hit a hard, observable process abort. |
| `lading/src/antithesis_hooks.rs:14` | `init()` call | — | Forwards to `lading_antithesis::init()`, which calls `antithesis_sdk::antithesis_init()` under the `antithesis` feature (no-op otherwise). Installs the panic hook as early as possible. |
| `lading/src/bin/lading.rs:741` | `init()` call site | — | Sole caller of `lading::antithesis_hooks::init()`; runs in `main()` before the tokio runtime is built. |
| `lading/src/bin/lading.rs:792` | `reachable!` | `"lading completed bootstrap"` (no details map) | Bootstrap probe proving the SDK is linked and the instrumentation path is wired. Fires in `main()` after arg parsing, before the runtime is built. |

### The facade

- `lading_antithesis` is the single project facade over `antithesis_sdk` (owns the sole
  `antithesis` cargo feature). It exports the full macro family: `always!`,
  `always_or_unreachable!`, `sometimes!`, `reachable!`, `unreachable!`,
  `always_gt/ge/lt/le!`, `sometimes_gt/ge/lt/le!`, `always_some!`, `sometimes_all!`
  (`lading_antithesis/src/lib.rs:38-322`). Each macro has an enabled arm forwarding to
  the SDK and a disabled no-op arm that elides its args unevaluated. Details maps are
  wrapped via `serde_json::json`.

### NOT Antithesis instrumentation (do not confuse)

Three `std`-library `unreachable!` sites in `lading/src` are core `unreachable!` macros,
not SDK assertions (they carry no `lading_antithesis::` prefix). They are panic-on-reach
guards subject to the no-panic ADR and would be caught by the panic hook if hit:
- `lading/src/bin/lading.rs:347` (clap one-of guarantee)
- `lading/src/generator/splunk_hec/acknowledgements.rs:110` (`Channel::Ack` arm)
- `lading/src/blackhole/otlp/http.rs:227` and `:258` (path already validated)

## Workload-side sites (`test/antithesis/`) — all domain invariants

These are the real oracles. They run in workload/checker containers, not the SUT.

### Sink load-arrival oracle
| Location | Kind | Message | Notes |
|---|---|---|---|
| `test/antithesis/.../sink/src/main.rs:82` | `sometimes!` `(total > 0)` | `"sink received bytes"` | Load-arrival non-vacuity, fired per-connection read in the never-faulted sink container. Guards against a whole config class delivering nothing (divide stall, capacity livelock, throttle bypass). SDK init at `sink/main.rs:32`. |

### Capture crash-consistency checker (`anytime_capture_consistent.rs`)
`test/antithesis/harness/src/bin/anytime_capture_consistent.rs` — five SDK sites plus a
reachable anchor. `MIN_RECORDS = 10` non-vacuity floor; runs in the workload container.
| Line | Kind | Condition | Message |
|---|---|---|---|
| 44 | `always!` | `torn_before_final == 0` | `"jsonl capture has no torn record before the final line"` |
| 49 | `always!` | `invariants_hold` | `"jsonl capture fetch_index and per-series time stay monotonic"` |
| 54 | `sometimes!` | `parsed >= 10` | `"jsonl capture accumulated records across the run"` |
| 66 | `always!` | `!readable \|\| invariants_hold` | `"readable parquet capture is internally consistent"` |
| 71 | `sometimes!` | `readable && records >= 10` | `"parquet capture finalized and readable across the run"` |
| 82 | `reachable!` | (only when `checked_any`) | `"capture consistency checker validated a capture file"` |

Encodes the capture crash-consistency ADR: jsonl torn-final tolerated, parquet
unreadable-after-kill tolerated but a *readable* parquet must be internally consistent.

### Config-menu anchor
| Location | Kind | Message | Notes |
|---|---|---|---|
| `test/antithesis/harness/src/bin/first_sample_config.rs:36` | `reachable!` | `"first_sample_config sampled a config"` details `{variant}` | Per-timeline config-menu anchor. Draws from `AntithesisRng` (post-`setup_complete`) so Antithesis branches each config pick; counting these in triage shows how many variants were explored. This is the one intentional facade bypass — it names `antithesis_sdk::random::AntithesisRng` directly (`first_sample_config.rs:25`), since the facade covers only assertion macros + `init()`, not the RNG. |

## Coverage gaps in existing assertions (actionable)

Nothing beyond the bootstrap probes exists in the SUT source, so these P0/high-value
paths are currently observable **only** indirectly (via the panic hook or the external
capture checker), never asserted directly:
- **Shutdown/termination safety** — no SDK assertion instruments SIGTERM handling,
  `target_child.wait()` timeouts, `kill_on_drop`-on-signal, or the connect-loop hangs
  (tcp/udp/unix_stream/unix_datagram/grpc). These P0 invariants are visible only as a
  panic-hook `unreachable!` or an unreadable capture at the external checker.
- **Determinism** — no `always!` expresses "same seed => byte-identical load"; the
  harness relies on the sink byte counter and config sampling, not a direct byte-equality
  oracle.
- Whether the `antithesis` feature is compiled into the scenario build (Dockerfile) — so
  the enabled-arm macros actually link in — was not verified in the source scan.
