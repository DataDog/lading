# Broad Config Coverage + Adversarial Receiver Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the `general` Antithesis scenario randomly cover most of lading's config surface and target-interaction failure modes, so config/startup-crash bugs and target-misbehavior bugs surface without per-bug scenarios.

**Architecture:** Two evolutions of the existing `general` scenario. (1) The per-timeline config sampler (`harness/src/config.rs`) grows from a single fixed `tcp` generator into a multi-generator sampler whose value menus include boundary values that crash lading at startup; the existing panic hook turns any such crash into an `unreachable!` finding. (2) A new `adversarial-receiver` component (separate from the pure `sink` oracle) speaks the HTTP protocols lading generates and returns a per-timeline sampled adversarial response (2xx / 429-503 / stall / reset / garbage-200), so target-interaction bugs surface. A SUT-side assertion on the `trace_agent` backoff loop catches the retry storm regardless of shutdown masking, and `shutdown-safety` reuses the sampler so the storm's drain impact is measured under the existing bounded-drain oracle.

**Tech Stack:** Rust 2024, tokio, hyper 1.x (server), the Antithesis SDK via the `lading_antithesis` facade, `serde_yaml`, Docker Compose, snouty.

## Global Constraints

- US-ASCII only in all code, comments, and docs.
- No panics in harness/receiver code: return `Result`, no `unwrap`/`expect`/`panic!`. (The sampler deliberately emits configs that crash *lading* at runtime -- that is data the SUT mishandles, not a panic in harness code.)
- No `std::collections::HashMap`; use `IndexMap`/`IndexSet` if a map is needed.
- Workspace clippy is deny-all + pedantic + `unwrap_used`; every crate sets `[lints] workspace = true`.
- TDD: write the failing test first, watch it fail, implement minimally, watch it pass, commit.
- No Jira/SMPTNG keys anywhere in source, including assertion messages.
- Antithesis assertion messages are self-documenting full statements of the property.
- Structured sampler choices draw from `AntithesisRng` (so Antithesis branches them); lading's payload `seed` draws from system entropy, never from `AntithesisRng`.
- Determinism: the sampler is a pure function of its `rng`; no wall-clock or ambient randomness beyond the documented `seed` draw.
- Do not commit or launch; hold for the user.

## File Structure

- `test/antithesis/adversarial-receiver/` (NEW crate) -- the adversarial HTTP receiver. `src/mode.rs` holds the pure, unit-tested behavior selection; `src/main.rs` is the hyper server wiring. Split so the decision logic is testable without a socket.
- `test/antithesis/harness/src/config.rs` (MODIFY) -- grows a `Generator` enum, a `TimelinePlan`, `sample_plan`, and a multi-generator `to_yaml`.
- `test/antithesis/harness/src/bin/first_sample_config.rs` (MODIFY) -- writes the receiver mode alongside `lading.yaml`.
- `test/antithesis/scenarios/general/` (MODIFY) -- `Dockerfile` gains an `adversarial-receiver` stage/target; `docker-compose.yaml` gains the service; `lading-entrypoint.sh` unchanged.
- `lading/src/generator/trace_agent.rs` (MODIFY) -- SUT-side assertion on the backoff loop.
- `test/antithesis/scenarios/shutdown-safety/` (MODIFY) -- sample the stressor config instead of hardwiring one.
- `Cargo.toml` (workspace) (MODIFY) -- add the `adversarial-receiver` member.

---

## Part 1 -- Adversarial receiver component

### Task 1: Receiver mode logic (pure, tested)

**Files:**
- Create: `test/antithesis/adversarial-receiver/Cargo.toml`
- Create: `test/antithesis/adversarial-receiver/src/mode.rs`
- Create: `test/antithesis/adversarial-receiver/src/lib.rs`
- Modify: `Cargo.toml` (workspace `members`)

**Interfaces:**
- Produces: `enum Mode { Ok, Retryable, Stall, Reset, GarbageOk }`; `Mode::from_env_or_file(default: Mode) -> Mode`; `Mode::parse(&str) -> Mode` (unknown -> `Ok`); `Mode::status(self) -> u16` (`Ok`/`Stall`/`GarbageOk` -> 200, `Retryable` -> 503); `Mode::body(self) -> &'static [u8]`.

- [ ] **Step 1: Add the crate to the workspace members** in `Cargo.toml` (workspace), alphabetically near the other `test/antithesis/*` members:

```toml
    "test/antithesis/adversarial-receiver",
```

- [ ] **Step 2: Write `Cargo.toml` for the crate**

```toml
[package]
name = "adversarial-receiver"
version = "0.1.0"
edition = "2024"
license = "MIT"
publish = false
description = "Adversarial HTTP receiver for the lading Antithesis general scenario."

[[bin]]
name = "adversarial-receiver"
path = "src/main.rs"

[lib]
doctest = false

[lints]
workspace = true

[dependencies]
lading-antithesis = { workspace = true, features = ["antithesis"] }
anyhow = { workspace = true }
tokio = { workspace = true, features = ["io-util", "macros", "net", "rt-multi-thread", "signal", "time"] }
hyper = { workspace = true, features = ["server", "http1"] }
hyper-util = { workspace = true, features = ["tokio", "server"] }
http-body-util = { workspace = true }
bytes = { workspace = true }
tracing = { workspace = true }
tracing-subscriber = { workspace = true, features = ["env-filter", "fmt"] }

[dev-dependencies]
proptest = { workspace = true }
```

(If `hyper`/`hyper-util`/`http-body-util`/`bytes` are not yet `[workspace.dependencies]`, they are already used by `lading`; add them to the workspace `[workspace.dependencies]` mirroring lading's versions.)

- [ ] **Step 3: Write `src/lib.rs`**

```rust
//! Adversarial HTTP receiver: pure behavior selection plus the server binary.
//!
//! The `mode` module is the decision logic, unit-tested without a socket; the
//! binary (`main.rs`) wires it to hyper.
pub mod mode;
```

- [ ] **Step 4: Write the failing test in `src/mode.rs`**

```rust
//! Per-timeline adversarial response behavior for the receiver.
//!
//! The mode is chosen once per timeline by the workload's config sampler and
//! written to the shared volume; the receiver reads it and applies it to every
//! request. Keeping selection pure makes it testable without a socket.

/// The adversarial behavior the receiver applies to every request this timeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Answer 200 with an empty body. The benign baseline.
    Ok,
    /// Answer 503 on every request. Drives retry/backoff paths.
    Retryable,
    /// Accept, then never respond. Drives client stall/timeout paths.
    Stall,
    /// Drop the connection without any response. Drives reset handling.
    Reset,
    /// Answer 200 with a body that is not any expected schema. Drives strict
    /// response-body parsing paths.
    GarbageOk,
}

impl Mode {
    /// Parse a mode token; any unknown token is the benign `Ok`.
    #[must_use]
    pub fn parse(s: &str) -> Self {
        match s.trim() {
            "retryable" => Self::Retryable,
            "stall" => Self::Stall,
            "reset" => Self::Reset,
            "garbage_ok" => Self::GarbageOk,
            _ => Self::Ok,
        }
    }

    /// HTTP status for a mode that returns a response.
    #[must_use]
    pub fn status(self) -> u16 {
        match self {
            Self::Retryable => 503,
            Self::Ok | Self::Stall | Self::GarbageOk => 200,
        }
    }

    /// Response body for a mode that returns a body.
    #[must_use]
    pub fn body(self) -> &'static [u8] {
        match self {
            Self::GarbageOk => b"not-a-known-schema",
            _ => b"",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Mode;

    #[test]
    fn parse_known_and_unknown() {
        assert_eq!(Mode::parse("retryable"), Mode::Retryable);
        assert_eq!(Mode::parse("stall"), Mode::Stall);
        assert_eq!(Mode::parse("reset"), Mode::Reset);
        assert_eq!(Mode::parse("garbage_ok"), Mode::GarbageOk);
        assert_eq!(Mode::parse("ok"), Mode::Ok);
        assert_eq!(Mode::parse("whatever"), Mode::Ok, "unknown -> benign");
        assert_eq!(Mode::parse("  retryable\n"), Mode::Retryable, "trimmed");
    }

    #[test]
    fn retryable_is_503_others_200() {
        assert_eq!(Mode::Retryable.status(), 503);
        assert_eq!(Mode::Ok.status(), 200);
        assert_eq!(Mode::GarbageOk.status(), 200);
    }

    #[test]
    fn garbage_ok_has_nonempty_body() {
        assert!(!Mode::GarbageOk.body().is_empty());
        assert!(Mode::Ok.body().is_empty());
    }
}
```

- [ ] **Step 5: Run the test, expect FAIL (crate does not build yet / no bin)**

Run: `cargo test -p adversarial-receiver --lib`
Expected: FAIL (missing `src/main.rs` for the `[[bin]]`, or unresolved deps).

- [ ] **Step 6: Add a minimal `src/main.rs` stub so the crate builds, then re-run**

```rust
//! Placeholder; real server wired in Task 2.
fn main() {}
```

Run: `cargo test -p adversarial-receiver --lib`
Expected: PASS (3 tests).

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml test/antithesis/adversarial-receiver
git commit -m "feat(antithesis): adversarial-receiver mode logic"
```

### Task 2: Receiver server (hyper wiring + mode source)

**Files:**
- Modify: `test/antithesis/adversarial-receiver/src/mode.rs` (add `from_env_or_file`)
- Modify: `test/antithesis/adversarial-receiver/src/main.rs`

**Interfaces:**
- Consumes: `Mode` from Task 1.
- Produces: a binary listening on `RECEIVER_LISTEN_ADDR` (default `0.0.0.0:8080`) that reads its mode from `RECEIVER_MODE_PATH` (default `/shared/receiver_mode`) at startup, falling back to `Ok` if absent, and applies it to every request.

- [ ] **Step 1: Add `from_env_or_file` to `mode.rs` with a failing test**

```rust
// add near the bottom of the impl block:
    /// Resolve the timeline's mode from a file path (one token). A missing or
    /// unreadable file yields the benign `Ok`, so the receiver serves normally
    /// until the sampler has written a mode.
    #[must_use]
    pub fn from_file(path: &std::path::Path) -> Self {
        match std::fs::read_to_string(path) {
            Ok(s) => Self::parse(&s),
            Err(_) => Self::Ok,
        }
    }
```

```rust
// add to `mod tests`:
    #[test]
    fn from_file_missing_is_ok() {
        let p = std::path::Path::new("/nonexistent/receiver_mode");
        assert_eq!(Mode::from_file(p), Mode::Ok);
    }
```

- [ ] **Step 2: Run, expect FAIL then PASS after adding the method**

Run: `cargo test -p adversarial-receiver --lib`
Expected: PASS (4 tests).

- [ ] **Step 3: Write the server in `src/main.rs`** (mirror the hyper 1.x serving boilerplate in `lading/src/blackhole/http.rs`: `hyper_util::server::conn::auto` or `hyper::server::conn::http1` + `TokioIo`, one task per accepted connection, `tokio::select!` on `ctrl_c`)

```rust
//! Adversarial HTTP receiver binary for the lading Antithesis general scenario.
//!
//! Reads its timeline mode from the shared volume and answers every request
//! per that mode: 200, 503, stall (never respond), reset (drop), or a 200 with
//! an off-schema body. It is a supporting harness component, not the system
//! under test, so it is built without coverage instrumentation.

use std::{convert::Infallible, net::SocketAddr, path::PathBuf, sync::Arc};

use adversarial_receiver::mode::Mode;
use anyhow::Context as _;
use bytes::Bytes;
use http_body_util::Full;
use hyper::{Request, Response, StatusCode, body::Incoming, service::service_fn};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

const DEFAULT_LISTEN_ADDR: &str = "0.0.0.0:8080";
const DEFAULT_MODE_PATH: &str = "/shared/receiver_mode";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();
    lading_antithesis::init();

    let addr: SocketAddr = std::env::var("RECEIVER_LISTEN_ADDR")
        .unwrap_or_else(|_| DEFAULT_LISTEN_ADDR.to_string())
        .parse()
        .context("parse RECEIVER_LISTEN_ADDR")?;
    let mode_path: PathBuf = std::env::var_os("RECEIVER_MODE_PATH")
        .map_or_else(|| PathBuf::from(DEFAULT_MODE_PATH), PathBuf::from);
    let mode = Mode::from_file(&mode_path);
    info!("adversarial-receiver listening on {addr} in mode {mode:?}");

    let listener = TcpListener::bind(addr).await.context("bind receiver")?;
    let mode = Arc::new(mode);

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _peer) = match accepted { Ok(v) => v, Err(e) => { error!("accept: {e}"); continue; } };
                let mode = Arc::clone(&mode);
                tokio::spawn(async move {
                    // Reset: drop the freshly accepted connection with no response.
                    if *mode == Mode::Reset { return; }
                    let io = TokioIo::new(stream);
                    let svc = service_fn(move |req| handle(req, *mode));
                    if let Err(e) = hyper::server::conn::http1::Builder::new().serve_connection(io, svc).await {
                        error!("serve_connection: {e}");
                    }
                });
            }
            _ = tokio::signal::ctrl_c() => { info!("shutdown, receiver exiting"); break; }
        }
    }
    Ok(())
}

async fn handle(_req: Request<Incoming>, mode: Mode) -> Result<Response<Full<Bytes>>, Infallible> {
    // The claim: lading actually reached the receiver with a request.
    lading_antithesis::sometimes!(true, "adversarial receiver received a request");
    if mode == Mode::Stall {
        // Accept the request and never answer, exercising client stall/timeout.
        std::future::pending::<()>().await;
    }
    let status = StatusCode::from_u16(mode.status()).unwrap_or(StatusCode::OK);
    let resp = Response::builder()
        .status(status)
        .body(Full::new(Bytes::from_static(mode.body())))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::new())));
    Ok(resp)
}
```

(Note: the two `unwrap_or_else`/`unwrap_or` above are total -- they cannot panic because `mode.status()` is always a valid code and the body builder cannot fail on a static body -- but they are written as fallbacks to satisfy `unwrap_used`. If clippy still flags them, replace with explicit `match`.)

- [ ] **Step 4: Build and smoke-test locally**

```bash
cargo build -p adversarial-receiver
RECEIVER_MODE_PATH=/dev/null RECEIVER_LISTEN_ADDR=127.0.0.1:18080 target/debug/adversarial-receiver &
sleep 1
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:18080/   # expect 200
printf 'retryable' > /tmp/rmode; kill %1
RECEIVER_MODE_PATH=/tmp/rmode RECEIVER_LISTEN_ADDR=127.0.0.1:18080 target/debug/adversarial-receiver &
sleep 1
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:18080/   # expect 503
kill %1
```

Expected: `200` then `503`.

- [ ] **Step 5: clippy + commit**

```bash
cargo clippy -p adversarial-receiver --all-targets
git add test/antithesis/adversarial-receiver
git commit -m "feat(antithesis): adversarial-receiver hyper server"
```

### Task 3: Wire the receiver into the general scenario

**Files:**
- Modify: `test/antithesis/scenarios/general/Dockerfile`
- Modify: `test/antithesis/scenarios/general/docker-compose.yaml`

- [ ] **Step 1: Add a build + runtime stage to the Dockerfile.** In `tools-builder`, add the receiver build alongside sink/harness:

```dockerfile
    cargo build --release --package adversarial-receiver --bin adversarial-receiver && \
    cp /tools/target/release/adversarial-receiver /usr/local/bin/adversarial-receiver && \
```

Add a runtime target after the `sink` target:

```dockerfile
FROM debian:bookworm-slim AS adversarial-receiver
ENV NO_COLOR=1
RUN apt-get update && apt-get install --no-install-recommends -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=tools-builder /usr/local/bin/adversarial-receiver /usr/local/bin/adversarial-receiver
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/adversarial-receiver"]
```

- [ ] **Step 2: Add the service to `docker-compose.yaml`** (reads the shared volume for its mode; healthchecked so lading waits for it):

```yaml
  adversarial-receiver:
    container_name: adversarial-receiver
    hostname: adversarial-receiver
    platform: linux/amd64
    init: true
    build:
      context: ../../../..
      dockerfile: test/antithesis/scenarios/general/Dockerfile
      target: adversarial-receiver
    image: adversarial-receiver:${ANTITHESIS_IMAGE_TAG:-latest}
    environment:
      NO_COLOR: "1"
    volumes:
      - shared:/shared:ro
    healthcheck:
      test: ["CMD-SHELL", "bash -c 'exec 3<>/dev/tcp/localhost/8080'"]
      interval: 2s
      timeout: 2s
      retries: 30
```

Add `adversarial-receiver: {condition: service_healthy}` to lading's and workload's `depends_on`.

- [ ] **Step 3: Build the scenario, expect success**

Run: `docker compose -f test/antithesis/scenarios/general/docker-compose.yaml build`
Expected: all four images build.

- [ ] **Step 4: Commit**

```bash
git add test/antithesis/scenarios/general/Dockerfile test/antithesis/scenarios/general/docker-compose.yaml
git commit -m "feat(antithesis): add adversarial-receiver to general scenario"
```

---

## Part 2 -- Broaden the config sampler

### Task 4: Multi-generator plan type + `to_yaml`

**Files:**
- Modify: `test/antithesis/harness/src/config.rs`
- Modify: `test/antithesis/harness/Cargo.toml` (add `hyper` for `Uri`/`Method`, `http` for `HeaderMap` -- both already workspace deps via lading)

**Interfaces:**
- Produces: `enum Generator { Tcp(tcp::Config), TraceAgent(trace_agent::Config), Http(http::Config) }`; `struct TimelinePlan { generators: Vec<Generator>, receiver_mode: &'static str, label: String }`; `fn sample_plan<R: Rng>(rng: &mut R) -> TimelinePlan`; `fn plan_to_yaml(generators: &[Generator]) -> Result<String, serde_yaml::Error>`.

- [ ] **Step 1: Add deps to `harness/Cargo.toml`**

```toml
hyper = { workspace = true }
http = { workspace = true }
```

- [ ] **Step 2: Write the failing test first** (append to `config.rs` tests) -- the load-bearing invariant is unchanged: everything sampled must parse as a lading config, across many seeds, for every generator kind the menu can produce.

```rust
    #[test]
    fn every_sampled_plan_parses_as_lading_config() {
        for s in 0..1024_u64 {
            let mut rng = StdRng::seed_from_u64(s);
            let plan = super::sample_plan(&mut rng);
            let yaml = super::plan_to_yaml(&plan.generators).expect("serialize plan");
            let parsed: Result<lading::config::Config, _> = serde_yaml::from_str(&yaml);
            assert!(parsed.is_ok(), "seed {s} rejected:\n{yaml}\n{:?}", parsed.err());
        }
    }

    #[test]
    fn menu_reaches_every_generator_kind_and_boundary() {
        // Prove the crash/interaction boundaries are actually reachable, so the
        // scenario is not silently benign. Requires the trace_agent contexts=0
        // crash config, the duplicate-http config, and the retryable mode to all
        // appear across a seed sweep.
        let mut saw_ctx0 = false;
        let mut saw_dup_http = false;
        let mut saw_retryable = false;
        for s in 0..2048_u64 {
            let mut rng = StdRng::seed_from_u64(s);
            let plan = super::sample_plan(&mut rng);
            if plan.label.contains("trace_agent:contexts0") { saw_ctx0 = true; }
            if plan.generators.iter().filter(|g| matches!(g, super::Generator::Http(_))).count() >= 2 { saw_dup_http = true; }
            if plan.receiver_mode == "retryable" { saw_retryable = true; }
        }
        assert!(saw_ctx0, "contexts=0 boundary never sampled");
        assert!(saw_dup_http, "duplicate-http boundary never sampled");
        assert!(saw_retryable, "retryable receiver mode never sampled");
    }
```

- [ ] **Step 3: Run, expect FAIL (symbols missing)**

Run: `cargo test -p harness --lib`
Expected: FAIL (no `sample_plan`, `plan_to_yaml`, `Generator`).

- [ ] **Step 4: Implement the plan types and `plan_to_yaml`** (replace the tcp-only `to_yaml`; keep `sample` for now or delete once `first_sample_config` switches in Task 6). Add at the top of `config.rs`:

```rust
use lading::generator::{http, tcp, trace_agent};
use lading_payload::trace_agent as ta_payload;
use lading_payload::common::config::ConfRange;

/// One sampled lading generator, typed against lading's own config structs so
/// the value menu cannot drift from the real schema.
pub enum Generator {
    Tcp(tcp::Config),
    TraceAgent(trace_agent::Config),
    Http(http::Config),
}

impl Generator {
    fn key(&self) -> &'static str {
        match self {
            Generator::Tcp(_) => "tcp",
            Generator::TraceAgent(_) => "trace_agent",
            Generator::Http(_) => "http",
        }
    }
    fn value(&self) -> Result<serde_yaml::Value, serde_yaml::Error> {
        match self {
            Generator::Tcp(c) => serde_yaml::to_value(c),
            Generator::TraceAgent(c) => serde_yaml::to_value(c),
            Generator::Http(c) => serde_yaml::to_value(c),
        }
    }
}

/// The full per-timeline plan: the generator set lading boots under, plus the
/// adversarial mode the receiver serves this timeline.
pub struct TimelinePlan {
    pub generators: Vec<Generator>,
    pub receiver_mode: &'static str,
    pub label: String,
}

/// Serialize a generator set into the `generator: [ { key: value }, ... ]` YAML
/// lading parses.
///
/// # Errors
/// Returns an error if a generator config fails to serialize.
pub fn plan_to_yaml(generators: &[Generator]) -> Result<String, serde_yaml::Error> {
    let mut seq = Vec::with_capacity(generators.len());
    for g in generators {
        let mut item = serde_yaml::Mapping::new();
        item.insert(serde_yaml::Value::from(g.key()), g.value()?);
        seq.push(serde_yaml::Value::Mapping(item));
    }
    let mut top = serde_yaml::Mapping::new();
    top.insert(serde_yaml::Value::from("generator"), serde_yaml::Value::Sequence(seq));
    serde_yaml::to_string(&serde_yaml::Value::Mapping(top))
}
```

(Confirm the `ConfRange` import path with `grep -rn "pub enum ConfRange" lading_payload/src`; adjust the `use` if it differs.)

- [ ] **Step 5: Implement `sample_plan`** (see Task 5 for the generator builders it calls). Minimal body to compile:

```rust
const RECEIVER_ADDR: &str = "adversarial-receiver:8080";

fn payload_seed() -> [u8; 32] {
    let mut seed = [0u8; 32];
    rand::rng().fill_bytes(&mut seed);
    seed
}

#[must_use]
pub fn sample_plan<R: Rng>(rng: &mut R) -> TimelinePlan {
    // Pick the generator family for this timeline. tcp -> sink (byte oracle);
    // trace_agent/http -> adversarial-receiver (interaction oracle).
    let kind = rng.random_range(0..4_u8);
    let receiver_mode = ["ok", "ok", "retryable", "stall", "garbage_ok", "reset"]
        .choose(rng).copied().unwrap_or("ok");
    match kind {
        0 => sample_tcp_plan(rng),
        1 => sample_trace_agent_plan(rng, receiver_mode),
        2 => sample_http_plan(rng, receiver_mode),
        _ => sample_dup_http_plan(rng, receiver_mode),
    }
}
```

- [ ] **Step 6: Run, expect FAIL until Task 5 adds the builders; then the parse + reachability tests pass**

Run: `cargo test -p harness --lib`
Expected after Task 5: PASS.

- [ ] **Step 7: Commit** (after Task 5).

### Task 5: Generator builders with boundary menus

**Files:**
- Modify: `test/antithesis/harness/src/config.rs`

**Interfaces:**
- Consumes: `Generator`, `TimelinePlan`, `RECEIVER_ADDR`, `payload_seed()`, the existing `variant_menu()`.
- Produces: `fn sample_tcp_plan`, `fn sample_trace_agent_plan`, `fn sample_http_plan`, `fn sample_dup_http_plan`.

- [ ] **Step 1: Implement `sample_tcp_plan`** (the existing tcp logic, now returning a plan against the sink; unchanged menus):

```rust
fn sample_tcp_plan<R: Rng>(rng: &mut R) -> TimelinePlan {
    let variant = variant_menu().choose(rng).cloned().unwrap_or(payload::Config::Ascii);
    let bps_mib = [1_u64, 5, 10, 50, 100].choose(rng).copied().unwrap_or(10);
    let bps = bps_mib * 1024 * 1024;
    let parallel_connections = rng.random_range(1..=8_u16);
    let per_conn = bps / u64::from(parallel_connections);
    let cfg = tcp::Config {
        seed: payload_seed(),
        addr: "sink:9000".to_string(),
        variant,
        bytes_per_second: Some(byte_unit::Byte::from_u64(bps)),
        maximum_block_size: byte_unit::Byte::from_u64(per_conn.clamp(1, 1024 * 1024)),
        maximum_prebuild_cache_size_bytes: byte_unit::Byte::from_u64(8 * 1024 * 1024),
        parallel_connections,
        throttle: None,
    };
    TimelinePlan { generators: vec![Generator::Tcp(cfg)], receiver_mode: "ok",
        label: format!("tcp:{}", variant_label(&variant)) }
}
```

- [ ] **Step 2: Implement `sample_trace_agent_plan`** -- the menu includes the `contexts=0` crash boundary (#4) and the `Ignore` backoff (#2):

```rust
fn sample_trace_agent_plan<R: Rng>(rng: &mut R, receiver_mode: &'static str) -> TimelinePlan {
    // contexts=0 is the empty-range crash boundary; the rest are benign.
    let (contexts, ctx_label) = match rng.random_range(0..3_u8) {
        0 => (ConfRange::Constant(0), "contexts0"),
        1 => (ConfRange::Constant(1), "contexts1"),
        _ => (ConfRange::Inclusive { min: 1, max: 50 }, "contextsN"),
    };
    let variant = ta_payload::Config::V04(ta_payload::v04::Config { contexts, ..Default::default() });
    let (backoff, bo_label) = if rng.random_bool_ratio(1, 2) {
        (trace_agent::BackoffBehavior::Ignore, "ignore")
    } else {
        (trace_agent::BackoffBehavior::Obey { max_retries: 3 }, "obey")
    };
    let uri = format!("http://{RECEIVER_ADDR}").parse().unwrap_or_else(|_| hyper::Uri::from_static("http://adversarial-receiver:8080"));
    let cfg = trace_agent::Config {
        seed: payload_seed(),
        target_uri: uri,
        backoff_behavior: backoff,
        variant,
        bytes_per_second: Some(byte_unit::Byte::from_u64(1024 * 1024)),
        maximum_block_size: byte_unit::Byte::from_u64(1024 * 1024),
        maximum_prebuild_cache_size_bytes: byte_unit::Byte::from_u64(8 * 1024 * 1024),
        block_cache_method: lading_payload::block::CacheMethod::Fixed,
        parallel_connections: rng.random_range(1..=4_u16),
        throttle: None,
    };
    TimelinePlan { generators: vec![Generator::TraceAgent(cfg)], receiver_mode,
        label: format!("trace_agent:{ctx_label}:{bo_label}") }
}
```

(`random_bool_ratio` is the rand 0.10 spelling; confirm with `grep -rn "fn random_bool" ~/.cargo` or use `rng.random_range(0..2) == 0`. Confirm `block::CacheMethod::Fixed` variant name with `grep -rn "enum CacheMethod" lading_payload/src`.)

- [ ] **Step 3: Implement `sample_http_plan` and `sample_dup_http_plan`** -- a single http generator (benign / stall via receiver mode) and the duplicate-http crash boundary (#9):

```rust
fn one_http(seed: [u8; 32]) -> http::Config {
    http::Config {
        seed,
        target_uri: hyper::Uri::from_static("http://adversarial-receiver:8080/"),
        method: http::Method::POST,
        headers: http::HeaderMap::new(),
        bytes_per_second: Some(byte_unit::Byte::from_u64(1024 * 1024)),
        maximum_block_size: byte_unit::Byte::from_u64(1024 * 1024),
        parallel_connections: 1,
        throttle: None,
    }
}
fn sample_http_plan<R: Rng>(_rng: &mut R, receiver_mode: &'static str) -> TimelinePlan {
    TimelinePlan { generators: vec![Generator::Http(one_http(payload_seed()))],
        receiver_mode, label: "http:single".to_string() }
}
fn sample_dup_http_plan<R: Rng>(_rng: &mut R, receiver_mode: &'static str) -> TimelinePlan {
    // Two http generators trip the process-global CONNECTION_SEMAPHORE double-set.
    TimelinePlan { generators: vec![Generator::Http(one_http(payload_seed())), Generator::Http(one_http(payload_seed()))],
        receiver_mode, label: "http:dup".to_string() }
}
```

(Confirm `http::Method`/`http::HeaderMap` are re-exported at those paths; lading serializes them via `http_serde`, so `serde_yaml::to_value` on the built config must round-trip. The `every_sampled_plan_parses` test proves it; if `HeaderMap`/`Method` fail to serialize under `http_serde`, fall back to building the http item as a raw `serde_yaml::Mapping` in `Generator::value`.)

- [ ] **Step 4: Run the harness tests, expect PASS**

Run: `cargo test -p harness --lib`
Expected: PASS, including `every_sampled_plan_parses_as_lading_config` and `menu_reaches_every_generator_kind_and_boundary`.

- [ ] **Step 5: clippy + commit**

```bash
cargo clippy -p harness --all-targets
git add test/antithesis/harness
git commit -m "feat(antithesis): sample trace_agent/http config boundaries in general"
```

### Task 6: `first_sample_config` writes the receiver mode

**Files:**
- Modify: `test/antithesis/harness/src/bin/first_sample_config.rs`

- [ ] **Step 1: Switch to `sample_plan` and write `receiver_mode` before `ready`:**

```rust
    let mut rng = rand::rand_core::UnwrapErr(antithesis_sdk::random::AntithesisRng);
    let plan = harness::config::sample_plan(&mut rng);
    let yaml = harness::config::plan_to_yaml(&plan.generators).context("serialize sampled plan")?;

    std::fs::write(dir.join("lading.yaml"), yaml.as_bytes()).context("write lading.yaml")?;
    std::fs::write(dir.join("receiver_mode"), plan.receiver_mode.as_bytes()).context("write receiver_mode")?;

    lading_antithesis::reachable!("first_sample_config sampled a timeline plan", { "plan": plan.label });

    std::fs::write(dir.join("ready"), b"ready\n").context("write ready sentinel")?;
```

- [ ] **Step 2: Build + clippy, expect success**

Run: `cargo clippy -p harness --all-targets && cargo build -p harness --bin first_sample_config`
Expected: clean.

- [ ] **Step 3: Rebuild the general images and `snouty validate`, expect success**

Run: `docker compose -f test/antithesis/scenarios/general/docker-compose.yaml build && snouty validate test/antithesis/scenarios/general --timeout 120`
Expected: "Setup validation successful." Then tear down: `docker compose -f test/antithesis/scenarios/general/docker-compose.yaml down -v`.

- [ ] **Step 4: Commit**

```bash
git add test/antithesis/harness/src/bin/first_sample_config.rs
git commit -m "feat(antithesis): first_sample_config writes receiver mode"
```

---

## Part 3 -- trace_agent backoff SUT assertion (robust #2)

### Task 7: Assert the retry loop is bounded / shutdown-aware

**Files:**
- Modify: `lading/src/generator/trace_agent.rs`
- Test: `lading/src/generator/trace_agent.rs` (existing `#[cfg(test)]`)

- [ ] **Step 1: Locate the resend loop** in `handle_request`/`spin` where `Backoff::wait` gates a resend on 429/503. Read it with `grep -n "wait()" lading/src/generator/trace_agent.rs`.

- [ ] **Step 2: Add a self-documenting assertion that the resend count stays bounded** at the top of each resend iteration (choose a generous ceiling, e.g. `MAX_RETRY_MILLIS`-independent `1_000`), so an unbounded `Ignore` storm trips it while a bounded `Obey` run does not:

```rust
// The retry loop must be bounded; an unbounded resend storm (e.g. Ignore on a
// persistent 429/503) never satisfies this and is caught even if runtime
// teardown happens to mask the exit hang.
lading_antithesis::always!(
    resend_attempts < 1_000,
    "trace_agent bounded its resend attempts against a rejecting target",
    { "resend_attempts": resend_attempts }
);
```

(Introduce a local `resend_attempts: u32` incremented each resend. This is instrumentation, not a behavior change.)

- [ ] **Step 3: Build both feature configs, expect clean**

Run: `cargo clippy -p lading --all-targets && cargo clippy -p lading --all-targets --features antithesis`
Expected: clean.

- [ ] **Step 4: Run trace_agent tests, expect PASS** (no behavior change)

Run: `cargo test -p lading generator::trace_agent`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add lading/src/generator/trace_agent.rs
git commit -m "feat(antithesis): assert trace_agent resend loop stays bounded"
```

---

## Part 4 -- shutdown-safety samples its stressor

### Task 8: Sample the stressor config under the bounded-drain oracle

**Files:**
- Modify: `test/antithesis/scenarios/shutdown-safety/` (add a `first_sample_config` to the workload; replace the hardwired `lading.yaml` with a sampled one) OR add a shutdown-oriented sampler entry.

- [ ] **Step 1: Decide the seam.** shutdown-safety currently bakes a fixed `lading.yaml`. To sample, give its workload the `first_sample_config` command (as general has) and drop the baked `lading.yaml` COPY; the entrypoint already blocks on `ready`.

- [ ] **Step 2: Add a `sample_plan` variant biased to drain-stressors** -- unreachable tcp (today), trace_agent+ignore vs a 503 receiver, http+stall. Reuse `sample_plan`; the shutdown-safety scenario simply needs the adversarial-receiver present too (add the service to its compose, or point trace_agent/http at an unreachable addr for the connect-storm subset).

- [ ] **Step 3: Build + `snouty validate` shutdown-safety, expect success; tear down.**

- [ ] **Step 4: Commit.**

(Task 8 is deliberately coarser: it depends on Parts 1-2 landing first and on a decision about whether shutdown-safety hosts its own receiver. Re-plan Task 8 concretely once Parts 1-2 are merged.)

---

## Extension catalog (follow-on, NOT specified as tasks here)

These reuse the machinery above; each is a menu entry or receiver behavior, added with the same TDD loop. They are listed so coverage gaps are explicit, not silently dropped:

- **#12 tcp_rr addr** -- add a `Generator::TcpRr(tcp_rr::Config)` menu entry with `addr` a hostname (`"adversarial-receiver"`), which crashes at `spin()`. Crash-only until a `tcp_rr` blackhole peer exists; document that.
- **#6 logrotate max_depth=0** -- `logrotate::Config` has private fields, so it cannot be typed-built from the harness; emit it as a raw `serde_yaml::Mapping` with `max_depth: 0`. Writes to a tmpfs, no receiver.
- **#31 --target-pid > i32::MAX** -- a CLI axis, not a YAML field. Sample it in the lading entrypoint (a value above `2147483647`), not in `config.rs`.
- **#5 splunk_hec strict-body** -- add a `Generator::SplunkHec` entry pointed at the receiver in `garbage_ok` mode (read `splunk_hec::Config` first).
- **#22 blackhole fatal-error** -- add a lading `blackhole` to the sampled config plus a client that stresses it (EMFILE/reset); distinct from the generator path.
- **#8 sqs, #24 datadog OOM, #20 httpd leak** -- long-run resource growth; need a memory/liveness oracle, not just the panic hook. Separate plan.

---

## Self-Review

- **Spec coverage:** #4 -> Task 5 (contexts=0 menu). #2 -> Task 5 (Ignore + retryable receiver) and Task 7 (bounded-resend assertion). #9 -> Task 5 (dup-http). #15 -> Task 2/3 (stall mode) + Task 5 (http). #5 -> receiver `garbage_ok` (Task 2), generator entry deferred to catalog. #6/#12/#31/#22 -> extension catalog (with the reason each is deferred). shutdown-safety stressor -> Task 8.
- **Placeholder scan:** the only intentionally-coarse task is Task 8 (depends on Parts 1-2) and the extension catalog (explicitly a roadmap, not tasks). All Part 1-3 steps carry real code.
- **Type consistency:** `Generator`, `TimelinePlan`, `sample_plan`, `plan_to_yaml`, `Mode`, `Mode::from_file`, `receiver_mode` string tokens (`ok`/`retryable`/`stall`/`reset`/`garbage_ok`) are used identically across the receiver, sampler, and `first_sample_config`.
- **Unverified API spellings flagged inline** (confirm before relying): `ConfRange` import path, `block::CacheMethod::Fixed`, `rng.random_bool_ratio`, `http::Method`/`http::HeaderMap` round-tripping under `http_serde`. Each has a fallback noted.
