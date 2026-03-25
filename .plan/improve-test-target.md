# Improve duck: backport smp-test-target features

## Background

Comparison of `smp-test-target` (in the SMP repo at `testing/target/`) with lading's
duck + sheepdog integration testing (`integration/ducks/` + `integration/sheepdog/`).

---

## Systems at a Glance

| Aspect | smp-test-target | duck + sheepdog |
|---|---|---|
| Control plane | CLI flags only | gRPC over Unix socket (StartTest / GetMetrics / Shutdown RPCs) |
| Metrics exposure | Prometheus HTTP endpoint | gRPC `GetMetrics` RPC → structured types |
| Port allocation | Fixed/configured addresses | Random ports (`127.0.0.1:0`), returned via RPC |
| Protocols | HTTP/1.1, TCP, UDS (simultaneously) | HTTP, TCP, UDP (one at a time via enum) |
| Failure simulation | Normal / Crash / Hang behavior modes | None |
| Data quality | Not measured | Shannon entropy + DDSketch quantiles |
| Tests | `proptest` timing + `insta` snapshots | None inside duck |
| Orchestration | External (SMP system) | Sheepdog builds both binaries, runs load test, asserts |

---

## Where duck/sheepdog is already better

- **gRPC control plane** – runtime config, structured metric collection, dynamic port
  allocation; far more composable than CLI flags.
- **Sheepdog orchestration** – true end-to-end harness; SMP's target is driven
  externally.
- **Data quality** – Shannon entropy + DDSketch per protocol; SMP only counts bytes.
- **Port isolation** – `127.0.0.1:0` is safe for parallel test runs; SMP uses fixed
  addresses.

---

## Backports (priority order)

### 1. Behavior modes — `Normal / Crash / Hang` ← most impactful

smp-test-target's `behavior.rs` simulates target misbehavior so the *harness* can
be tested. Duck has none of this; sheepdog never exercises lading's reaction to a
failing target.

**Design:**

Add `BehaviorConfig` to `DucksConfig` in `integration/shared/src/lib.rs`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BehaviorConfig {
    Normal,
    Crash { delay_secs: u64 },
    Hang  { delay_secs: u64 },
}
```

Duck's `start_test()` spawns a behavior task alongside the listener. On `Hang`, the
behavior task drops its shutdown receiver so it must be explicitly aborted (mirror
smp-test-target's approach of aborting the task handle after the 5-second timeout).

**Files:**
- `integration/shared/src/lib.rs` — add `BehaviorConfig` to `DucksConfig`
- `integration/ducks/src/main.rs` — spawn behavior task; explicit abort on Hang
- `integration/sheepdog/src/lib.rs` — new test cases exercising Crash/Hang

**Reference:** `../single-machine-performance/testing/target/src/behavior.rs`

---

### 2. Shutdown timeout

smp-test-target enforces a 5-second timeout waiting for tasks to exit. Duck has no
timeout guard; a stuck task will block shutdown indefinitely.

**Design:** wrap duck's `JoinSet`/task-wait in
`tokio::time::timeout(Duration::from_secs(5), ...)`.

**Files:**
- `integration/ducks/src/main.rs` — shutdown path

---

### 3. Multiple simultaneous inputs

smp-test-target runs HTTP + TCP + UDS together. Duck's `ListenConfig` is a
single-variant enum so only one protocol can be active per test.

**Design:** replace the enum with a struct:

```rust
pub struct ListenConfig {
    pub http: bool,
    pub tcp:  bool,
    pub udp:  bool,
    pub uds:  Option<PathBuf>,  // see backport 4
}
```

**Files:**
- `integration/shared/src/lib.rs` — restructure `ListenConfig`
- `integration/ducks/src/main.rs` — spin up all enabled listeners
- `integration/sheepdog/src/lib.rs` — update test configs

---

### 4. UDS input

smp-test-target has a Unix datagram socket input (`inputs/uds.rs`). Duck supports
HTTP / TCP / UDP but not UDS, despite lading being able to generate UDS traffic.

**Design:** add `uds_listen()` to duck; add `uds: Option<PathBuf>` to `ListenConfig`
(naturally pairs with backport 3); add `SocketMetrics` for UDS to the protobuf.

**Files:**
- `integration/ducks/src/main.rs` — `uds_listen()` function
- `integration/shared/src/lib.rs` — UDS in `ListenConfig` + `Metrics`
- `integration/shared/proto/integration_api.proto` — UDS in `Metrics` message

**Reference:** `../single-machine-performance/testing/target/src/inputs/uds.rs`

---

### 5. Internal tests for duck

smp-test-target uses `proptest` + Tokio time-pausing (`time::pause()` /
`time::advance()`) to validate behavior delay timing without real sleeps. Duck has
no internal tests at all.

Add `proptest` tests for behavior timing once backport 1 is in place.

**Files:**
- `integration/ducks/src/main.rs` (or a new `behavior.rs` module) — `#[cfg(test)]`
  block

---

## Verification

```
cargo build -p ducks
cargo build -p sheepdog
cargo test -p sheepdog          # existing tests still green
cargo test -p sheepdog crash    # new Crash behavior test
```

New sheepdog test goal: lading exits 0 even when duck exits 1 (Crash mode).
