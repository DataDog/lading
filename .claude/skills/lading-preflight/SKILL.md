---
name: lading-preflight
description: Environment validation checklist. Run this FIRST when starting a new Claude session to verify the environment is ready for optimization work. Checks Rust toolchain, ci/ scripts, build, benchmarking tools, profilers, memory tools, and git state.
allowed-tools: Bash
---

# Pre-flight Checklist

Run each check. **STOP on any failure and report the issue.**

### Check 1: Rust Toolchain

```bash
rustc --version
```
**Expected:** `rustc 1.70.0` or newer (Rust 2024 edition requires 1.82+)
**If fails:** Install via rustup: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`

```bash
cargo --version
```
**Expected:** Version string like `cargo 1.XX.X`
**If fails:** Reinstall rustup

```bash
cargo nextest --version
```
**Expected:** Version string like `cargo-nextest nextest X.Y.Z`
**If fails:** `cargo install cargo-nextest`

---

### Check 2: CI Scripts

```bash
test -x ci/validate && echo "ci/validate executable" || echo "FAIL: ci/validate"
test -x ci/test && echo "ci/test executable" || echo "FAIL: ci/test"
test -x ci/check && echo "ci/check executable" || echo "FAIL: ci/check"
test -x ci/clippy && echo "ci/clippy executable" || echo "FAIL: ci/clippy"
test -x ci/fmt && echo "ci/fmt executable" || echo "FAIL: ci/fmt"
test -x ci/kani && echo "ci/kani present" || echo "WARN: ci/kani (optional for proofs)"
```
**Expected:** All scripts exist and are executable
**If fails:** Check you're in the lading repo root, run `chmod +x ci/*`

---

### Check 3: Build Verification

```bash
ci/check
```
**Expected:** Clean compilation with no errors
**If fails:** Check `cargo update`, may need dependency fixes

```bash
cargo build --release --bin payloadtool
```
**Expected:** Successful build, binary at `target/release/payloadtool`
**If fails:** Missing dependencies or Rust version too old

---

### Check 4: Benchmarking Tools

```bash
which hyperfine && hyperfine --version || echo "WARN: hyperfine not found"
```
**Expected:** Version like `hyperfine 1.X.X`
**If missing:** `cargo install hyperfine` or `brew install hyperfine`

```bash
cargo criterion --version 2>/dev/null || echo "WARN: cargo-criterion not found"
```
**Expected:** Version like `cargo-criterion 1.X.X`
**If missing:** `cargo install cargo-criterion`

```bash
which gnuplot > /dev/null 2>&1 && gnuplot --version || echo "WARN: gnuplot not found (needed for criterion graphs)"
```
**Expected:** Version like `gnuplot 5.X`
**If missing:** `brew install gnuplot` (macOS) or `apt install gnuplot` (Linux)

```bash
grep -rq "criterion" lading_payload/Cargo.toml 2>/dev/null && echo "criterion: found in lading_payload" || echo "WARN: criterion not found"
ls lading_payload/benches/*.rs 2>/dev/null | head -3 && echo "criterion benchmarks: present" || echo "WARN: no criterion benchmarks"
```
**Expected:** criterion found, benchmarks present in `lading_payload/benches/`
**Note:** criterion is for micro-benchmarks; proptest is for property tests (different purposes)

---

### Check 5: Profiling Tools

```bash
PROFILERS=""
which samply > /dev/null 2>&1 && PROFILERS="${PROFILERS}samply "
which cargo-flamegraph > /dev/null 2>&1 && PROFILERS="${PROFILERS}cargo-flamegraph "
which sample > /dev/null 2>&1 && PROFILERS="${PROFILERS}sample(macOS) "
which perf > /dev/null 2>&1 && PROFILERS="${PROFILERS}perf(linux) "

if [ -n "$PROFILERS" ]; then
    echo "Available profilers: $PROFILERS"
else
    echo "WARN: No profilers found"
    echo "Install one: cargo install samply OR cargo install flamegraph"
fi
```
**Expected:** At least one profiler available
**Recommended:**
- macOS: `cargo install samply` (best option) or use built-in `sample`
- Linux: `perf` (system package) or `cargo install samply`

---

### Check 6: Memory Tools

```bash
./target/release/payloadtool --help 2>&1 | grep -q "memory-stats" && echo "payloadtool --memory-stats: supported" || echo "WARN: payloadtool missing --memory-stats"
```
**Expected:** `payloadtool --memory-stats: supported`
**If fails:** Rebuild payloadtool from current branch

```bash
# Optional advanced memory tools
which heaptrack > /dev/null 2>&1 && echo "heaptrack: available" || echo "heaptrack: not found (optional)"
which valgrind > /dev/null 2>&1 && echo "valgrind: available" || echo "valgrind: not found (optional)"
```
**Expected:** Optional tools reported; not required for basic work
**Note:** `payloadtool --memory-stats` is the primary memory measurement tool

---

### Check 7: Git State

```bash
git config user.name || echo "FAIL: git user.name not set"
git config user.email || echo "FAIL: git user.email not set"
```
**Expected:** Name and email configured
**If fails:** `git config user.name "Name"` and `git config user.email "email"`

```bash
git status --short | head -10
DIRTY=$(git status --porcelain | wc -l | tr -d ' ')
if [ "$DIRTY" -gt 0 ]; then
    echo "WARN: Working tree has $DIRTY uncommitted changes"
else
    echo "Working tree: clean"
fi
```
**Expected:** Report status; dirty tree is a warning, not a failure
**Note:** Commit or stash changes before starting optimization work

## Report Format

After running checks, output:

```
LADING PREFLIGHT REPORT
=======================
Timestamp: <current time>
Status: PASS | FAIL

Check 1 - Rust Toolchain:
  [*] rustc 1.XX.X
  [*] cargo 1.XX.X
  [*] cargo-nextest X.Y.Z

Check 2 - CI Scripts:
  [*] ci/validate
  [*] ci/test
  [*] ci/check
  [*] ci/clippy
  [*] ci/fmt
  [*] ci/kani

Check 3 - Build:
  [*] ci/check passes
  [*] payloadtool built

Check 4 - Benchmarking:
  [*] hyperfine X.Y.Z
  [*] cargo-criterion X.Y.Z
  [ ] gnuplot X.Y.Z (optional)

Check 5 - Profiling:
  [*] samply (or sample/perf)

Check 6 - Memory:
  [*] payloadtool --memory-stats
  [ ] heaptrack (optional)

Check 7 - Git:
  [*] user.name configured
  [*] user.email configured
  [!] Working tree dirty (12 files)
```

Or if failed:

```
LADING PREFLIGHT REPORT
=======================
Status: FAIL

Failed checks:
  [X] cargo-nextest - not found
      Fix: cargo install cargo-nextest

  [X] ci/validate - not executable
      Fix: chmod +x ci/validate

DO NOT proceed until all required checks pass.
```

---
