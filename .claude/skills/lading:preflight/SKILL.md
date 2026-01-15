---
name: lading:preflight
description: Environment validation checklist. Run this FIRST when starting a new Claude session to verify the environment is ready for optimization work. Checks Rust toolchain, ci/ scripts, hyperfine, profilers, payloadtool, git, and database access.
---

# Pre-flight Checklist

**Run this skill FIRST when starting a new session.**

This validates that the environment is properly configured for optimization hunting, reviewing, and rescuing in lading.

---

## Execute All Checks

Run all these checks as one BASH script to minimize the number of tool calls. 
Each check should be done in order and use the Report Format below to report the result.

### Phase 1: Main Branch Sync

```bash
echo "=== Phase 1: Main Branch Sync ==="
git fetch origin
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
echo "Current branch: $CURRENT_BRANCH"
```
**Expected:** Shows current branch name
**Action:** Will check if at tip of main only if on main branch

```bash
if [ "$CURRENT_BRANCH" = "main" ]; then
    LOCAL_MAIN=$(git rev-parse HEAD)
    REMOTE_MAIN=$(git rev-parse origin/main)

    if [ "$LOCAL_MAIN" != "$REMOTE_MAIN" ]; then
        echo "FAIL: Not at tip of main branch"
        echo "Local main:  $LOCAL_MAIN"
        echo "Remote main: $REMOTE_MAIN"
        echo "Fix: git pull origin main"
        exit 1
    fi
    echo "At tip of main branch"
else
    echo "Not on main branch - skipping sync check"
fi
```
**Expected:** If on main, must be at tip of origin/main; if on another branch, check is skipped
**If fails:** Pull latest changes with `git pull origin main`

```bash
git status --short | head -10
DIRTY=$(git status --porcelain | wc -l | tr -d ' ')
if [ "$DIRTY" -gt 0 ]; then
    echo "WARN: Working tree has $DIRTY uncommitted changes"
    echo "Consider: git stash or git commit before starting optimization work"
else
    echo "Working tree: clean"
fi
```
**Expected:** Report working tree status; dirty is WARNING not failure
**Note:** Clean working tree is strongly recommended before starting optimization work

---

### Phase 2: Rust Toolchain

```bash
echo "=== Phase 2: Rust Toolchain ==="
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

### Phase 3: CI Scripts

```bash
echo "=== Phase 3: CI Scripts ==="
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

### Phase 4: Build Verification

```bash
echo "=== Phase 4: Build Verification ==="
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

### Phase 5: Benchmarking Tools

```bash
echo "=== Phase 5: Benchmarking Tools ==="
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
grep -rq "criterion" lading_payload/Cargo.toml 2>/dev/null && echo "criterion: found in lading_payload" || echo "WARN: criterion not found"
ls lading_payload/benches/*.rs 2>/dev/null | head -3 && echo "criterion benchmarks: present" || echo "WARN: no criterion benchmarks"
```
**Expected:** criterion found, benchmarks present in `lading_payload/benches/`
**Note:** criterion is for micro-benchmarks; proptest is for property tests (different purposes)

---

### Phase 6: Profiling Tools

```bash
echo "=== Phase 6: Profiling Tools ==="
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

### Phase 7: Memory Tools

```bash
echo "=== Phase 7: Memory Tools ==="
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

### Phase 8: Git Config

```bash
echo "=== Phase 8: Git Config ==="
git config user.name || echo "FAIL: git user.name not set"
git config user.email || echo "FAIL: git user.email not set"
```
**Expected:** Name and email configured
**If fails:** `git config user.name "Name"` and `git config user.email "email"`

### Phase 9: Print datetime

```bash
echo "=== Phase 9: Print date & time"
date
```
**Expected:** The date & time are printed out
**NOte:** This should never fail and if it does, something is seriously wrong.`

## Report Format

After running checks, output:

```
LADING PREFLIGHT REPORT
=======================
Timestamp: <current time in format 'YYYY-MM-DD HH:MM:ss'>
Status: PASS | FAIL
Legend: [*] present  [ ] missing (optional)  [X] failed

Phase 1 - Main Branch Sync:
  [*] At tip of main (or "Not on main - check skipped")
  [*] Working tree: clean (or "N uncommitted changes")

Phase 2 - Rust Toolchain:
  [*] rustc 1.XX.X
  [*] cargo 1.XX.X
  [*] cargo-nextest X.Y.Z

Phase 3 - CI Scripts:
  [*] ci/validate
  [*] ci/test
  [*] ci/check
  [*] ci/clippy
  [*] ci/fmt
  [*] ci/kani

Phase 4 - Build:
  [*] ci/check passes
  [*] payloadtool built

Phase 5 - Benchmarking:
  [*] hyperfine X.Y.Z
  [*] cargo-criterion X.Y.Z

Phase 6 - Profiling:
  [*] samply (or sample/perf)

Phase 7 - Memory:
  [*] payloadtool --memory-stats
  [ ] heaptrack (optional)
  [ ] valgrind (optional)

Phase 8 - Git Config:
  [*] user.name configured
  [*] user.email configured

Ready for: /lading:optimize:hunt, /lading:optimize:review, /lading:optimize:rescue
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

DO NOT proceed with optimization skills until all required checks pass.
```

---

## Usage

```
/lading:preflight             # Run full checklist
```

**Run this at the start of every new Claude session before any optimization work.**
