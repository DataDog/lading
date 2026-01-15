# Benchmarking Reference

## Thresholds

Pass if ANY of:
- Time: >=5% improvement
- Memory: >=10% reduction
- Allocations: >=20% reduction

## Worktree Setup (one-time)

```bash
git worktree add ../lading-baseline main
```

Never use `git stash` or `git checkout` to switch. Baseline stays at `../lading-baseline`.

## Micro-benchmarks

```bash
# Baseline
cd ../lading-baseline && cargo criterion --bench default -- <filter>

# Optimized (your branch)
cargo criterion --bench default -- <filter>
```

Compare the "time:" lines. Criterion reports percentage changes.

## Macro-benchmarks

```bash
CONFIG=ci/fingerprints/<type>/lading.yaml

# Baseline
cd ../lading-baseline
cargo build --release --bin payloadtool
hyperfine --warmup 3 --runs 30 "./target/release/payloadtool $CONFIG"
./target/release/payloadtool "$CONFIG" --memory-stats

# Optimized
cargo build --release --bin payloadtool
hyperfine --warmup 3 --runs 30 "./target/release/payloadtool $CONFIG"
./target/release/payloadtool "$CONFIG" --memory-stats
```

## Determinism Check

```bash
./target/release/payloadtool "$CONFIG" --fingerprint > /tmp/run1.txt
./target/release/payloadtool "$CONFIG" --fingerprint > /tmp/run2.txt
diff /tmp/run1.txt /tmp/run2.txt  # Must be empty
```
