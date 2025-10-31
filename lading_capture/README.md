# Lading Capture

A crate containing Lading's capture structures and serialization code.

## Fuzzing

This crate includes an AFL++ fuzzing harness that asserts capture file
validity. We do not use ci/fuzz as `CaptureManager` must install a global
singleton -- the Recorder -- and so we need a forking fuzzer.

### Setup

Install cargo-afl for AFL++ instrumentation:

```bash
cargo install cargo-afl
```

Build the fuzz harness with cargo-afl for proper instrumentation:

```bash
cargo afl build --release -p lading-capture --bin fuzz_capture_harness --features fuzz
```

Create a seeds directory with initial test cases:

```bash
mkdir -p seeds
```

Seed format (10 bytes):
- 8 bytes: seed (u64, little-endian) - seeds the RNG for operation generation
- 2 bytes: `runtime_secs` (u16, little-endian, 1-300) - how long to run the test

Examples:

```bash
# 2 second run
printf '\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00' > seeds/2sec.bin

# 5 second run
printf '\x01\x00\x00\x00\x00\x00\x00\x00\x05\x00' > seeds/5sec.bin

# 60 second run
printf '\x01\x00\x00\x00\x00\x00\x00\x00\x3c\x00' > seeds/60sec.bin
```

To run the fuzz rig:

```bash
cargo afl fuzz -t 5000 -i seeds/ -o findings/ target/release/fuzz_capture_harness
```
