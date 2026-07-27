# syntax=docker/dockerfile:1
#
# Antithesis harness image for the lading "shutdown-safety" scenario.
#
# Build context is the repository root. Named runtime targets:
#   - lading   : the instrumented lading binary (the system under test), sancov on
#   - workload : bakes the bounded-drain checker, emits setup_complete, then idles
#
# There is deliberately NO sink: the point of this scenario is a generator aimed
# at an UNREACHABLE address (connection refused -> busy reconnect), exercising
# whether lading drains promptly on graceful (experiment-timer) shutdown.
#
# lading is built native x86_64-unknown-linux-gnu (glibc).

# ---------------------------------------------------------------------------
# Shared build environment: Rust toolchain (pinned by rust-toolchain.toml) plus
# native build dependencies (protoc for prost/tonic, openssl for reqwest).
# ---------------------------------------------------------------------------
FROM debian:bookworm AS build-base
ENV DEBIAN_FRONTEND=noninteractive \
    NO_COLOR=1 \
    CARGO_TERM_COLOR=never
RUN apt-get update && \
    apt-get install --no-install-recommends -y \
        build-essential ca-certificates curl pkg-config libssl-dev cmake protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*
# Install rustup with no default toolchain; the pinned toolchain auto-installs on
# first cargo invocation inside the repo (rust-toolchain.toml).
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
        sh -s -- -y --profile minimal --default-toolchain none
ENV PATH="/root/.cargo/bin:${PATH}"

# ---------------------------------------------------------------------------
# Build the instrumented lading (the system under test).
#
# Coverage instrumentation uses the modern Antithesis Rust flow: the
# `antithesis-instrumentation` crate (referenced once in the binary behind the
# `antithesis` feature) provides the runtime shim, and these RUSTFLAGS enable
# LLVM sancov coverage. `--build-id` is required for symbolization. LTO is forced
# off for predictable sancov instrumentation, and split-debuginfo is forced off
# so the binary keeps embedded DWARF. The release profile already sets
# `panic = "abort"`, so any lading panic becomes SIGABRT, caught as a hard crash.
#
# The sancov RUSTFLAGS are passed via `--config target.<triple>.rustflags` with an
# explicit `--target`, NOT via the RUSTFLAGS env var. With an explicit target,
# Cargo builds host artifacts (build scripts, proc-macros) for the host and does
# NOT apply the target rustflags to them, so they are not instrumented and link
# cleanly.
# ---------------------------------------------------------------------------
FROM build-base AS lading-builder
ENV CARGO_PROFILE_RELEASE_LTO=off \
    CARGO_PROFILE_RELEASE_SPLIT_DEBUGINFO=off
WORKDIR /lading
COPY . /lading
RUN --mount=type=cache,target=/lading/target,id=antithesis-lading-target \
    --mount=type=cache,target=/root/.cargo/registry,id=cargo-registry \
    --mount=type=cache,target=/root/.cargo/git,id=cargo-git \
    cargo build --release --package lading --bin lading --features antithesis \
        --target x86_64-unknown-linux-gnu \
        --config 'target.x86_64-unknown-linux-gnu.rustflags=["-Ccodegen-units=1","-Cpasses=sancov-module","-Cllvm-args=-sanitizer-coverage-level=3","-Cllvm-args=-sanitizer-coverage-trace-pc-guard","-Clink-args=-Wl,--build-id"]' && \
    cp /lading/target/x86_64-unknown-linux-gnu/release/lading /usr/local/bin/lading && \
    echo "Validating Antithesis instrumentation symbols..." && \
    nm /usr/local/bin/lading | grep -q "antithesis_load_libvoidstar" && \
    nm /usr/local/bin/lading | grep -q "sanitizer_cov_trace_pc_guard" && \
    echo "Instrumentation symbols present."

# ---------------------------------------------------------------------------
# Build the bounded-drain checker, uninstrumented. It is a supporting harness
# component (a workload test command), not the system under test, so it needs no
# coverage instrumentation. There is no sink in this scenario.
# ---------------------------------------------------------------------------
FROM build-base AS tools-builder
WORKDIR /tools
COPY . /tools
RUN --mount=type=cache,target=/tools/target,id=antithesis-tools-target \
    --mount=type=cache,target=/root/.cargo/registry,id=cargo-registry \
    --mount=type=cache,target=/root/.cargo/git,id=cargo-git \
    cargo build --release --package harness --bin anytime_lading_drained_bounded && \
    cp /tools/target/release/anytime_lading_drained_bounded /usr/local/bin/anytime_lading_drained_bounded

# ---------------------------------------------------------------------------
# Runtime: instrumented lading (system under test).
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS lading
ENV NO_COLOR=1
RUN apt-get update && \
    apt-get install --no-install-recommends -y ca-certificates libssl3 && \
    rm -rf /var/lib/apt/lists/*
COPY --from=lading-builder /usr/local/bin/lading /usr/local/bin/lading
COPY --chmod=755 test/antithesis/scenarios/shutdown-safety/lading-entrypoint.sh /usr/local/bin/lading-entrypoint.sh
# Fixed config, baked in: this scenario does NOT sample configs, so the generator
# is hard-wired to an unreachable address.
COPY test/antithesis/scenarios/shutdown-safety/lading.yaml /etc/lading/lading.yaml
# Antithesis scans /symbols in every pushed image and matches by build-id.
RUN mkdir -p /symbols && ln -s /usr/local/bin/lading /symbols/lading
ENTRYPOINT ["/usr/local/bin/lading-entrypoint.sh"]

# ---------------------------------------------------------------------------
# Runtime: workload driver. Bakes the bounded-drain checker, emits
# setup_complete, then idles.
#
# The checker links only lading_antithesis + std (no lading, no openssl), so the
# slim base already carries everything it needs (libgcc_s, libc). No extra
# packages are installed.
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS workload
ENV NO_COLOR=1
COPY --chmod=755 test/antithesis/scenarios/shutdown-safety/workload/setup-complete.sh /opt/antithesis/setup-complete.sh
COPY --chmod=755 test/antithesis/scenarios/shutdown-safety/workload/entrypoint.sh /entrypoint.sh
# Antithesis test commands live here; the platform discovers and runs them from
# /opt/antithesis/test/v1/<template>/<command>. Checked-in files provide the
# template structure; the compiled command binary is injected below.
COPY --chmod=755 test/antithesis/scenarios/shutdown-safety/workload/test/ /opt/antithesis/test/
COPY --from=tools-builder --chmod=755 /usr/local/bin/anytime_lading_drained_bounded /opt/antithesis/test/v1/main/anytime_lading_drained_bounded
ENTRYPOINT ["/entrypoint.sh"]
