# syntax=docker/dockerfile:1
#
# Antithesis harness image for the lading "general" scenario.
#
# Build context is the repository root. Named runtime targets:
#   - lading   : the instrumented lading binary (the system under test), sancov on
#   - sink     : the TCP byte-counting oracle, SDK-linked but uninstrumented
#   - workload : emits setup_complete then idles (the driver seam)
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
# Build the sink oracle, uninstrumented. It is a supporting harness component,
# not the system under test, so it needs no coverage instrumentation.
# ---------------------------------------------------------------------------
FROM build-base AS tools-builder
WORKDIR /tools
COPY . /tools
RUN --mount=type=cache,target=/tools/target,id=antithesis-tools-target \
    --mount=type=cache,target=/root/.cargo/registry,id=cargo-registry \
    --mount=type=cache,target=/root/.cargo/git,id=cargo-git \
    cargo build --release --package sink --bin sink && \
    cp /tools/target/release/sink /usr/local/bin/sink

# ---------------------------------------------------------------------------
# Runtime: instrumented lading (system under test).
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS lading
ENV NO_COLOR=1
RUN apt-get update && \
    apt-get install --no-install-recommends -y ca-certificates libssl3 && \
    rm -rf /var/lib/apt/lists/*
COPY --from=lading-builder /usr/local/bin/lading /usr/local/bin/lading
COPY test/antithesis/scenarios/general/lading.yaml /etc/lading/lading.yaml
# Antithesis scans /symbols in every pushed image and matches by build-id.
RUN mkdir -p /symbols && ln -s /usr/local/bin/lading /symbols/lading
# lading requires telemetry to be configured; --prometheus-addr satisfies it
# without the unbounded disk growth a capture file would incur over an infinite run.
ENTRYPOINT ["/usr/local/bin/lading", "--no-target", "--experiment-duration-infinite", "--prometheus-addr", "0.0.0.0:9102", "--config-path", "/etc/lading/lading.yaml"]

# ---------------------------------------------------------------------------
# Runtime: sink oracle (SDK-linked, uninstrumented).
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS sink
ENV NO_COLOR=1
RUN apt-get update && \
    apt-get install --no-install-recommends -y ca-certificates && \
    rm -rf /var/lib/apt/lists/*
COPY --from=tools-builder /usr/local/bin/sink /usr/local/bin/sink
EXPOSE 9000
ENTRYPOINT ["/usr/local/bin/sink"]

# ---------------------------------------------------------------------------
# Runtime: workload driver. Emits setup_complete, then idles.
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS workload
ENV NO_COLOR=1
COPY --chmod=755 test/antithesis/scenarios/general/workload/setup-complete.sh /opt/antithesis/setup-complete.sh
COPY --chmod=755 test/antithesis/scenarios/general/workload/entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
