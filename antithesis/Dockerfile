# Antithesis harness image for the lading rig. One Dockerfile, three runtime
# targets selected by docker-compose:
#
#   lading          — the SUT, coverage-instrumented, production panic=abort profile
#   lading-probe    — purpose-built target/oracle skeleton
#   workload-client — test driver skeleton
#
# Build context is the repo root (see antithesis/config/docker-compose.yaml).
#
# Coverage instrumentation is a single unit: the antithesis-instrumentation
# crate (pulled in by each crate's `antithesis` feature) plus the sancov
# rustflags below. `-Wl,--build-id` is mandatory for symbolization.
#
# The rustflags are applied via `--config target.<triple>.rustflags` together
# with `build.target`, NOT via RUSTFLAGS. This scopes coverage to artifacts
# built for the target triple and excludes host build scripts and proc-macros
# (which do not link the sanitizer runtime and would otherwise fail with
# `undefined symbol: __sanitizer_cov_trace_pc_guard_init`). With an explicit
# target set, build binaries land under target/x86_64-unknown-linux-gnu/release.

ARG RUST_IMAGE=docker.io/rust:1.90.0-slim-bookworm
ARG RUNTIME_IMAGE=docker.io/debian:bookworm-20241202-slim

# ---------------------------------------------------------------------------
# lading (SUT) builder
# ---------------------------------------------------------------------------
FROM ${RUST_IMAGE} AS lading-build
ENV CARGO_INCREMENTAL=0
# Override the repo release profile for a tractable instrumented build: embed
# debug info (repo default is split/unpacked) and drop LTO (does not compose
# well with sancov and is not needed in-rig).
ENV CARGO_PROFILE_RELEASE_SPLIT_DEBUGINFO=off
ENV CARGO_PROFILE_RELEASE_LTO=false
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    protobuf-compiler \
    fuse3 \
    libfuse3-dev \
    libssl-dev \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release --locked --bin lading --features logrotate_fs,antithesis \
    --config 'build.target = "x86_64-unknown-linux-gnu"' \
    --config 'target.x86_64-unknown-linux-gnu.rustflags = ["-Ccodegen-units=1", "-Cpasses=sancov-module", "-Cllvm-args=-sanitizer-coverage-level=3", "-Cllvm-args=-sanitizer-coverage-trace-pc-guard", "-Clink-args=-Wl,--build-id"]'
# Fail the build if instrumentation did not link.
RUN nm target/x86_64-unknown-linux-gnu/release/lading | grep -q antithesis_load_libvoidstar

# ---------------------------------------------------------------------------
# lading-probe builder
# ---------------------------------------------------------------------------
FROM ${RUST_IMAGE} AS probe-build
ENV CARGO_INCREMENTAL=0
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /probe
COPY antithesis/lading-probe/ .
RUN cargo build --release --locked --features antithesis \
    --config 'build.target = "x86_64-unknown-linux-gnu"' \
    --config 'target.x86_64-unknown-linux-gnu.rustflags = ["-Ccodegen-units=1", "-Cpasses=sancov-module", "-Cllvm-args=-sanitizer-coverage-level=3", "-Cllvm-args=-sanitizer-coverage-trace-pc-guard", "-Clink-args=-Wl,--build-id"]'
RUN nm target/x86_64-unknown-linux-gnu/release/lading-probe | grep -q antithesis_load_libvoidstar

# ---------------------------------------------------------------------------
# workload-client builder
# ---------------------------------------------------------------------------
FROM ${RUST_IMAGE} AS client-build
ENV CARGO_INCREMENTAL=0
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*
# The client links `lading-throttle` by path to exercise the real `divide`, so
# the whole repo is the build context (the crate resolves `../../lading_throttle`
# and that crate's `[lints] workspace = true` against the repo-root workspace).
WORKDIR /src
COPY . .
WORKDIR /src/antithesis/workload-client
RUN cargo build --release --locked --features antithesis \
    --config 'build.target = "x86_64-unknown-linux-gnu"' \
    --config 'target.x86_64-unknown-linux-gnu.rustflags = ["-Ccodegen-units=1", "-Cpasses=sancov-module", "-Cllvm-args=-sanitizer-coverage-level=3", "-Cllvm-args=-sanitizer-coverage-trace-pc-guard", "-Clink-args=-Wl,--build-id"]'
RUN nm target/x86_64-unknown-linux-gnu/release/workload-client | grep -q antithesis_load_libvoidstar

# ---------------------------------------------------------------------------
# lading runtime (the SUT)
# ---------------------------------------------------------------------------
FROM ${RUNTIME_IMAGE} AS lading
ENV NO_COLOR=1
RUN apt-get update && apt-get install -y --no-install-recommends \
    fuse3 \
    libfuse3-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=lading-build /app/target/x86_64-unknown-linux-gnu/release/lading /usr/bin/lading
COPY antithesis/config/lading.yaml /etc/lading/lading.yaml
RUN mkdir -p /symbols && ln -s /usr/bin/lading /symbols/lading
# Smoke test.
RUN ["/usr/bin/lading", "--help"]
ENTRYPOINT ["/usr/bin/lading"]

# ---------------------------------------------------------------------------
# lading-probe runtime
# ---------------------------------------------------------------------------
FROM ${RUNTIME_IMAGE} AS lading-probe
ENV NO_COLOR=1
COPY --from=probe-build /probe/target/x86_64-unknown-linux-gnu/release/lading-probe /usr/bin/lading-probe
RUN mkdir -p /symbols && ln -s /usr/bin/lading-probe /symbols/lading-probe
ENTRYPOINT ["/usr/bin/lading-probe"]

# ---------------------------------------------------------------------------
# workload-client runtime
# ---------------------------------------------------------------------------
FROM ${RUNTIME_IMAGE} AS workload-client
ENV NO_COLOR=1
# The baseline driver spawns the real instrumented `lading` binary to observe a
# clean run (exit code + capture + wire bytes). `lading` is dynamically linked
# against libfuse3 (built with the logrotate_fs feature), so the client runtime
# needs the same shared libraries as the lading runtime.
RUN apt-get update && apt-get install -y --no-install-recommends \
    fuse3 \
    libfuse3-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=client-build /src/antithesis/workload-client/target/x86_64-unknown-linux-gnu/release/workload-client /usr/bin/workload-client
# The SUT binary the baseline driver runs. It is the same coverage-instrumented
# build as the `lading` service, so its coverage and bootstrap assertions are
# cataloged when a driver runs it.
COPY --from=lading-build /app/target/x86_64-unknown-linux-gnu/release/lading /usr/bin/lading
# Bake the test templates into the image so they are present hermetically in
# both `snouty validate` and real Antithesis runs.
COPY antithesis/test/v1/ /opt/antithesis/test/v1/
# Inject the compiled test-command binaries into the "main" test template.
COPY --from=client-build --chmod=755 /src/antithesis/workload-client/target/x86_64-unknown-linux-gnu/release/serial_driver_divide_throttle /opt/antithesis/test/v1/main/serial_driver_divide_throttle
COPY --from=client-build --chmod=755 /src/antithesis/workload-client/target/x86_64-unknown-linux-gnu/release/serial_driver_baseline_clean_run /opt/antithesis/test/v1/main/serial_driver_baseline_clean_run
RUN mkdir -p /symbols && ln -s /usr/bin/workload-client /symbols/workload-client && ln -s /usr/bin/lading /symbols/lading
ENTRYPOINT ["/usr/bin/workload-client"]
