# Update the rust version in-sync with the version in rust-toolchain.toml

# Refresh system packages before any build tools make TLS connections.
FROM docker.io/rust:1.99.1-slim-bookworm AS base
COPY ci/update-ca-certificates /usr/local/bin/lading-update-ca-certificates
RUN apt-get update && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends ca-certificates \
    && lading-update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Stage 0: Planner - Extract dependency metadata
FROM base AS planner
WORKDIR /app
RUN cargo install cargo-chef --version 0.1.78 --locked
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Stage 1: Cacher - Build dependencies only
FROM base AS cacher
ARG SCCACHE_BUCKET
ARG SCCACHE_REGION
ENV CARGO_INCREMENTAL=0
WORKDIR /app
RUN apt-get update && apt-get install -y \
    pkg-config=1.8.1-1 \
    libssl-dev=3.0.22-1~deb12u1 \
    protobuf-compiler=3.21.12-3+deb12u1 \
    fuse3=3.14.0-4 \
    libfuse3-dev=3.14.0-4 \
    curl \
    && rm -rf /var/lib/apt/lists/*
# Download pre-built sccache binary
RUN SCCACHE_VERSION=0.18.0 && \
    case "$(uname -m)" in \
        x86_64) ARCH=x86_64-unknown-linux-musl; SCCACHE_SHA256=45f1447fbe231e3037bde351ef70677dd212216c8d62ae7ca409fecc4d6acc89 ;; \
        aarch64) ARCH=aarch64-unknown-linux-musl; SCCACHE_SHA256=2b3284d5da3b46a47dc4229e75bb7b88ac4aa99c8d754fb7d2f84997e5a4354a ;; \
        *) echo "Unsupported architecture" && exit 1 ;; \
    esac && \
    curl -fsSL "https://github.com/mozilla/sccache/releases/download/v${SCCACHE_VERSION}/sccache-v${SCCACHE_VERSION}-${ARCH}.tar.gz" -o /tmp/sccache.tar.gz && \
    echo "${SCCACHE_SHA256}  /tmp/sccache.tar.gz" | sha256sum -c - && \
    tar -xzf /tmp/sccache.tar.gz -C /tmp && \
    mv "/tmp/sccache-v${SCCACHE_VERSION}-${ARCH}/sccache" /usr/local/cargo/bin/ && \
    rm -rf /tmp/sccache.tar.gz "/tmp/sccache-v${SCCACHE_VERSION}-${ARCH}"
RUN cargo install cargo-chef --version 0.1.78 --locked
COPY --from=planner /app/recipe.json recipe.json
# This layer is cached until Cargo.toml/Cargo.lock change
# Use BuildKit secrets to pass AWS credentials securely (not exposed in image metadata)
RUN --mount=type=secret,id=aws_access_key_id \
    --mount=type=secret,id=aws_secret_access_key \
    --mount=type=secret,id=aws_session_token \
    export AWS_ACCESS_KEY_ID=$(cat /run/secrets/aws_access_key_id) && \
    export AWS_SECRET_ACCESS_KEY=$(cat /run/secrets/aws_secret_access_key) && \
    export AWS_SESSION_TOKEN=$(cat /run/secrets/aws_session_token) && \
    if [ -n "${SCCACHE_BUCKET:-}" ]; then export RUSTC_WRAPPER=sccache; fi && \
    cargo chef cook --release --locked --features logrotate_fs --recipe-path recipe.json

# Stage 2: Builder - Build source code
FROM base AS builder
ARG SCCACHE_BUCKET
ARG SCCACHE_REGION
ENV CARGO_INCREMENTAL=0
ENV SCCACHE_BUCKET=${SCCACHE_BUCKET}
ENV SCCACHE_REGION=${SCCACHE_REGION}
WORKDIR /app
RUN apt-get update && apt-get install -y \
    pkg-config=1.8.1-1 \
    libssl-dev=3.0.22-1~deb12u1 \
    protobuf-compiler=3.21.12-3+deb12u1 \
    fuse3=3.14.0-4 \
    libfuse3-dev=3.14.0-4 \
    && rm -rf /var/lib/apt/lists/*
# Copy cached dependencies and sccache from cacher
COPY --from=cacher /app/target target
COPY --from=cacher /usr/local/cargo /usr/local/cargo
# Copy source code (frequently changes)
COPY . .
# Build binary - reuses cached dependencies + sccache
# Use BuildKit secrets to pass AWS credentials securely (not exposed in image metadata)
RUN --mount=type=secret,id=aws_access_key_id \
    --mount=type=secret,id=aws_secret_access_key \
    --mount=type=secret,id=aws_session_token \
    export AWS_ACCESS_KEY_ID=$(cat /run/secrets/aws_access_key_id) && \
    export AWS_SECRET_ACCESS_KEY=$(cat /run/secrets/aws_secret_access_key) && \
    export AWS_SESSION_TOKEN=$(cat /run/secrets/aws_session_token) && \
    if [ -n "${SCCACHE_BUCKET:-}" ]; then export RUSTC_WRAPPER=sccache; fi && \
    cargo build --release --locked --bin lading --features logrotate_fs

# Stage 3: Runtime
FROM docker.io/debian:bookworm-20241202-slim AS runtime
COPY ci/update-ca-certificates /usr/local/bin/lading-update-ca-certificates
RUN apt-get update && apt-get upgrade -y && apt-get install -y \
    ca-certificates \
    libfuse3-dev=3.14.0-4 \
    fuse3=3.14.0-4 \
    && lading-update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/lading /usr/bin/lading

# Smoke test
RUN ["/usr/bin/lading", "--help"]
ENTRYPOINT ["/usr/bin/lading"]
