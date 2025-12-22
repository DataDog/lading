# Update the rust version in-sync with the version in rust-toolchain.toml

# Stage 0: Planner - Extract dependency metadata
FROM docker.io/rust:1.90.0-slim-bookworm AS planner
WORKDIR /app
RUN cargo install cargo-chef --version 0.1.73
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Stage 1: Cacher - Build dependencies only
FROM docker.io/rust:1.90.0-slim-bookworm AS cacher
ARG SCCACHE_BUCKET
ARG SCCACHE_REGION
ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY
ARG AWS_SESSION_TOKEN
ENV CARGO_INCREMENTAL=0
WORKDIR /app
RUN apt-get update && apt-get install -y \
    pkg-config=1.8.1-1 \
    libssl-dev=3.0.17-1~deb12u3 \
    protobuf-compiler=3.21.12-3 \
    fuse3=3.14.0-4 \
    libfuse3-dev=3.14.0-4 \
    curl \
    && rm -rf /var/lib/apt/lists/*
# Download pre-built sccache binary
RUN case "$(uname -m)" in \
    x86_64) ARCH=x86_64-unknown-linux-musl ;; \
    aarch64) ARCH=aarch64-unknown-linux-musl ;; \
    *) echo "Unsupported architecture" && exit 1 ;; \
    esac && \
    curl -L https://github.com/mozilla/sccache/releases/download/v0.8.2/sccache-v0.8.2-${ARCH}.tar.gz | tar xz && \
    mv sccache-v0.8.2-${ARCH}/sccache /usr/local/cargo/bin/ && \
    rm -rf sccache-v0.8.2-${ARCH}
RUN cargo install cargo-chef --version 0.1.73
COPY --from=planner /app/recipe.json recipe.json
# This layer is cached until Cargo.toml/Cargo.lock change
# Use BuildKit secrets to pass AWS credentials securely (not exposed in image metadata)
RUN --mount=type=secret,id=aws_access_key_id \
    --mount=type=secret,id=aws_secret_access_key \
    --mount=type=secret,id=aws_session_token \
    export AWS_ACCESS_KEY_ID=$(cat /run/secrets/aws_access_key_id) && \
    export AWS_SECRET_ACCESS_KEY=$(cat /run/secrets/aws_secret_access_key) && \
    export AWS_SESSION_TOKEN=$(cat /run/secrets/aws_session_token) && \
    export RUSTC_WRAPPER=sccache && \
    cargo chef cook --release --locked --features logrotate_fs --recipe-path recipe.json

# Stage 2: Builder - Build source code
FROM docker.io/rust:1.90.0-slim-bookworm AS builder
ARG SCCACHE_BUCKET
ARG SCCACHE_REGION
ENV CARGO_INCREMENTAL=0
ENV SCCACHE_BUCKET=${SCCACHE_BUCKET}
ENV SCCACHE_REGION=${SCCACHE_REGION}
WORKDIR /app
RUN apt-get update && apt-get install -y \
    pkg-config=1.8.1-1 \
    libssl-dev=3.0.17-1~deb12u3 \
    protobuf-compiler=3.21.12-3 \
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
    export RUSTC_WRAPPER=sccache && \
    cargo build --release --locked --bin lading --features logrotate_fs

# Stage 3: Runtime
FROM docker.io/debian:bookworm-20241202-slim
RUN apt-get update && apt-get install -y \
    libfuse3-dev=3.14.0-4 \
    fuse3=3.14.0-4 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/lading /usr/bin/lading

# Smoke test
RUN ["/usr/bin/lading", "--help"]
ENTRYPOINT ["/usr/bin/lading"]
