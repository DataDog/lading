# Update the rust version in-sync with the version in rust-toolchain.toml

# Stage 0: Planner - Extract dependency metadata
FROM docker.io/rust:1.90.0-slim-bookworm AS planner
WORKDIR /app
RUN cargo install cargo-chef --version 0.1.73
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Stage 1: Cacher - Build dependencies only
FROM docker.io/rust:1.90.0-slim-bookworm AS cacher
WORKDIR /app
RUN apt-get update && apt-get install -y \
    protobuf-compiler=3.21.12-3 \
    fuse3=3.14.0-4 \
    libfuse3-dev=3.14.0-4 \
    && rm -rf /var/lib/apt/lists/*
RUN cargo install cargo-chef --version 0.1.73
COPY --from=planner /app/recipe.json recipe.json
# This layer is cached until Cargo.toml/Cargo.lock change
RUN cargo chef cook --release --locked --features logrotate_fs --recipe-path recipe.json

# Stage 2: Builder - Build source code
FROM docker.io/rust:1.90.0-slim-bookworm AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y \
    protobuf-compiler=3.21.12-3 \
    fuse3=3.14.0-4 \
    libfuse3-dev=3.14.0-4 \
    && rm -rf /var/lib/apt/lists/*
# Copy cached dependencies
COPY --from=cacher /app/target target
COPY --from=cacher /usr/local/cargo /usr/local/cargo
# Copy source code (frequently changes)
COPY . .
# Build binary - reuses cached dependencies
RUN cargo build --release --locked --bin lading --features logrotate_fs

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
