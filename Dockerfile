# Lading arm64 builds are very slow. We use cargo-chef and aggressive layer
# caching to avoid some of the build delay.

# Builder image, cargo-chef installed
FROM docker.io/rust:1.81.0-bullseye AS chef
WORKDIR /app
RUN apt-get update && apt-get install -y \
    protobuf-compiler fuse3 libfuse3-dev \
    && rm -rf /var/lib/apt/lists/*
RUN cargo install cargo-chef

# Planning stage, artifact is 'recipe.json'
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Cache dependencies per the recipe artifact.
FROM chef AS cacher
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Actually build lading.
FROM chef AS builder
COPY . .
RUN cargo build --release --locked --bin lading

# Install lading. This is the image that users will run.
FROM docker.io/debian:bullseye-20241016-slim AS target
RUN apt-get update && apt-get install -y \
    libfuse3-dev=3.10.3-2 fuse3=3.10.3-2 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/lading /usr/bin/lading

# Smoke test
RUN ["/usr/bin/lading", "--help"]
ENTRYPOINT ["/usr/bin/lading"]
