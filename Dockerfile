# Update the rust version in-sync with the version in rust-toolchain.toml
FROM docker.io/rust:1.81.0-bullseye AS builder

RUN apt-get update && apt-get install -y \
    protobuf-compiler fuse3 libfuse3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app
RUN cargo build --release --locked --bin lading

FROM docker.io/debian:bullseye-20240701-slim
RUN apt-get update && apt-get install -y libfuse3-dev && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/lading /usr/bin/lading

# smoke test
RUN ["/usr/bin/lading", "--help"]
ENTRYPOINT ["/usr/bin/lading"]
