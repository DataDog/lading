# Update the rust version in-sync with the version in rust-toolchain.toml
FROM docker.io/rust:1.80.1-bullseye as builder

RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app
RUN cargo build --release --locked --bin lading

FROM docker.io/debian:bullseye-20240812-slim
COPY --from=builder /app/target/release/lading /usr/bin/lading

# smoke test
RUN ["/usr/bin/lading", "--help"]
ENTRYPOINT ["/usr/bin/lading"]
