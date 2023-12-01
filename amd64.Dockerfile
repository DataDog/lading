FROM docker.io/rust:1.74.0-bullseye@sha256:763c58c3619f2f8c22b51603da68ed6561930db5750ab5508559643b827af1a1 as builder

RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app
RUN cargo build --release --locked

FROM docker.io/debian:bullseye-slim@sha256:b0d53c872fd640c2af2608ba1e693cfc7dedea30abcd8f584b23d583ec6dadc7
COPY --from=builder /app/target/release/lading /usr/bin/lading

# smoke test
RUN ["/usr/bin/lading", "--help"]
ENTRYPOINT ["/usr/bin/lading"]
