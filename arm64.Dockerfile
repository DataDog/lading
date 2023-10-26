FROM docker.io/rust:1.71.0-bullseye@sha256:4ca63d7dc4792e2069e3d302fdf6b48de5ca8016c7abbe50f14466d769117a83 as builder

RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app
RUN cargo build --release --locked

FROM docker.io/debian:bullseye-slim@sha256:490274cda23d9ce2fd599bfe2d31ce68582491bead3b452fa3d27e8997fa9101
COPY --from=builder /app/target/release/lading /usr/bin/lading

# smoke test
RUN ["/usr/bin/lading", "--help"]
ENTRYPOINT ["/usr/bin/lading"]
