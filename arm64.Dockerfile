FROM docker.io/rust:1.74.0-bullseye@sha256:cbf2fab601f7884e19ded0b582f67d35fb5e76b2ff2365d4007a6086befeb9ec as builder

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
