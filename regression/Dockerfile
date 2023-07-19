#
# LADING BUILDER
#
FROM docker.io/rust:1.71.0-bullseye@sha256:bef59af02f103760cd57e8d6ccadf364954b0ae5e74ea7c7203d26744aeec051 as builder
WORKDIR /work
COPY . .
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*
RUN cargo build --bin lading --release --locked

#
# RUNTIME
#

FROM docker.io/debian:bullseye-slim@sha256:f7d141c1ec6af549958a7a2543365a7829c2cdc4476308ec2e182f8a7c59b519
COPY --from=builder /work/target/release/lading /usr/local/bin/lading

# Smoke test
RUN ["/usr/local/bin/lading", "--help"]

ENTRYPOINT ["/usr/local/bin/lading"]
