#
# LADING BUILDER
#
FROM docker.io/rust:1.68.1-bullseye@sha256:ce1f9cc5f88955a7384a957a64793012945a7c130883edab0af44168723c7b77 as builder
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
