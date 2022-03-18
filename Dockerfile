FROM docker.io/library/rust:1.58.1 AS builder

WORKDIR /app
COPY . /app
RUN cargo build --release --locked

FROM docker.io/library/debian:stable-20220316-slim AS runtime
COPY --from=builder /app/target/release/lading /usr/bin/lading
RUN /usr/bin/lading --help # smoke test
ENTRYPOINT ["/usr/bin/lading"]
