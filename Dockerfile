FROM docker.io/rust:1.62.1-bullseye@sha256:9bdedf91cf0ae0a76cc6b17b333898926dcace17dee3a50974242684b386cc23 as builder

WORKDIR /app
COPY . /app
RUN cargo build --release --locked

FROM docker.io/debian:bullseye-slim@sha256:b0d53c872fd640c2af2608ba1e693cfc7dedea30abcd8f584b23d583ec6dadc7
COPY --from=builder /app/target/release/lading /usr/bin/lading

# smoke test
RUN ["/usr/bin/lading", "--help"]
ENTRYPOINT ["/usr/bin/lading"]
