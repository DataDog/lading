FROM docker.io/rust:1.58-bullseye@sha256:e4979d36d5d30838126ea5ef05eb59c4c25ede7f064985e676feb21402d0661b as builder

WORKDIR /app
COPY . /app
RUN cargo build --release --locked

FROM docker.io/debian:bullseye-slim@sha256:b0d53c872fd640c2af2608ba1e693cfc7dedea30abcd8f584b23d583ec6dadc7
COPY --from=builder /app/target/release/lading /usr/bin/lading

# smoke test
RUN ["/usr/bin/lading", "--help"]
ENTRYPOINT ["/usr/bin/lading"]
