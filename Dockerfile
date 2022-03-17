FROM docker.io/library/rust:1.58.1 AS builder

WORKDIR /app
COPY . /app
RUN cargo build --release --locked

FROM gcr.io/distroless/cc AS runtime
COPY --from=builder /app/target/release/lading /usr/bin/lading
ENTRYPOINT ["/usr/bin/lading"]
