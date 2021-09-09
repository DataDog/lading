FROM docker.io/library/rust:1.54.0 AS builder

WORKDIR /app
COPY . /app
RUN cargo build --release --locked

FROM gcr.io/distroless/cc AS runtime
COPY --from=builder /app/target/release/file_gen /
COPY --from=builder /app/target/release/http_gen /
COPY --from=builder /app/target/release/kafka_gen /
COPY --from=builder /app/target/release/tcp_gen /
COPY --from=builder /app/target/release/http_blackhole /
COPY --from=builder /app/target/release/udp_blackhole /
