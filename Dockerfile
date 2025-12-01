# Update the rust version in-sync with the version in rust-toolchain.toml
FROM docker.io/rust:1.90.0-bookworm AS builder

RUN apt-get update && apt-get install -y \
    protobuf-compiler fuse3 libfuse3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app
RUN cargo build --release --locked --bin lading --features logrotate_fs

FROM docker.io/debian:bookworm-20241202-slim
RUN apt-get update && apt-get install -y \
    libfuse3-dev=3.14.0-4 \
    fuse3=3.14.0-4 \
    tcpdump \
    procps \
    lsof \
    net-tools \
    iproute2 \
    curl \
    strace \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/lading /usr/bin/lading

# Create wrapper script that starts tcpdump before lading
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
echo "Starting tcpdump on port 11538..."\n\
tcpdump -i any -nn -s0 port 11538 -w /captures/grpc-traffic.pcap &\n\
TCPDUMP_PID=$!\n\
echo $TCPDUMP_PID > /tmp/tcpdump.pid\n\
echo "tcpdump started with PID $TCPDUMP_PID, capturing to /captures/grpc-traffic.pcap"\n\
\n\
# Give tcpdump time to initialize\n\
sleep 2\n\
\n\
# Start lading with all arguments\n\
exec /usr/bin/lading "$@"\n\
' > /usr/bin/lading-wrapper && chmod +x /usr/bin/lading-wrapper

# smoke test
RUN ["/usr/bin/lading", "--help"]
ENTRYPOINT ["/usr/bin/lading-wrapper"]
