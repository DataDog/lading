# Update the rust version in-sync with the version in rust-toolchain.toml

# Stage 0: Planner - Extract dependency metadata
FROM docker.io/rust:1.90.0-slim-bookworm AS planner
WORKDIR /app
RUN cargo install cargo-chef --version 0.1.73
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Stage 1: Cacher - Build dependencies only
FROM docker.io/rust:1.90.0-slim-bookworm AS cacher
ENV JEMALLOC_SYS_WITH_PROFILING=1
WORKDIR /app
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config=1.8.1-1 \
    libssl-dev=3.0.17-1~deb12u3 \
    protobuf-compiler=3.21.12-3 \
    fuse3=3.14.0-4 \
    libfuse3-dev=3.14.0-4 \
    curl \
    && rm -rf /var/lib/apt/lists/*
RUN cargo install cargo-chef --version 0.1.73
COPY --from=planner /app/recipe.json recipe.json
# This layer is cached until Cargo.toml/Cargo.lock change
RUN cargo chef cook --release --locked --features "logrotate_fs jemalloc_profiling" --recipe-path recipe.json

# Stage 2: Builder - Build source code
FROM docker.io/rust:1.90.0-slim-bookworm AS builder
ENV JEMALLOC_SYS_WITH_PROFILING=1
WORKDIR /app
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config=1.8.1-1 \
    libssl-dev=3.0.17-1~deb12u3 \
    protobuf-compiler=3.21.12-3 \
    fuse3=3.14.0-4 \
    libfuse3-dev=3.14.0-4 \
    && rm -rf /var/lib/apt/lists/*
# Copy cached dependencies and sccache from cacher
COPY --from=cacher /app/target target
COPY --from=cacher /usr/local/cargo /usr/local/cargo
# Copy source code (frequently changes)
COPY . .
# Build binary - reuses cached dependencies + sccache
# Use BuildKit secrets to pass AWS credentials securely (not exposed in image metadata)
RUN cargo build --release --locked --bin lading --features "logrotate_fs jemalloc_profiling"

# Stage 3: Runtime
FROM docker.io/debian:bookworm-20241202-slim
RUN apt-get update && apt-get install -y \
    libfuse3-dev=3.14.0-4 \
    fuse3=3.14.0-4 \
    curl \
    ca-certificates \
    procps \
    gawk \
    perl \
    libjemalloc2 \
    && rm -rf /var/lib/apt/lists/*
RUN ARCH="$(dpkg --print-architecture)" && \
    url_release="https://github.com/DataDog/ddprof/releases/latest/download/ddprof-${ARCH}" && \
    curl -L -o /usr/local/bin/ddprof "${url_release}" && \
    chmod 755 /usr/local/bin/ddprof
# Lightweight page-table/RSS sampler helper
RUN cat > /usr/local/bin/pt-sampler <<'EOF' && chmod +x /usr/local/bin/pt-sampler
#!/bin/sh
# Usage: pt-sampler <pid> [interval_seconds]
pid="$1"
interval="${2:-5}"
if [ -z "$pid" ]; then
  echo "usage: pt-sampler <pid> [interval_seconds]" >&2
  exit 1
fi
while :; do
  ts="$(date -Is)"
  pt_kb="$(awk '/^PageTables:/ {sum+=$2} END {print sum+0}' /proc/"$pid"/smaps 2>/dev/null)"
  rss_kb="$(awk '/^VmRSS:/ {print $2}' /proc/"$pid"/status 2>/dev/null)"
  echo "$ts rss_kb=${rss_kb:-0} pagetables_kb=${pt_kb:-0}"
  sleep "$interval"
done
EOF

# Simple smaps rollup helper to find largest RSS mappings
RUN cat > /usr/local/bin/smaps-top <<'EOF' && chmod +x /usr/local/bin/smaps-top
#!/bin/sh
# Usage: smaps-top <pid> [topN]
pid="$1"
top="${2:-5}"
if [ -z "$pid" ]; then
  echo "usage: smaps-top <pid> [topN]" >&2
  exit 1
fi
gawk -v top="$top" '
BEGIN{RS=""; OFS=""; c=0}
{
  rss=0; size=0;
  if (match($0,/Rss:[[:space:]]+([0-9]+)/,a)) rss=a[1];
  if (match($0,/Size:[[:space:]]+([0-9]+)/,b)) size=b[1];
  hdr=$0; sub(/\n.*/,"",hdr);
  blocks[c,"rss"]=rss; blocks[c,"size"]=size; blocks[c,"hdr"]=hdr; blocks[c,"blk"]=$0; c++;
}
END{
  n=c;
  for(i=0;i<n;i++){
    max=i;
    for(j=i+1;j<n;j++){
      if(blocks[j,"rss"]>blocks[max,"rss"]) max=j;
    }
    if(max!=i){
      tmp_r=blocks[i,"rss"]; tmp_s=blocks[i,"size"]; tmp_h=blocks[i,"hdr"]; tmp_b=blocks[i,"blk"];
      blocks[i,"rss"]=blocks[max,"rss"]; blocks[i,"size"]=blocks[max,"size"]; blocks[i,"hdr"]=blocks[max,"hdr"]; blocks[i,"blk"]=blocks[max,"blk"];
      blocks[max,"rss"]=tmp_r; blocks[max,"size"]=tmp_s; blocks[max,"hdr"]=tmp_h; blocks[max,"blk"]=tmp_b;
    }
  }
  limit=(top<n)?top:n;
  for(i=0;i<limit;i++){
    print "---- Block ", i+1, " ----";
    print "Rss=", blocks[i,"rss"], " kB  Size=", blocks[i,"size"], " kB";
    print blocks[i,"hdr"];
    print blocks[i,"blk"];
  }
}
' /proc/"$pid"/smaps
EOF

COPY --from=builder /app/target/release/lading /usr/bin/lading
# ENV DD_TRACE_AGENT_URL=http://host.docker.internal:8126 \
#     DD_SERVICE=jsaf-lading \
#     DD_ENV=local \
#     DD_VERSION=dev

# Lightweight jeprof helper script for analyzing jemalloc heap dumps.
# Fetch the upstream script (templated perl) and replace configure placeholders.
RUN curl -fsSL https://raw.githubusercontent.com/jemalloc/jemalloc/5.3.0/bin/jeprof.in \
    | sed \
    -e 's|@PERL@|/usr/bin/env perl|' \
    -e 's|@jemalloc_version@|5.3.0|' \
    -e 's|@prefix@|/usr/local|' \
    -e 's|@bindir@|/usr/local/bin|' \
    > /usr/local/bin/jeprof && \
    chmod +x /usr/local/bin/jeprof

# Default jemalloc profiling configuration: enabled, active, 1MiB sample, dumps under /tmp.
# Note: tikv-jemalloc-sys uses _RJEM_MALLOC_CONF due to the _rjem_ symbol prefix.
# We set both for compatibility.
ENV MALLOC_CONF=prof:true,prof_active:true,lg_prof_sample:20,prof_prefix:/tmp/lading-heap \
    _RJEM_MALLOC_CONF=prof:true,prof_active:true,lg_prof_sample:20,prof_prefix:/tmp/lading-heap

# Entrypoint wrapper: ensures MALLOC_CONF defaults are present and dump dir exists.
RUN cat > /usr/local/bin/lading-entrypoint.sh <<'EOF' && chmod +x /usr/local/bin/lading-entrypoint.sh
#!/bin/sh
set -e

# Allow override; if not set, use baked-in default
# tikv-jemalloc-sys uses _RJEM_MALLOC_CONF due to symbol prefix, set both for compatibility
: "${MALLOC_CONF:=prof:true,prof_active:true,lg_prof_sample:20,prof_prefix:/tmp/lading-heap}"
: "${_RJEM_MALLOC_CONF:=$MALLOC_CONF}"
export MALLOC_CONF _RJEM_MALLOC_CONF

# Best-effort create dump directory from prof_prefix
prefix="$(printf '%s' "$MALLOC_CONF" | tr ',' '\n' | sed -n 's/^prof_prefix://p' | head -n1)"
if [ -n "$prefix" ]; then
  dir="$(dirname "$prefix")"
  mkdir -p "$dir" 2>/dev/null || true
fi

exec /usr/bin/lading "$@"
EOF

# Smoke test
RUN ["/usr/bin/lading", "--help"]
ENTRYPOINT ["/usr/local/bin/lading-entrypoint.sh"]
