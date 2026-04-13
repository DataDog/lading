#!/usr/bin/env bash
# Run a real DD agent against lading's FUSE mount and capture the results.
#
# Usage: ./scripts/agent-test/run.sh [duration_seconds]
#
# Prerequisites:
#   docker pull datadog/agent-dev:nightly-full-main-5940559f-jmx

set -euo pipefail

DURATION="${1:-120}"
MOUNT="/tmp/smp-shared"
AGENT_IMAGE="datadog/agent-dev:nightly-full-main-5940559f-jmx"
AGENT_NAME="lading-test-agent"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cleanup() {
    echo ""
    echo "=== Cleaning up ==="
    docker rm -f "$AGENT_NAME" 2>/dev/null || true
    if mountpoint -q "$MOUNT" 2>/dev/null; then
        fusermount -u "$MOUNT" 2>/dev/null || umount "$MOUNT" 2>/dev/null || true
    fi
}

trap cleanup EXIT

# Clean up any stale state
cleanup 2>/dev/null

echo "Duration: ${DURATION}s"
echo "Agent:    $AGENT_IMAGE"
echo ""

# Build lading and analysis tool
cargo build --bin lading --features logrotate_fs --bin lading-analysis

# Start lading in the background
echo "Starting lading..."
cargo run --bin lading --features logrotate_fs -- \
  --config-path scripts/agent-test/lading.yaml \
  --no-target \
  --capture-path scratch/captures/metrics.jsonl \
  --experiment-duration-seconds "$DURATION" &
LADING_PID=$!

# Wait for the FUSE mount
echo "Waiting for FUSE mount at $MOUNT..."
for i in $(seq 1 30); do
    if mountpoint -q "$MOUNT" 2>/dev/null; then
        break
    fi
    sleep 1
done

if ! mountpoint -q "$MOUNT" 2>/dev/null; then
    echo "ERROR: FUSE mount never appeared"
    kill $LADING_PID 2>/dev/null
    exit 1
fi
echo "Mount ready."

# Start the DD agent container
# - --network host: agent can reach lading's blackhole on 127.0.0.1
# - Mount the FUSE directory at /tmp/smp-shared (matching the agent's log config)
# - Mount the agent config
echo "Starting DD agent container..."
docker run -d \
  --name "$AGENT_NAME" \
  --network host \
  --privileged \
  -e DD_API_KEY=a0000001 \
  -e DD_HOSTNAME=lading-capture-test \
  -v "$MOUNT:/tmp/smp-shared:ro" \
  -v "$SCRIPT_DIR/datadog-agent/datadog.yaml:/etc/datadog-agent/datadog.yaml:ro" \
  -v "$SCRIPT_DIR/datadog-agent/conf.d:/etc/datadog-agent/conf.d:ro" \
  "$AGENT_IMAGE"

echo "Agent container started. Waiting for experiment to complete..."

# Wait for lading to finish
wait $LADING_PID 2>/dev/null || true

echo ""
echo "Lading finished. Giving agent 10s to flush..."
sleep 10

# Stop the agent gracefully
echo "Stopping agent..."
docker stop -t 30 "$AGENT_NAME" 2>/dev/null || true

echo ""
echo "=== Capture files ==="
echo "FUSE reads:  $(wc -l < scratch/captures/fuse_reads.jsonl) records"
echo "Blackhole:   $(wc -l < scratch/captures/blackhole.jsonl) records"

echo ""
echo "=== Running analysis ==="
cargo run --bin lading-analysis -- --config scripts/agent-test/analysis.yaml || true

echo ""
echo "=== Agent logs (last 20 lines) ==="
docker logs --tail 20 "$AGENT_NAME" 2>&1 || true
