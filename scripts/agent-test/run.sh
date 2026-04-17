#!/usr/bin/env bash
# Run a real DD agent against lading's FUSE mount and capture the results.
#
# Usage: ./scripts/agent-test/run.sh [duration_seconds] [lading_config] [analysis_config]
#
# Examples:
#   ./scripts/agent-test/run.sh 60                                          # ascii (default)
#   ./scripts/agent-test/run.sh 60 scripts/agent-test/lading_templated_json.yaml scripts/agent-test/analysis_templated_json.yaml
#   ./scripts/agent-test/run.sh 60 scripts/agent-test/lading_truncation.yaml scripts/agent-test/analysis_truncation.yaml
#
# Prerequisites:
#   docker pull datadog/agent-dev:nightly-full-main-5940559f-jmx

set -euo pipefail

DURATION="${1:-120}"
LADING_CONFIG="${2:-scripts/agent-test/lading_ascii.yaml}"
ANALYSIS_CONFIG="${3:-scripts/agent-test/analysis_ascii.yaml}"
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
    # Clean up the bind mount
    sudo umount "$MOUNT" 2>/dev/null || true
}

trap cleanup EXIT

# Clean up any stale state
cleanup 2>/dev/null

echo "Duration: ${DURATION}s"
echo "Lading:   $LADING_CONFIG"
echo "Analysis: $ANALYSIS_CONFIG"
echo "Agent:    $AGENT_IMAGE"
echo ""

# Build lading and analysis tool
cargo build --bin lading --features logrotate_fs --bin lading-analysis

# Clear stale capture files from previous runs
rm -f scratch/captures/fuse_reads.jsonl scratch/captures/blackhole.jsonl scratch/captures/metrics.jsonl

# Set up shared mount point with rshared propagation.
# When lading FUSE-mounts inside $MOUNT, the agent container sees it
# via mount propagation — regardless of start order.
echo "Setting up rshared mount at $MOUNT..."
sudo mkdir -p "$MOUNT"
sudo mount --bind "$MOUNT" "$MOUNT"
sudo mount --make-rshared "$MOUNT"

# Start lading in background. It will poll for the agent container via
# --target-container and start the experiment timer once it detects it.
echo "Starting lading..."
RUST_LOG=info cargo run --bin lading --features logrotate_fs -- \
  --config-path "$LADING_CONFIG" \
  --target-container "$AGENT_NAME" \
  --capture-path scratch/captures/metrics.jsonl \
  --experiment-duration-seconds "$DURATION" \
  --cooldown-duration-seconds 30 &
LADING_PID=$!

# Give lading a moment to start and create the FUSE mount
sleep 3

# Start the DD agent container. Lading detects it via Docker API polling
# and begins the experiment.
echo "Starting DD agent container..."
docker run -d \
  --name "$AGENT_NAME" \
  --network host \
  --privileged \
  -e DD_API_KEY=a0000001 \
  -e DD_HOSTNAME=lading-capture-test \
  -v "$MOUNT:/tmp/smp-shared:rshared" \
  -v "$SCRIPT_DIR/datadog-agent/datadog.yaml:/etc/datadog-agent/datadog.yaml:ro" \
  -v "$SCRIPT_DIR/datadog-agent/conf.d:/etc/datadog-agent/conf.d:ro" \
  "$AGENT_IMAGE"

echo "Agent container started. Waiting for lading to complete..."

# Wait for lading to finish (warmup + experiment + cooldown)
wait $LADING_PID 2>/dev/null || true
echo "Lading finished."

# Stop the agent gracefully
echo "Stopping agent..."
docker stop -t 30 "$AGENT_NAME" 2>/dev/null || true

echo ""
echo "=== Capture files ==="
echo "FUSE reads:  $(wc -l < scratch/captures/fuse_reads.jsonl) records"
echo "Blackhole:   $(wc -l < scratch/captures/blackhole.jsonl) records"

echo ""
echo "=== Running analysis ==="
cargo run --bin lading-analysis -- --config "$ANALYSIS_CONFIG" || true

echo ""
echo "=== Agent logs (last 20 lines) ==="
docker logs --tail 20 "$AGENT_NAME" 2>&1 || true
