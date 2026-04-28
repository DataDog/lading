#!/usr/bin/env bash
# Run lading with capture enabled and exercise the FUSE mount + blackhole.
# Usage: ./scripts/run_capture_test.sh [cat|tail|agent] [duration_seconds] [config_yaml]
#
# Modes:
#   cat   - bulk read entire files (smoke test)
#   tail  - track offsets, read only new bytes
#   agent - emulate DD logs agent: split by newline, send as JSON log entries
#
# Default: cat mode, 60 seconds, scripts/capture_test.yaml

set -euo pipefail

MODE="${1:-cat}"
DURATION="${2:-60}"
CONFIG="${3:-scripts/capture_test.yaml}"
MOUNT="/tmp/lading_mount"
BLACKHOLE="http://127.0.0.1:9091"

cleanup_mount() {
    if mountpoint -q "$MOUNT" 2>/dev/null; then
        echo "Cleaning up stale FUSE mount at $MOUNT..."
        fusermount -u "$MOUNT" 2>/dev/null || umount "$MOUNT" 2>/dev/null || true
    fi
}

trap cleanup_mount EXIT

echo "Mode:     $MODE"
echo "Duration: ${DURATION}s"
echo "Config:   $CONFIG"
echo ""

# Clean up any stale mount from a previous run
cleanup_mount

# Build first
cargo build --bin lading --features logrotate_fs

# Start lading in the background
cargo run --bin lading --features logrotate_fs -- \
  --config-path "$CONFIG" \
  --no-target \
  --capture-path scratch/captures/metrics.jsonl \
  --experiment-duration-seconds "$DURATION" &
LADING_PID=$!

# Wait for the FUSE mount to appear
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

# Run the exerciser until lading exits
if [ "$MODE" = "agent" ]; then
    # Agent mode: launch the agent emulator script in the background
    ./scripts/exercise_agent.sh "$MOUNT" "$BLACKHOLE" 2 &
    EXERCISER_PID=$!
    wait $LADING_PID 2>/dev/null || true
    kill $EXERCISER_PID 2>/dev/null || true
    wait $EXERCISER_PID 2>/dev/null || true
else
    if [ "$MODE" = "tail" ]; then
        INTERVAL=2
    else
        INTERVAL=5
    fi

    while kill -0 $LADING_PID 2>/dev/null; do
        if [ "$MODE" = "tail" ]; then
            # Tail mode: read only new bytes per file
            declare -A OFFSETS 2>/dev/null || true
            for f in $(find "$MOUNT" -type f 2>/dev/null); do
                size=$(stat --format=%s "$f" 2>/dev/null || echo 0)
                offset=${OFFSETS[$f]:-0}
                if [ "$size" -gt "$offset" ]; then
                    new_bytes=$((size - offset))
                    chunk=$(dd if="$f" bs=1 skip="$offset" count="$new_bytes" 2>/dev/null)
                    OFFSETS[$f]=$size
                    echo "[$(date -Iseconds)] tail $f: +${new_bytes} bytes (${offset}->${size})"
                    if [ -n "$chunk" ]; then
                        curl -s -X POST "$BLACKHOLE" \
                            -H "Content-Type: application/json" \
                            -d "[{\"message\":$(echo "$chunk" | head -c 4096 | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))')}]" \
                            -o /dev/null -w "  POST %{http_code}\n"
                    fi
                fi
            done
        else
            # Cat mode: read entire files
            for f in $(find "$MOUNT" -type f 2>/dev/null); do
                bytes=$(wc -c < "$f")
                echo "[$(date -Iseconds)] cat $f ($bytes bytes)"
            done
            curl -s -X POST "$BLACKHOLE" \
                -H "Content-Type: application/json" \
                -d "[{\"message\":\"hello from cat exerciser at $(date -Iseconds)\"}]" \
                -o /dev/null -w "  POST %{http_code}\n"
        fi
        sleep "$INTERVAL"
    done
    wait $LADING_PID 2>/dev/null || true
fi

echo ""
echo "=== Capture files ==="
echo "FUSE reads:  $(wc -l < scratch/captures/fuse_reads.jsonl) records"
echo "Blackhole:   $(wc -l < scratch/captures/blackhole.jsonl) records"
echo ""
echo "Sample FUSE reads:"
head -5 scratch/captures/fuse_reads.jsonl
echo ""
echo "Sample blackhole:"
head -5 scratch/captures/blackhole.jsonl
