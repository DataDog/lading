#!/usr/bin/env bash
# Minimal smoke test: cat every file in the FUSE mount and POST to blackhole.
# Good for verifying capture plumbing produces records. Not realistic.

set -euo pipefail

MOUNT="${1:-/tmp/lading_mount}"
BLACKHOLE="${2:-http://127.0.0.1:9091}"
INTERVAL="${3:-10}"

echo "Mount point: $MOUNT"
echo "Blackhole:   $BLACKHOLE"
echo "Interval:    ${INTERVAL}s"
echo ""

while true; do
    for f in $(find "$MOUNT" -type f 2>/dev/null); do
        bytes=$(wc -c < "$f")
        echo "[$(date -Iseconds)] cat $f ($bytes bytes)"
    done

    curl -s -X POST "$BLACKHOLE" \
        -H "Content-Type: application/json" \
        -d "[{\"message\":\"hello from cat exerciser at $(date -Iseconds)\"}]" \
        -o /dev/null -w "  POST %{http_code}\n"

    sleep "$INTERVAL"
done
