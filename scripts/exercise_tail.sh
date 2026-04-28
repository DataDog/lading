#!/usr/bin/env bash
# Tail-like exerciser: tracks read offsets per file, reads only new bytes,
# detects new/rotated files on each scan. Mimics how a logs agent tails.
#
# Posts every line it reads to the blackhole, simulating agent forwarding.

set -euo pipefail

MOUNT="${1:-/tmp/lading_mount}"
BLACKHOLE="${2:-http://127.0.0.1:9091}"
INTERVAL="${3:-2}"

# Associative array: filepath -> bytes already read
declare -A OFFSETS

echo "Mount point: $MOUNT"
echo "Blackhole:   $BLACKHOLE"
echo "Interval:    ${INTERVAL}s"
echo ""

while true; do
    # Discover all current files
    mapfile -t FILES < <(find "$MOUNT" -type f 2>/dev/null | sort)

    for f in "${FILES[@]}"; do
        size=$(stat --format=%s "$f" 2>/dev/null || echo 0)
        offset=${OFFSETS[$f]:-0}

        if [ "$size" -le "$offset" ]; then
            continue
        fi

        new_bytes=$((size - offset))
        # Read only the new bytes using dd
        chunk=$(dd if="$f" bs=1 skip="$offset" count="$new_bytes" 2>/dev/null)
        OFFSETS[$f]=$size

        echo "[$(date -Iseconds)] $f: +${new_bytes} bytes (offset ${offset}->${size})"

        # Post the new content to the blackhole, like an agent would
        if [ -n "$chunk" ]; then
            curl -s -X POST "$BLACKHOLE" \
                -H "Content-Type: application/json" \
                -d "[{\"message\":$(echo "$chunk" | head -c 4096 | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))')}]" \
                -o /dev/null -w "  POST %{http_code}\n"
        fi
    done

    # Detect files that disappeared (rotated away / deleted)
    for tracked in "${!OFFSETS[@]}"; do
        if [ ! -f "$tracked" ]; then
            echo "[$(date -Iseconds)] $tracked: gone (was at offset ${OFFSETS[$tracked]})"
            unset "OFFSETS[$tracked]"
        fi
    done

    sleep "$INTERVAL"
done
