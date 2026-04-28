#!/usr/bin/env bash
# Minimal logs-agent emulator: tails files, splits by newline, sends each line
# as a separate message in DD logs agent JSON format to the blackhole.
#
# Usage: ./scripts/exercise_agent.sh [mount] [blackhole_url] [interval_sec]

set -euo pipefail

MOUNT="${1:-/tmp/lading_mount}"
BLACKHOLE="${2:-http://127.0.0.1:9091}"
INTERVAL="${3:-2}"

declare -A OFFSETS
STOPPING=false

trap 'STOPPING=true; echo "[$(date -Iseconds)] SIGTERM received, flushing..."' TERM INT

echo "Mount point: $MOUNT"
echo "Blackhole:   $BLACKHOLE"
echo "Interval:    ${INTERVAL}s"
echo ""

# Read new bytes from all files and send to blackhole. Returns 0 if any data
# was sent, 1 if there was nothing new.
flush_once() {
    local sent=false
    mapfile -t FILES < <(find "$MOUNT" -type f 2>/dev/null | sort)

    for f in "${FILES[@]}"; do
        size=$(stat --format=%s "$f" 2>/dev/null || echo 0)
        offset=${OFFSETS[$f]:-0}

        if [ "$size" -le "$offset" ]; then
            continue
        fi

        new_bytes=$((size - offset))
        chunk=$(dd if="$f" bs=1 skip="$offset" count="$new_bytes" 2>/dev/null)
        OFFSETS[$f]=$size

        # Split chunk into lines and build a JSON array of log entries
        payload="["
        first=true
        while IFS= read -r line; do
            [ -z "$line" ] && continue
            escaped=$(python3 -c 'import sys,json; print(json.dumps(sys.stdin.read().rstrip("\n")))' <<< "$line")
            if [ "$first" = true ]; then
                first=false
            else
                payload+=","
            fi
            payload+="{\"message\":${escaped},\"status\":\"info\",\"timestamp\":$(date +%s%3N),\"hostname\":\"lading-exerciser\",\"service\":\"exercise_agent\",\"ddsource\":\"file\",\"ddtags\":\"\"}"
        done <<< "$chunk"
        payload+="]"

        line_count=$(echo "$chunk" | grep -c . || true)
        echo "[$(date -Iseconds)] $f: +${new_bytes} bytes, ${line_count} lines (${offset}->${size})"

        if [ "$first" = false ]; then
            echo "$payload" | curl -s -X POST "$BLACKHOLE" \
                -H "Content-Type: application/json" \
                -d @- \
                -o /dev/null -w "  POST %{http_code}\n"
            sent=true
        fi
    done

    $sent
}

while true; do
    flush_once || true

    if [ "$STOPPING" = true ]; then
        echo "[$(date -Iseconds)] Final flush..."
        flush_once || true
        echo "[$(date -Iseconds)] Done, exiting."
        exit 0
    fi

    # Use sleep in background so SIGTERM interrupts it immediately
    sleep "$INTERVAL" &
    wait $! 2>/dev/null || true
done
