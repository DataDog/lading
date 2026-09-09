#!/usr/bin/env bash
# End-to-end smoke test for the tcp_crr generator + blackhole pair.
#
# Runs a single lading process with paired tcp_crr generator and blackhole on
# loopback, captures metrics to a JSONL file, and verifies the round-trip
# emitted non-zero counters for the core request/response/connection metrics.
#
# Exits 0 on success, non-zero on failure. Environment overrides:
#   LADING_BIN        path to the lading binary (default: target/debug/lading)
#   WARMUP_SECS       lading warmup duration (default: 5)
#   EXPERIMENT_SECS   lading experiment duration (default: 15)
#   FLOWS             flow count on the blackhole (default: 4)
#   THREADS           thread count on both sides (default: 1)
#   DATA_PORT         data port to bind on (default: 14867)
#   CONTROL_PORT      control port to bind on (default: 14866)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LADING_BIN="${LADING_BIN:-$REPO_ROOT/target/debug/lading}"
WARMUP_SECS="${WARMUP_SECS:-5}"
EXPERIMENT_SECS="${EXPERIMENT_SECS:-15}"
FLOWS="${FLOWS:-4}"
THREADS="${THREADS:-1}"
DATA_PORT="${DATA_PORT:-14867}"
CONTROL_PORT="${CONTROL_PORT:-14866}"

if ! command -v jq >/dev/null 2>&1; then
    echo "ERROR: jq is required for capture parsing" >&2
    exit 1
fi

if [[ ! -x "$LADING_BIN" ]]; then
    echo "[setup] building lading (debug) ..."
    (cd "$REPO_ROOT" && cargo build -p lading --bin lading) >&2
fi

WORK_DIR="$(mktemp -d -t lading-tcp-crr-e2e.XXXXXX)"
trap 'rm -rf "$WORK_DIR"' EXIT

CONFIG="$WORK_DIR/config.yaml"
CAPTURE="$WORK_DIR/capture.jsonl"

cat >"$CONFIG" <<EOF
generator:
  - tcp_crr:
      addr: "127.0.0.1"
      data_port: $DATA_PORT
      control_port: $CONTROL_PORT
      threads: $THREADS
      request_size: 64
      response_size: 64
      no_delay: true

blackhole:
  - tcp_crr:
      addr: "127.0.0.1"
      data_port: $DATA_PORT
      control_port: $CONTROL_PORT
      threads: $THREADS
      flows: $FLOWS
      request_size: 64
      response_size: 64
      no_delay: true
      backlog: 1024
EOF

echo "[setup] config:    $CONFIG"
echo "[setup] capture:   $CAPTURE"
echo "[setup] params:    warmup=${WARMUP_SECS}s experiment=${EXPERIMENT_SECS}s flows=$FLOWS threads=$THREADS"
echo "[setup] ports:     data=$DATA_PORT control=$CONTROL_PORT"

# Hard timeout = warmup + experiment + generous shutdown slack.
TOTAL_TIME=$((WARMUP_SECS + EXPERIMENT_SECS + 30))

echo "[run] starting lading (hard timeout ${TOTAL_TIME}s) ..."
timeout --foreground -k 10 "$TOTAL_TIME" \
    "$LADING_BIN" \
    --config-path "$CONFIG" \
    --no-target \
    --capture-path "$CAPTURE" \
    --warmup-duration-seconds "$WARMUP_SECS" \
    --experiment-duration-seconds "$EXPERIMENT_SECS" \
    --max-shutdown-delay 10
echo "[run] lading exited cleanly"

if [[ ! -s "$CAPTURE" ]]; then
    echo "FAIL: capture file empty: $CAPTURE" >&2
    exit 1
fi

# Sum a counter's deltas across all lines.
sum_metric() {
    jq -r --arg m "$1" 'select(.metric_name == $m) | .value' "$CAPTURE" \
        | awk '{ s += $1 } END { print s + 0 }'
}

echo ""
echo "== Captured metric totals =="

# Core counters: every one must be non-zero for the workload to have run.
EXPECTED=(
    requests_sent
    responses_received
    bytes_written
    bytes_read
    connections_accepted
    requests_received
    responses_sent
    bytes_received
)

ok=true
for m in "${EXPECTED[@]}"; do
    total="$(sum_metric "$m")"
    if [[ "$total" -eq 0 ]]; then
        printf 'FAIL  %-25s = 0\n' "$m"
        ok=false
    else
        printf 'ok    %-25s = %s\n' "$m" "$total"
    fi
done

# CRR-specific sanity: connections_accepted must grow well past the flow
# count because each transaction opens and closes a fresh connection. If
# it stayed near FLOWS, the workload is behaving like tcp_rr, not tcp_crr.
accepted="$(sum_metric connections_accepted)"
if (( accepted <= FLOWS * 2 )); then
    printf 'FAIL  connections_accepted=%s is too close to flows=%s - CRR is not reconnecting\n' \
        "$accepted" "$FLOWS"
    ok=false
fi

if [[ "$ok" != "true" ]]; then
    echo ""
    echo "Test FAILED"
    exit 1
fi

echo ""
echo "Test PASSED"
