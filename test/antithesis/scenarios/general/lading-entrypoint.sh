#!/usr/bin/env bash
set -euo pipefail

# lading (system under test) entrypoint. lading reads its config once at startup
# and cannot be reconfigured, so the per-timeline config must be chosen before it
# boots. Block on the `ready` sentinel that the first_sample_config command writes
# to the shared volume, then exec lading under that sampled config.

CONFIG_DIR="${CONFIG_DIR:-/shared}"

echo "lading: waiting for ${CONFIG_DIR}/ready" >&2
while [ ! -f "${CONFIG_DIR}/ready" ]; do
  sleep 1
done
echo "lading: config ready, starting" >&2

exec /usr/local/bin/lading \
  --no-target \
  --experiment-duration-infinite \
  --prometheus-addr 0.0.0.0:9102 \
  --config-path "${CONFIG_DIR}/lading.yaml"
