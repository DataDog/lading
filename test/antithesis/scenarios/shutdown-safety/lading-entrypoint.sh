#!/usr/bin/env bash
# NOTE: intentionally NO `set -e`. We must capture lading's exit code even when
# it is non-zero (e.g. a `timeout` kill), so an aborting shell would defeat the
# whole test.
set -uo pipefail

# lading (system under test) entrypoint for the shutdown-safety scenario.
#
# lading is run under a wall-clock `timeout` watchdog so that a PROMPT drain is
# distinguishable from reliance on the force-drop backstop:
#
#   * --max-shutdown-delay 60 makes lading's own force-drop backstop long (60s).
#   * The experiment timer fires at EXPERIMENT=15s and signals graceful shutdown.
#   * `timeout` kills lading at EXPERIMENT+20=35s.
#
# If the generator's connect/retry loop is shutdown-responsive, lading drains and
# exits at ~15s -> rc=0 (drained promptly). If that loop cannot observe the
# shutdown signal while busy-reconnecting to the unreachable destination, lading hangs
# on the drain (only the 60s backstop could ever release it) -> `timeout` kills
# it at 35s -> rc=124 (the finding). Any other rc indicates a crash.
#
# The exit code is written to the shared volume for the workload's checker.

EXPERIMENT=15

rc=0
timeout $((EXPERIMENT + 20)) /usr/local/bin/lading \
  --no-target --experiment-duration-seconds "$EXPERIMENT" --warmup-duration-seconds 0 \
  --max-shutdown-delay 60 \
  --capture-path /capture/capture.jsonl --config-path /etc/lading/lading.yaml || rc=$?

echo "$rc" > /shared/lading_exit
echo "lading exited with rc=$rc (0=drained promptly, 124=watchdog kill / did not drain)" >&2

exec tail -f /dev/null
