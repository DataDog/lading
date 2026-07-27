#!/usr/bin/env bash
set -euo pipefail

# Workload driver entrypoint.
#
# lading drives itself under a `timeout` watchdog, so the workload only emits
# setup_complete and then idles. Antithesis then runs the baked test command
# (anytime_lading_drained_bounded), which fires whenever it is scheduled and
# reads lading's recorded exit code from the shared volume.

/opt/antithesis/setup-complete.sh
echo "setup_complete emitted; workload idle, awaiting Antithesis test commands."
exec tail -f /dev/null
