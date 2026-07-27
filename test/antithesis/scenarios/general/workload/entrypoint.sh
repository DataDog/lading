#!/usr/bin/env bash
set -euo pipefail

# Workload driver entrypoint.
#
# Gated on sink-healthy (compose depends_on). lading drives the load itself, so
# the workload only emits setup_complete and then idles, awaiting the Antithesis
# test commands that will vary configs in a later iteration.

/opt/antithesis/setup-complete.sh
echo "setup_complete emitted; workload idle, awaiting Antithesis test commands."
exec tail -f /dev/null
