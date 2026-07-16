#!/usr/bin/env bash
set -euo pipefail

# Run this script to tell Antithesis it can start running test commands.
# You can also emit setup-complete from your system with the Antithesis SDK,
# if that's easier.
#
# Antithesis sets the `ANTITHESIS_OUTPUT_DIR` environment variable
# automatically. This script emits `setup_complete` to the `sdk.jsonl` file
# in that directory.

if [[ -n "${ANTITHESIS_OUTPUT_DIR:-}" ]]; then
  OUTPUT_PATH="${ANTITHESIS_OUTPUT_DIR}/sdk.jsonl"
  mkdir -p $(dirname "$OUTPUT_PATH")
  echo "Running in Antithesis, emitting setup_complete to ${OUTPUT_PATH}"
  echo '{"antithesis_setup":{"status":"complete","details":{"message":"ready to go"}}}' >> "${OUTPUT_PATH}"
else
  echo "\$ANTITHESIS_OUTPUT_DIR is unset, not emitting setup-complete"
fi
