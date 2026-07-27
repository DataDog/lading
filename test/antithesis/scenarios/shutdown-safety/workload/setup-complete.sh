#!/usr/bin/env bash
set -euo pipefail

# Notify Antithesis that setup is complete and test commands may start. Antithesis
# sets ANTITHESIS_OUTPUT_DIR automatically; this writes setup_complete to the
# sdk.jsonl file there. Only run once the whole system is ready for testing.

OUTPUT_PATH="/tmp/antithesis_sdk.jsonl"
if [[ -n "${ANTITHESIS_OUTPUT_DIR:-}" ]]; then
  OUTPUT_PATH="${ANTITHESIS_OUTPUT_DIR}/sdk.jsonl"
  echo "Running in Antithesis, emitting setup_complete to ${OUTPUT_PATH}"
elif [[ -n "${ANTITHESIS_SDK_LOCAL_OUTPUT:-}" ]]; then
  OUTPUT_PATH="${ANTITHESIS_SDK_LOCAL_OUTPUT}"
  echo "Antithesis SDK local output override detected, emitting setup_complete to ${OUTPUT_PATH}"
fi

mkdir -p "$(dirname "$OUTPUT_PATH")"
echo '{"antithesis_setup":{"status":"complete","details":{"message":"ready to go"}}}' >> "${OUTPUT_PATH}"
