#!/usr/bin/env bash
# Truncation flag bug demo — UTF-16-LE log file via lading_proptest.
#
# Runs the truncation_flag_main and truncation_flag_control tests against
# a real DD Agent container, collects the per-case artifacts (config,
# input bytes, output entries), and writes them into a structured output
# directory along with an analysis report.
#
# Usage:
#   ./run.sh [--output-dir <path>] [--agent-image <image>] [--docker-host <socket>]
#
# Defaults:
#   --output-dir    ./truncation_flag_demo_output (relative to current dir)
#   --agent-image   datadog/agent:7.78.2-full
#   --docker-host   auto-detected (Colima socket if present, else system default)

set -euo pipefail

# --- Defaults ---
OUTPUT_DIR="$(pwd)/truncation_flag_demo_output"
AGENT_IMAGE="datadog/agent:7.78.2-full"
DOCKER_HOST_ARG=""

# Auto-detect Colima docker socket on macOS
if [[ -S "$HOME/.colima/default/docker.sock" ]]; then
    DOCKER_HOST_ARG="unix://$HOME/.colima/default/docker.sock"
fi

# --- Arg parsing ---
while [[ $# -gt 0 ]]; do
    case "$1" in
        --output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --agent-image)
            AGENT_IMAGE="$2"
            shift 2
            ;;
        --docker-host)
            DOCKER_HOST_ARG="$2"
            shift 2
            ;;
        -h|--help)
            sed -n '2,16p' "$0"
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            echo "Use --help for usage." >&2
            exit 1
            ;;
    esac
done

# --- Locate the crate ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CRATE_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

if [[ ! -f "$CRATE_DIR/Cargo.toml" ]]; then
    echo "ERROR: Could not find lading_proptest Cargo.toml at $CRATE_DIR" >&2
    exit 2
fi

# --- Intro ---
cat <<'INTRO'

========================================================================
 Datadog Agent Truncation Flag Bug — Reproduction Demo
========================================================================

This demo proves that the agent's CombiningAggregator silently drops
the upstream IsTruncated flag set by the framer when:

  * a log line is framer-truncated (raw bytes exceed max_message_size_bytes)
  * a byte-shrinking parser (here: UTF-16-LE → UTF-8) reduces the post-parse
    byte count below max_message_size_bytes
  * the line is processed via Path C (auto_multi_line_detection: true) and
    hits bucket.flush() with lineCount=1

In bucket.flush(), the truncation-marker decision uses only the size check
(b.contentLen >= b.maxContentSize) — it does NOT honor the upstream flag.
Compare to bucket.emitSingle() at line 130 of combining_aggregator.go,
which OR's msg.ParsingExtra.IsTruncated into the same decision.

Two cases run with identical input bytes; only the routing path differs:

  CONTROL  auto_multi_line_detection: false  (Path A SingleLineHandler)
           Expected: ...TRUNCATED... marker AND truncated:single_line tag

  MAIN     auto_multi_line_detection: true   (Path C CombiningAggregator)
           Expected: marker MISSING AND tag MISSING — this is the bug

========================================================================

INTRO

# --- Print configuration ---
echo "Configuration:"
echo "  Output directory: $OUTPUT_DIR"
echo "  Agent image:      $AGENT_IMAGE"
if [[ -n "$DOCKER_HOST_ARG" ]]; then
    echo "  Docker host:      $DOCKER_HOST_ARG"
else
    echo "  Docker host:      (system default)"
fi
echo "  Crate dir:        $CRATE_DIR"
echo

# --- Confirm prerequisites ---
echo "Checking prerequisites..."

if ! command -v cargo >/dev/null 2>&1; then
    echo "ERROR: cargo not found — install Rust toolchain" >&2
    exit 2
fi

if ! command -v docker >/dev/null 2>&1; then
    echo "ERROR: docker not found" >&2
    exit 2
fi

# Check the agent image is available locally
if [[ -n "$DOCKER_HOST_ARG" ]]; then
    DOCKER_HOST="$DOCKER_HOST_ARG" docker image inspect "$AGENT_IMAGE" >/dev/null 2>&1 || {
        echo "Pulling $AGENT_IMAGE..."
        DOCKER_HOST="$DOCKER_HOST_ARG" docker pull "$AGENT_IMAGE"
    }
else
    docker image inspect "$AGENT_IMAGE" >/dev/null 2>&1 || {
        echo "Pulling $AGENT_IMAGE..."
        docker pull "$AGENT_IMAGE"
    }
fi

echo "  Prerequisites OK"
echo

# --- Determine where the tests will write their artifacts ---
#
# On macOS with Colima/Lima, Docker bind mounts only see paths under $HOME.
# Default the test artifacts location to a subdirectory of $HOME if the
# requested OUTPUT_DIR is outside of it.
TESTS_TMP_BASE="$OUTPUT_DIR/.test_artifacts"

# If OUTPUT_DIR is outside $HOME, warn and fall back to a $HOME location for
# the test artifacts (we'll still copy them into OUTPUT_DIR at the end).
case "$OUTPUT_DIR" in
    "$HOME"/*)
        # Output dir is under $HOME — safe to also put test artifacts there.
        ;;
    *)
        FALLBACK_TMP_BASE="$HOME/.lading_proptest_tmp_demo"
        if [[ "$DOCKER_HOST_ARG" == *colima* ]] || [[ "$(uname -s)" == "Darwin" ]]; then
            echo "Note: --output-dir is outside \$HOME. Tests will write artifacts to"
            echo "      $FALLBACK_TMP_BASE (visible to Docker bind mounts), then copy"
            echo "      them to $OUTPUT_DIR at the end."
            echo
            TESTS_TMP_BASE="$FALLBACK_TMP_BASE"
        fi
        ;;
esac

# Clean the test artifacts dir so we know which dirs are from this run.
rm -rf "$TESTS_TMP_BASE"
mkdir -p "$TESTS_TMP_BASE"

# --- Run the tests ---
echo "Running both tests under cargo (each spins up a fresh agent container)..."
echo "Expected runtime: ~2 minutes."
echo "Test artifacts will be written to: $TESTS_TMP_BASE"
echo

cd "$CRATE_DIR"

# Use a subshell so env vars don't leak out
set +e
(
    export LADING_KEEP_TEMP=1
    export LADING_TMP_BASE="$TESTS_TMP_BASE"
    export PROPTEST_MAX_SHRINK_ITERS=0
    export PROPTEST_CASES=1
    export DD_AGENT_IMAGE="$AGENT_IMAGE"
    if [[ -n "$DOCKER_HOST_ARG" ]]; then
        export DOCKER_HOST="$DOCKER_HOST_ARG"
    fi
    export RUST_LOG=info
    cargo test -p lading_proptest --lib truncation_flag -- --nocapture --test-threads=1
)
TEST_EXIT=$?
set -e

# We expect the main test to FAIL (bug demo) and control to PASS, so the
# overall cargo exit code is non-zero. That's expected.
echo
echo "(cargo exit code: $TEST_EXIT — non-zero is expected: the main test fails because the bug exists)"
echo

# --- Find the run's temp dirs ---
NEW_DIRS=()
while IFS= read -r dir; do
    [[ -n "$dir" ]] && NEW_DIRS+=("$dir")
done < <(ls -1 "$TESTS_TMP_BASE" 2>/dev/null)

if [[ ${#NEW_DIRS[@]} -eq 0 ]]; then
    echo "ERROR: no test artifacts found in $TESTS_TMP_BASE — did the tests run?" >&2
    exit 2
fi

# --- Classify each as main or control by inspecting the source config ---
MAIN_DIR=""
CONTROL_DIR=""
for dir in "${NEW_DIRS[@]}"; do
    full_dir="$TESTS_TMP_BASE/$dir"
    src_yaml="$full_dir/config/conf.d/proptest.d/conf.yaml"
    if [[ ! -f "$src_yaml" ]]; then
        continue
    fi
    if grep -q 'auto_multi_line_detection: true' "$src_yaml" 2>/dev/null; then
        MAIN_DIR="$full_dir"
    else
        CONTROL_DIR="$full_dir"
    fi
done

if [[ -z "$MAIN_DIR" || -z "$CONTROL_DIR" ]]; then
    echo "ERROR: could not identify both main and control temp dirs" >&2
    echo "  main:    ${MAIN_DIR:-<not found>}" >&2
    echo "  control: ${CONTROL_DIR:-<not found>}" >&2
    echo "  found dirs: ${NEW_DIRS[*]}" >&2
    exit 2
fi

# --- Copy artifacts into the output directory with clear names ---
mkdir -p "$OUTPUT_DIR"
rm -rf "$OUTPUT_DIR/control" "$OUTPUT_DIR/main"
mkdir -p "$OUTPUT_DIR/control" "$OUTPUT_DIR/main"
cp -R "$CONTROL_DIR"/* "$OUTPUT_DIR/control/"
cp -R "$MAIN_DIR"/* "$OUTPUT_DIR/main/"

# If we used the fallback $HOME location, clean it up now that we've copied.
if [[ "$TESTS_TMP_BASE" != "$OUTPUT_DIR/.test_artifacts" ]]; then
    rm -rf "$TESTS_TMP_BASE"
fi

# --- Helper to render one case ---
render_case() {
    local title="$1"
    local dir="$2"
    local expected_marker="$3"  # "PRESENT" or "MISSING"

    echo "========================================================================"
    echo "  $title"
    echo "========================================================================"
    echo
    echo "Artifacts: $dir"
    echo

    # Source config
    echo "------ Source config (conf.d/proptest.d/conf.yaml) ------"
    cat "$dir/config/conf.d/proptest.d/conf.yaml"
    echo

    # Relevant agent config lines
    echo "------ Agent config (relevant fields) ------"
    grep -E 'max_message_size_bytes|tag_truncated_logs|auto_multi_line_detection|logs_no_ssl|use_compression' \
        "$dir/config/datadog.yaml" || true
    echo

    # Input bytes (hex preview)
    echo "------ Input bytes (first 96 bytes, hex + ASCII) ------"
    if command -v xxd >/dev/null 2>&1; then
        xxd "$dir/logs/proptest.log" | head -6
    else
        od -An -tx1z -w16 "$dir/logs/proptest.log" | head -6
    fi
    local total_bytes
    total_bytes=$(wc -c < "$dir/logs/proptest.log" | tr -d ' ')
    echo "..."
    echo "(Total file size: $total_bytes bytes — UTF-16-LE encoded ASCII line)"
    echo

    # Output entries
    echo "------ Output entries received at intake ------"
    if [[ -s "$dir/output_messages.txt" ]]; then
        cat "$dir/output_messages.txt" | head -20
    else
        echo "(no output entries — check $dir/output.json)"
    fi
    echo

    # Tags from output.json
    echo "------ Output ddtags ------"
    if [[ -s "$dir/output.json" ]] && command -v python3 >/dev/null 2>&1; then
        python3 -c "
import json
with open('$dir/output.json') as f:
    entries = json.load(f)
for i, e in enumerate(entries):
    print(f'  [{i}] ddtags={e.get(\"ddtags\")!r}')
"
    fi
    echo

    # Marker / tag presence check
    echo "------ Truncation evidence ------"
    local marker_count tag_count
    marker_count=$(grep -c '\.\.\.TRUNCATED\.\.\.' "$dir/output_messages.txt" 2>/dev/null || echo 0)
    tag_count=$(grep -o 'truncated:[a-z_]*' "$dir/output.json" 2>/dev/null | wc -l | tr -d ' ' || echo 0)

    echo "  '...TRUNCATED...' marker occurrences in output: $marker_count"
    echo "  'truncated:*' tag occurrences in output:        $tag_count"

    if [[ "$expected_marker" == "PRESENT" ]]; then
        if [[ "$marker_count" -gt 0 && "$tag_count" -gt 0 ]]; then
            echo "  Result: ✓ marker AND tag present (as expected for this path)"
        else
            echo "  Result: ✗ UNEXPECTED — marker or tag missing on this path"
        fi
    else
        if [[ "$marker_count" -eq 0 && "$tag_count" -eq 0 ]]; then
            echo "  Result: ✗ marker AND tag MISSING — bug confirmed"
        else
            echo "  Result: ✓ marker/tag present (bug NOT reproduced on this path)"
        fi
    fi
    echo
}

# --- Render both cases (control first since it's the baseline) ---
render_case "CONTROL — Path A SingleLineHandler (auto_multi_line_detection: false)" \
    "$OUTPUT_DIR/control" "PRESENT"

render_case "MAIN — Path C CombiningAggregator (auto_multi_line_detection: true)" \
    "$OUTPUT_DIR/main" "MISSING"

# --- Final analysis ---
cat <<ANALYSIS
========================================================================
  Analysis
========================================================================

Both cases received the same input bytes (UTF-16-LE encoded ASCII line
of ~700 chars, raw size ~1402 bytes including the \\n\\0 line terminator)
through the same agent version with max_message_size_bytes: 1024.

Pipeline trace (both cases):
  1. Framer reads UTF-16-LE bytes from the file.
     Raw line size > contentLenLimit (1024) → framer cuts at 1024,
     sets msg.ParsingExtra.IsTruncated = true.
  2. encodedtext parser decodes UTF-16-LE → UTF-8.
     ASCII content halves: post-parse byte count ≈ 512.

Case divergence at the line handler:

  CONTROL  Path A SingleLineHandler.process()
           - Calls applyTruncation(..., shouldTruncate =
             (len(content) > maxContentSize || msg.ParsingExtra.IsTruncated),
             "single_line")
           - Upstream IsTruncated is honored.
           - Marker bytes appended, truncated:single_line tag added.

  MAIN     Path C CombiningAggregator.Process()
           - Label = aggregate (no \`{\` so JSON detector misses,
             no timestamp shape so datetime detector misses).
           - Bucket is empty → hits the aggregate-on-empty-bucket path:
             bucket.add(msg) + flushToCollected() → bucket.flush() with
             lineCount = 1.
           - bucket.flush() calls applyTruncation(..., shouldTruncate =
             (b.contentLen >= b.maxContentSize), ...).
           - b.contentLen ≈ 512, b.maxContentSize = 1024 → false.
           - msg.ParsingExtra.IsTruncated is never read.
           - No marker, no tag.

Bug location: combining_aggregator.go:103.

Conditions to manifest:
  1. Framer truncates (raw line > contentLenLimit)
  2. Parser shrinks bytes successfully on truncated input
     (encodedtext does this; dockerfile parser does NOT because it
     validates JSON and fails on cut input)
  3. Flow routes through bucket.flush()

The bug exists for all bucket.flush() invocation paths. The aggregate-
on-empty-bucket path was the simplest to trigger and is what this demo
exercises. The same bug is reachable via:
  - noAggregate single-line emit
  - normal combined emission (lineCount > 1)
  - oversized startGroup
  - external Flush() at end-of-stream

========================================================================
  Where to find the artifacts
========================================================================

$OUTPUT_DIR
├── control/
│   ├── config/
│   │   ├── datadog.yaml                       — main agent config
│   │   └── conf.d/proptest.d/conf.yaml        — source config
│   ├── logs/proptest.log                      — UTF-16-LE input bytes
│   ├── output.json                            — full intake payloads
│   ├── output_messages.txt                    — one entry per line
│   └── summary.txt                            — action sequence
└── main/
    └── ... (same layout)

To inspect input bytes:
  xxd $OUTPUT_DIR/main/logs/proptest.log | head -10

To compare ddtags side-by-side:
  python3 -c "import json; print(json.dumps(json.load(open('$OUTPUT_DIR/control/output.json')), indent=2))" | head -30
  python3 -c "import json; print(json.dumps(json.load(open('$OUTPUT_DIR/main/output.json')), indent=2))" | head -30

========================================================================
ANALYSIS

echo "Done."
