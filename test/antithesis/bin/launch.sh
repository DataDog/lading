#!/usr/bin/env bash
# Generic Antithesis launcher shared by every lading scenario.
#
#   ./launch.sh <scenario> [extra snouty flags]
#
# <scenario> names a directory under `test/antithesis/scenarios/` holding a
# docker-compose.yaml and a launch.env. launch.env supplies the per-scenario
# bits; everything else -- image tagging, the fault profile shape,
# build-before-submit -- is common and lives here so every shot is identical and
# comparable and no fault flag is ever fumbled or forgotten. Change a shot's
# faults by editing launch.env's node list, not by passing one-off flags.
#
# launch.env, sourced from the scenario directory, sets:
#   SCENARIO_TEST_NAME      test name reported to Antithesis
#   SCENARIO_DESCRIPTION    human description; the git commit is appended
#   SCENARIO_FAULT_NODES    space-separated SUT container names to node-fault;
#                           empty means no node termination/hang/throttle at all
#   SCENARIO_WEBHOOK        optional; tenant webhook, default run_test
#
# Required environment, read by snouty:
#   ANTITHESIS_TENANT       tenant name
#   ANTITHESIS_API_KEY      api key, or ANTITHESIS_USERNAME + ANTITHESIS_PASSWORD
#   ANTITHESIS_REPOSITORY   registry to push the built config + service images to
#
# Optional overrides win over launch.env / defaults:
#   DURATION=<minutes>      default 30
#   TEST_NAME=<name>        default SCENARIO_TEST_NAME
#   DESCRIPTION=<text>      default SCENARIO_DESCRIPTION; commit is appended
#   FAULT_NODES=<names>     default SCENARIO_FAULT_NODES
#   WEBHOOK=<name>          default SCENARIO_WEBHOOK or run_test
#   SOURCE=<identifier>     antithesis.source, default datadog_agent
#   SIMULTANEOUS_FAULTS=<bool>       default false. Same node faults on all
#                                    FAULT_NODES at once, no-op without FAULT_NODES
#   FORCE_DISABLE_ALL_FAULTS=<bool>  default false. Master switch over every
#                                    fault above, runs with none
#   DRY_RUN=1               print the exact command and exit without submitting
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ANTITHESIS_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$ANTITHESIS_DIR/../.." && pwd)"

SCENARIO="${1:?usage: launch.sh <scenario> [extra snouty flags]}"
shift || true
SCENARIO_DIR="$ANTITHESIS_DIR/scenarios/$SCENARIO"
[ -d "$SCENARIO_DIR" ] || { echo "error: no scenario directory $SCENARIO_DIR" >&2; exit 1; }
[ -f "$SCENARIO_DIR/docker-compose.yaml" ] || { echo "error: $SCENARIO_DIR/docker-compose.yaml not found" >&2; exit 1; }
[ -f "$SCENARIO_DIR/launch.env" ] || { echo "error: $SCENARIO_DIR/launch.env not found" >&2; exit 1; }

# Per-scenario settings. Declared here so a missing one is caught, not silently empty.
SCENARIO_TEST_NAME=""
SCENARIO_DESCRIPTION=""
SCENARIO_FAULT_NODES=""
SCENARIO_WEBHOOK=""
# shellcheck source=/dev/null
. "$SCENARIO_DIR/launch.env"

# Immutable per-build revision: the short commit, marked -dirty when the working
# tree has uncommitted changes so the tag never claims to be a clean commit it is
# not. Images are tagged by this, never :latest, so a shot can never reuse a stale
# mutable tag and every pushed image traces back to the source it was built from.
GIT_SHA="$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
if [[ -n "$(git -C "$REPO_ROOT" status --porcelain 2>/dev/null)" ]]; then
  GIT_SHA="${GIT_SHA}-dirty"
fi
export ANTITHESIS_IMAGE_TAG="$GIT_SHA"

WEBHOOK="${WEBHOOK:-${SCENARIO_WEBHOOK:-run_test}}"
DURATION="${DURATION:-30}"
TEST_NAME="${TEST_NAME:-${SCENARIO_TEST_NAME:?launch.env must set SCENARIO_TEST_NAME}}"
DESCRIPTION="${DESCRIPTION:-$SCENARIO_DESCRIPTION} (commit ${GIT_SHA})"
# May be empty: an empty node list means no node faults for this scenario.
FAULT_NODES="${FAULT_NODES-$SCENARIO_FAULT_NODES}"
SIMULTANEOUS_FAULTS="${SIMULTANEOUS_FAULTS:-false}"
FORCE_DISABLE_ALL_FAULTS="${FORCE_DISABLE_ALL_FAULTS:-false}"

# antithesis.source groups this project's property history and selects tenant
# customizations. Fixed to `datadog_agent` to match saluki on the shared datadog
# tenant: it selects the tenant customizations and the tracked property-history
# bucket. Without a recognized source snouty runs ephemeral and surfaces no
# findings to triage.
SOURCE="${SOURCE:-datadog_agent}"

# Pinned fault profile, submitted to the run_test endpoint. cpu_mod and
# clock_jitter are global and symmetric, so every scenario gets them. Network
# faults stay on everywhere and heal before judging. Node termination, hang, and
# throttle apply only to the containers in FAULT_NODES, so a scenario gets none by
# leaving it empty.
FAULTS=(
  --param custom.cpu_mod=true
  --param custom.clock_jitter=true
  --param custom.force_disable_all_faults="$FORCE_DISABLE_ALL_FAULTS"
)
if [[ -n "$FAULT_NODES" ]]; then
  FAULTS+=(
    --param custom.include_for_node_termination="$FAULT_NODES"
    --param custom.include_for_node_hang="$FAULT_NODES"
    --param custom.include_for_node_throttle="$FAULT_NODES"
    --param custom.simultaneous_faults="$SIMULTANEOUS_FAULTS"
  )
fi

for v in ANTITHESIS_TENANT ANTITHESIS_REPOSITORY; do
  if [[ -z "${!v:-}" ]]; then
    echo "error: $v is not set (required to build and submit the run)" >&2
    exit 1
  fi
done

# Build the images with the concrete tag so snouty reuses them instead of
# rebuilding, and so a shot can never ship a stale mutable tag. lading has no
# converged base image, so this is a plain compose build; layer caching keeps it
# near-instant when nothing changed.
build=(docker compose -f "$SCENARIO_DIR/docker-compose.yaml" build)

# Launch from a rendered copy so the image tag is concrete. snouty ships the compose
# uninterpolated, so an `${ANTITHESIS_IMAGE_TAG:-latest}` tag reaches the platform as
# the never-pushed `:latest`; `docker compose config` bakes in the tag snouty pushed.
LAUNCH_DIR="$SCENARIO_DIR/.launch"
render=(docker compose -f "$SCENARIO_DIR/docker-compose.yaml" config)

cmd=(snouty launch
  --webhook "$WEBHOOK"
  --config "$LAUNCH_DIR"
  --test-name "$TEST_NAME"
  --description "$DESCRIPTION"
  --source "$SOURCE"
  --duration "$DURATION"
  "${FAULTS[@]}"
  "$@")

printf 'build:'; printf ' %q' "${build[@]}"; printf '\n'
printf 'render:'; printf ' %q' "${render[@]}"; printf ' > %q\n' "$LAUNCH_DIR/docker-compose.yaml"
printf 'launch:'; printf ' %q' "${cmd[@]}"; printf '\n'
if [[ "${DRY_RUN:-0}" == "1" ]]; then
  echo "(dry run; not building or submitting)"
  exit 0
fi
"${build[@]}"
mkdir -p "$LAUNCH_DIR"
"${render[@]}" >"$LAUNCH_DIR/docker-compose.yaml"
exec "${cmd[@]}"
