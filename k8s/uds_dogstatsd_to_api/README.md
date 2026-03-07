# Lading in k8s Demonstration

Testing setup to demonstrate memory limits for Datadog Agent under lading load.

Experiment is rigged up through `experiment.sh`. That script takes multiple
memory parameters for each configured Agent pod container, setting them as
limits in `manifests/datadog-agent.yaml`. Experiment runs for a given duration
-- suggested, 300 seconds at a minimum -- and does two things:

* watches for container restarts during the experiment, signaling failure if one
  is detected or
* executes to experiment duration and queries Prometheus to calculate the peak
  memory consumed by each Agent container, relative to configured limits.

Experiments are **isolated from the internet** to avoid sending metrics et al to
actual Datadog intake. See `manifests/deny-egress.yaml` for details.

## Prerequisites

- kind: `brew install kind`
- kubectl: `brew install kubectl`
- helm: `brew install helm`
- jq: `brew install jq`
- python3: System Python 3
- Docker running

## Usage

### Test a specific memory limit

```bash
# Test 2000 MB total for 5 minutes with explicit per-container limits
./k8s/experiment.sh --total-limit 2000 --agent-memory 1200 --trace-memory 400 --sysprobe-memory 300 --process-memory 100 --tags "purpose:test,limit:2000mb"
```

All memory flags are mandatory and must sum to `--total-limit`, which acts as a check flag.

### To find a minimum memory limit

Run the script multiple times with different limits. Results are:

- **OOMKilled** (FAILURE): Agent needs more memory, script exits
- **Stable** (SUCCESS): Agent survived test duration, cluster kept running for examination

## Manifests

All manifests are in `manifests/` directory. The script uses template
substitution for:

- **manifests/datadog-agent.yaml**: DatadogAgent CRD for Datadog Operator
  - Uses `{{ AGENT_MEMORY_MB }}`, `{{ TRACE_MEMORY_MB }}`, `{{
    SYSPROBE_MEMORY_MB }}`, `{{ PROCESS_MEMORY_MB }}`, and `{{ DD_TAGS }}`
    placeholders
  - Configured for DogStatsD via Unix domain socket at `/var/run/datadog/dsd.socket`
  - Shares `/var/run/datadog` via hostPath with lading pod

- **manifests/lading.yaml**: Lading load generator (lading 0.29.2)
  - ConfigMap with exact config from `uds_dogstatsd_to_api` test
  - Sends 100 MiB/s of DogStatsD metrics
  - High cardinality: 1k-10k contexts, many tags
  - Service with Prometheus scrape annotations for lading metrics

- **manifests/lading-intake.yaml**: Lading intake (blackhole) mimicking Datadog
  API (lading 0.29.2)
  - Receives and discards agent output for self-contained testing

- **manifests/datadog-secret.yaml**: Placeholder secret (fake API key, not validated)
- **manifests/deny-egress.yaml**: NetworkPolicy blocking internet egress (security isolation)

## Test configuration

Taken from
[`datadog-agent/test/regression/cases/uds_dogstatsd_to_api`](https://github.com/DataDog/datadog-agent/blob/main/test/regression/cases/uds_dogstatsd_to_api/lading/lading.yaml). This
experiment is **high stress** for metrics intake and high memory use from
`agent` container is expected.

Adjust lading load generation configuration in the ConfigMap called
`lading-config`. Adjust Agent configuration in `manifests/datadog-agent.yaml`.

## Cleanup

Cluster is left online after script exits. Re-run of `experiment.sh` will
destroy the cluster. Manually clean up the cluster like so:

```bash
kind delete cluster --name lading-test
```

## Notes

- **Agent version**: 7.72.1
- **Lading version**: 0.29.2
- **Agent features enabled**: APM (trace-agent), Log Collection, NPM/system-probe, DogStatsD, Prometheus scrape
