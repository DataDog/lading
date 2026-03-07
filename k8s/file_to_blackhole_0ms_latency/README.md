# file_to_blackhole_0ms_latency Experiment

Tests Datadog Agent log tailing under load from lading's `logrotate` file generator.

## Overview

This experiment:
1. Deploys lading to generate rotating log files at `/var/log/lading/`
2. Deploys Datadog Agent configured to tail those log files
3. Routes agent output to a blackhole (lading-intake)
4. Monitors for OOMKills or restarts

Uses the `logrotate` generator (not `logrotate_fs`) which writes to real filesystem
and doesn't require FUSE or privileged containers.

## Prerequisites

- kind: `brew install kind`
- kubectl: `brew install kubectl`
- helm: `brew install helm`
- jq: `brew install jq`
- Docker running

## Usage

```bash
./k8s/file_to_blackhole_0ms_latency/experiment.sh \
    --agent-memory 512 \
    --tags "purpose:test,experiment:file_to_blackhole_0ms_latency"
```

### Options

| Flag | Required | Description |
|------|----------|-------------|
| `--agent-memory` | Yes | Agent container memory limit in MB |
| `--duration` | No | Test duration in seconds (default: 300) |
| `--tags` | Yes | DD_TAGS value for the agent |

## Load Configuration

Default load: 8 concurrent log files at 1.3 MiB/s total throughput.

To modify, edit `manifests/lading.yaml`:

```yaml
throttle:
  stable:
    bytes_per_second: "1.3 MiB"  # Adjust rate
    timeout_millis: 0            # 0 = immediate writes
```

## Results

- **SUCCESS**: Agent survived test duration without restarts
- **FAILURE (OOMKilled)**: Agent needs more memory
- **FAILURE (other)**: Configuration or stability issue

## Cleanup

```bash
kind delete cluster --name lading-test
```
