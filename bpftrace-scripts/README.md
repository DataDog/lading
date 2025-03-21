# Lading bpftrace Scripts

This directory contains BPF/bpftrace scripts that can be used with Lading's inspector feature to monitor and observe process behavior during performance testing.

## Available Scripts

### target-process-events.bt

This script monitors process events related to the target application and its child processes:

- Process creation events with command arguments
- Process termination events with exit codes
- Signal reception by processes
- CPU migrations (for performance analysis)
- Major page faults (indicating memory pressure)

The script automatically uses the `TARGET_PID` environment variable that Lading passes to the inspector, allowing it to focus on the target application and its children when run with a specified target.

## Using with Lading

Example configuration for using these scripts can be found in:
- `examples/bpftrace-inspector.yaml` - Basic process monitoring
- `examples/bpftrace-advanced-inspector.yaml` - Advanced process monitoring with the standalone script

## Writing Custom Scripts

When writing custom scripts for use with Lading's inspector:

1. Scripts should check for the `TARGET_PID` environment variable which Lading automatically passes to the inspector
2. Consider providing fallback behavior when `TARGET_PID` is not set (no-target mode)
3. Keep performance impact in mind, especially with high-throughput events

## Installation

When using Lading via the Docker image, these scripts are automatically installed to `/usr/share/lading/bpftrace-scripts/` and are ready to use with the example configurations.

For local development or custom installations, ensure that bpftrace is installed and that these scripts are available at a location that matches the path specified in your Lading configuration.