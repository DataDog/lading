# Advanced bpftrace Inspector for Process Monitoring

This example demonstrates a more sophisticated use of bpftrace as a Lading inspector to monitor detailed process behavior focusing on a target process and its children.

## Prerequisites

- Lading installed
- `bpftrace` installed (`apt-get install bpftrace` on Debian/Ubuntu systems)
- Root permissions or appropriate capabilities for bpftrace

## Features of this Example

This configuration:

1. Uses a standalone bpftrace script (`/usr/share/lading/bpftrace-scripts/target-process-events.bt`) for more structured process monitoring
2. Automatically focuses on the target process and its children when run with `--target-path`
3. Tracks detailed process events:
   - Process creation with command arguments
   - Process terminations with exit codes
   - Signal reception
   - CPU migrations
   - Major page faults
4. Provides a summary of events when Lading terminates

## Running the Example

### With a Target Application

```bash
sudo lading --config-path examples/bpftrace-advanced-inspector.yaml --prometheus-addr '0.0.0.0:9000' --target-path /path/to/your/application [app arguments]
```

### In No-Target Mode (monitors all processes)

```bash
sudo lading --config-path examples/bpftrace-advanced-inspector.yaml --prometheus-addr '0.0.0.0:9000' --no-target
```

## Understanding the Output

The bpftrace script will log detailed process events to `/tmp/lading-target-process-events.log` including:

- Process creations (EXEC events) with command arguments
- Process terminations (EXIT events) with exit codes
- Signals received by processes
- CPU migrations for performance analysis
- Major page faults (indicating memory pressure)

At the end of the run, a summary of events will be logged.

## Customizing the bpftrace Script

The standalone script at `/usr/share/lading/bpftrace-scripts/target-process-events.bt` can be modified to:

1. Track additional events by adding more probe points
2. Filter different process types
3. Collect performance statistics
4. Add histograms or other bpftrace features

## Performance Considerations

This script is more resource-intensive than the basic example due to the additional probes and tracking. In production environments, you may want to:

1. Be selective about which events you monitor
2. Increase buffer sizes for high-volume events using `BPFTRACE_PERF_RB_PAGES`
3. Apply filtering to reduce overhead

## Debugging

If bpftrace encounters errors, they will be logged to `/tmp/lading-bpftrace-errors.log`.

## Integrating with Other Monitoring

This example also includes optional `target_metrics` configuration that can scrape Prometheus metrics from node_exporter (if available), allowing you to correlate process events with system metrics.