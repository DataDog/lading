# Lading with bpftrace Inspector Example

This example demonstrates how to use Lading with bpftrace as an inspector to monitor system-wide process creation and termination events.

## Prerequisites

- Lading installed
- `bpftrace` installed (`apt-get install bpftrace` on Debian/Ubuntu systems)
- Root permissions or appropriate capabilities for bpftrace (requires eBPF access)

## What This Example Does

This configuration:

1. Sets up a simple TCP generator and blackhole to generate and receive traffic
2. Configures bpftrace as an inspector to monitor:
   - Process creation events (execve syscalls)
   - Process termination events (exit and exit_group syscalls)
3. Writes all collected process events to `/tmp/lading-process-events.log`

## Running the Example

```bash
sudo lading --config-path examples/bpftrace-inspector.yaml --prometheus-addr '0.0.0.0:9000'
```

Root permissions are required because bpftrace needs access to eBPF.

## Understanding the Output

After running the example, check `/tmp/lading-process-events.log` to see all process events that occurred during the run. The format is:

```
EXEC: process_name (PID: 1234, PPID: 5678) at timestamp
EXIT: process_name (PID: 1234) at timestamp
```

## Customizing the Example

You can modify the bpftrace script in the `arguments` section to capture different kernel events. The current script tracks:

- `tracepoint:syscalls:sys_enter_execve`: Process creation
- `tracepoint:syscalls:sys_exit_exit` and `tracepoint:syscalls:sys_exit_exit_group`: Process termination

## Using With Your Own Target Application

To monitor a specific target application instead of running the example in no-target mode:

```bash
sudo lading --config-path examples/bpftrace-inspector.yaml --prometheus-addr '0.0.0.0:9000' --target-path /path/to/your/application [app arguments]
```

When targeting a specific application, bpftrace will still monitor all processes, but the `TARGET_PID` environment variable will be available to your inspector, allowing you to filter events specifically related to your target application if needed.

## Security Considerations

This example requires significant system access through eBPF. In production environments, consider:

1. Limiting the scope of traced events
2. Using more targeted bpftrace scripts
3. Implementing appropriate access controls