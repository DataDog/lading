# ADR-001: Generator-Target-Blackhole Architecture

## Status

**Accepted**

## Date

2024-01-01 (formalized from existing design)

## Context

Lading measures performance of long-running programs (daemons) using synthetic,
repeatable load generation. We needed an architecture that:

1. Clearly separates concerns between load generation, target execution, and output consumption
2. Allows independent scaling and configuration of each component
3. Enables deterministic, reproducible performance measurements
4. Minimizes interference between measurement infrastructure and the target being measured

## Decision

Lading operates with three distinct components in a pipeline:

```
Generator --> Target --> Blackhole
```

### Generators

Create deterministic load and push to targets. Generators:

- Pre-compute payloads to minimize runtime overhead
- Support multiple protocols (HTTP, TCP, UDP, gRPC, file I/O, etc.)
- Are rate-controlled via throttles
- Must be "faster" than targets - users must never wonder if lading is the bottleneck

### Targets

The programs being tested, run as subprocesses by lading. Targets:

- Are launched, monitored, and terminated by lading
- Have their resource usage (CPU, memory, I/O) observed
- Receive load from generators
- Send output to blackholes

### Blackholes

Optionally receive and consume output from targets. Blackholes:

- Accept data over various protocols (HTTP, TCP, UDP, Unix sockets, etc.)
- Discard data after optional validation/counting
- Must be fast enough to never become backpressure on the target
- Can report metrics about received data (bytes, messages, etc.)

## Consequences

### Positive

- **Clear mental model**: Each component has a single responsibility
- **Composability**: Mix and match generators/blackholes for different test scenarios
- **Isolation**: Target measurement is isolated from load generation overhead
- **Extensibility**: New protocols can be added as generator or blackhole variants
- **Reproducibility**: Deterministic generators enable repeatable experiments

### Negative

- **Complexity**: Three components to configure vs. a simpler single-process tool
- **Coordination overhead**: Components must be synchronized for experiment timing
- **Resource consumption**: Lading itself consumes resources that could affect measurements

### Neutral

- Configuration files tend to be verbose but explicit
- Each component can be developed and tested independently

## Alternatives Considered

### Single-process load generator

Simpler but conflates generation with measurement. Hard to isolate target performance
from tool overhead.

### External load generation tools (wrk, hey, etc.)

Existing tools lack:
- Deterministic payload generation
- Integrated target lifecycle management
- Unified observation/telemetry
- Protocol coverage for Datadog-specific formats

### Sidecar architecture

Running generators/blackholes as separate processes would add IPC overhead and
complicate coordination. Subprocess model is simpler.

## References

- `lading/src/generator/` - Generator implementations
- `lading/src/blackhole/` - Blackhole implementations
- `lading/src/target/` - Target lifecycle management
- ADR-002: Pre-computation Philosophy (explains why generators pre-compute)
