# Fuzzing with Lading

Lading includes a built-in fuzzing generator that can create malformed/mutated payloads to test target resilience and discover crashes or vulnerabilities.

## Overview

The fuzzing generator wraps existing payload types (JSON, OpenTelemetry, etc.) and applies various mutation strategies while maintaining determinism through explicit seeding. This allows for:

- **Reproducible fuzzing sessions**: Same seed produces same mutations
- **Black-box testing**: No target instrumentation required
- **Protocol-aware fuzzing**: Format-specific mutations available
- **High performance**: Pre-computed payloads with mutations applied

## Configuration

### Basic Structure

```yaml
generator:
  - fuzzing:
      seed: [1, 2, 3, ...]  # 32-byte seed for determinism
      addr: "target:port"   # Target address
      variant: json         # Base payload type
      bytes_per_second: "10 MiB"
      mutation:
        probability: 0.2    # 20% of payloads mutated
        strategies:         # List of mutation strategies
          - bit_flip:
              probability: 0.1
```

### Mutation Strategies

1. **BitFlip**: Randomly flips bits in bytes
   ```yaml
   - bit_flip:
       probability: 0.1  # Probability per byte
   ```

2. **ByteShuffle**: Swaps random byte positions
   ```yaml
   - byte_shuffle:
       operations: 5     # Number of swaps
   ```

3. **Truncate**: Reduces payload size
   ```yaml
   - truncate:
       min_ratio: 0.3    # Keep at least 30%
   ```

4. **ByteInsertion**: Inserts random bytes
   ```yaml
   - byte_insertion:
       max_bytes: 10     # Maximum bytes to insert
   ```

5. **ByteReplacement**: Replaces bytes with random values
   ```yaml
   - byte_replacement:
       probability: 0.05 # Probability per byte
   ```

## Use Cases

### 1. Testing OpenTelemetry Collector

```yaml
generator:
  - fuzzing:
      addr: "otel-collector:4317"
      variant:
        opentelemetry_logs:
          context:
            total_contexts: 100
            logs_per_scope:
              min: 1
              max: 50
      mutation:
        probability: 0.4
        strategies:
          - truncate:
              min_ratio: 0.1  # Aggressive truncation
          - bit_flip:
              probability: 0.2
```

### 2. JSON API Testing

```yaml
generator:
  - fuzzing:
      addr: "api-server:8080"
      variant: json
      mutation:
        probability: 0.3
        strategies:
          - bit_flip:
              probability: 0.1
          - byte_insertion:
              max_bytes: 100  # Test buffer overflows
```

### 3. Container-Based Fuzzing

When fuzzing targets in separate containers:

```yaml
# docker-compose.yml
services:
  lading:
    image: lading:latest
    command: ["--config", "/config/fuzzing.yaml"]
    volumes:
      - ./fuzzing.yaml:/config/fuzzing.yaml
    networks:
      - fuzzing-net

  target:
    image: otel/opentelemetry-collector:latest
    networks:
      - fuzzing-net
    # Add health checks to detect crashes

networks:
  fuzzing-net:
```

## Crash Detection

While lading doesn't have built-in crash detection for external targets, you can:

1. **Monitor connection failures**: Tracked in `connection_failure` metric
2. **Use health checks**: External monitoring of target health
3. **Log analysis**: Parse target logs for panics/errors
4. **Container monitoring**: Track exit codes and restarts

## Integration with AFL++/LibAFL

For advanced coverage-guided fuzzing:

1. **Instrument target**: Compile with AFL++ instrumentation
2. **Export coverage**: Use shared memory or files
3. **Adaptive mutations**: Adjust strategies based on coverage
4. **Corpus management**: Save interesting inputs

Example integration approach:

```yaml
# Future enhancement - not yet implemented
generator:
  - fuzzing:
      coverage_feedback:
        type: "afl_shm"
        shm_id: 12345
      corpus_dir: "/tmp/corpus"
      mutation:
        adaptive: true  # Adjust based on coverage
```

## Performance Considerations

1. **Pre-computation**: Base payloads are pre-computed
2. **Mutation overhead**: Minimal - applied during send
3. **Memory usage**: Controlled by `maximum_prebuild_cache_size_bytes`
4. **Throughput**: Limited by `bytes_per_second` setting

## Metrics

The fuzzing generator emits:

- `bytes_written`: Successfully sent bytes
- `packets_sent`: Number of packets sent
- `mutations_applied`: Count of mutated payloads
- `connection_failure`: Failed connection attempts
- `request_failure`: Failed write operations

## Best Practices

1. **Start conservative**: Begin with low mutation probability
2. **Increase gradually**: Ramp up aggression over time
3. **Save seeds**: Record seeds that find crashes
4. **Monitor resources**: Watch target memory/CPU usage
5. **Use deterministic mode**: Same seed for reproduction
6. **Combine strategies**: Multiple mutations find more bugs

## Future Enhancements

Planned improvements include:

- Format-aware mutations (JSON, Protobuf)
- Coverage-guided fuzzing integration
- Crash artifact collection
- Automatic minimization
- Distributed fuzzing support