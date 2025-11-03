# Patterned Log Generator

## Overview

The patterned log generator creates realistic log loads with common patterns, designed to more accurately simulate production logging scenarios compared to purely random data generation.

## Features

### Realistic Patterns

The patterned generator includes:

1. **Common Entities**: Reuses a fixed set of service names, hostnames, and user IDs to simulate realistic log patterns
2. **HTTP Request Patterns**: Generates realistic HTTP methods, paths, and status codes with production-like distributions
3. **Distributed Tracing Support**: Optional trace and span IDs for correlation across log lines
4. **Multiple Log Formats**: Supports JSON, logfmt, and traditional syslog-style formats

### HTTP Status Code Distribution

Status codes follow a realistic distribution:
- 60% - 200 (success)
- 10% - 201 (created)
- 5% - 204 (no content)
- 8% - 400 (bad request)
- 5% - 401 (unauthorized)
- 7% - 404 (not found)
- 3% - 500 (server error)
- 1% - 502 (bad gateway)
- 1% - 503 (service unavailable)

### Log Level Distribution

Log levels follow a weighted distribution:
- 10% - DEBUG
- 64% - INFO
- 18% - WARN
- 8% - ERROR

## Configuration

### Basic Configuration

```yaml
generator:
  - file_gen:
      traditional:
        # ... standard file_gen options ...
        variant:
          patterned:
            service_count: 10        # Number of unique services (default: 10)
            host_count: 20           # Number of unique hosts (default: 20)
            user_count: 100          # Number of unique users (default: 100)
            format: json             # json, logfmt, or traditional (default: json)
            include_trace_ids: true  # Include trace/span IDs (default: false)
```

### Format Options

#### JSON Format

Produces structured JSON logs:

```json
{
  "timestamp": "2024-11-03T10:23:45.123Z",
  "level": "INFO",
  "service": "service-5",
  "host": "host-12.example.com",
  "message": "Request completed",
  "user_id": "user-00042",
  "http": {
    "method": "GET",
    "path": "/api/v1/users",
    "status": 200,
    "duration_ms": 145
  },
  "trace_id": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
  "span_id": "1234567890abcdef"
}
```

#### Logfmt Format

Produces key=value format:

```
timestamp="2024-11-03T10:23:45.123Z" level=INFO service=service-5 host=host-12.example.com message="Request completed" user_id=user-00042 method=GET path=/api/v1/users status=200 duration_ms=145 trace_id=a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6 span_id=1234567890abcdef
```

#### Traditional Format

Produces syslog-style logs:

```
2024-11-03T10:23:45.123Z host-12.example.com INFO: Request completed (service=service-5, user=user-00042)
```

## Examples

### Example 1: Traditional File Generator with Patterned JSON Logs

```yaml
generator:
  - file_gen:
      traditional:
        seed: [13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13]
        path_template: "/tmp/patterned-logs-%NNN%.log"
        duplicates: 3
        maximum_bytes_per_file: "100 MB"
        bytes_per_second: "10 MB"
        maximum_prebuild_cache_size_bytes: "100 MB"
        maximum_block_size: "1 MB"
        rotate: true
        variant:
          patterned:
            service_count: 10
            host_count: 20
            user_count: 100
            format: json
            include_trace_ids: true

blackhole:
  - log_target:
      path: "/tmp/patterned-logs.log"

target:
  type: "no-op"
```

### Example 2: Logrotate Generator with Patterned Logs

```yaml
generator:
  - file_gen:
      logrotate:
        seed: [13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13]
        root: "/tmp/patterned-logs"
        concurrent_logs: 5
        maximum_bytes_per_log: "50 MB"
        total_rotations: 3
        max_depth: 2
        bytes_per_second: "5 MB"
        maximum_prebuild_cache_size_bytes: "100 MB"
        maximum_block_size: "1 MB"
        variant:
          patterned:
            service_count: 15
            host_count: 30
            user_count: 200
            format: json
            include_trace_ids: true

blackhole:
  - log_target:
      path: "/tmp/patterned-blackhole.log"

target:
  type: "no-op"
```

### Example 3: Logfmt Format

```yaml
generator:
  - file_gen:
      traditional:
        seed: [42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42]
        path_template: "/tmp/patterned-logfmt-%NNN%.log"
        duplicates: 2
        maximum_bytes_per_file: "50 MB"
        bytes_per_second: "2 MB"
        maximum_prebuild_cache_size_bytes: "50 MB"
        maximum_block_size: "512 KB"
        rotate: true
        variant:
          patterned:
            service_count: 5
            host_count: 10
            user_count: 50
            format: logfmt
            include_trace_ids: false

blackhole:
  - log_target:
      path: "/tmp/patterned-logfmt-blackhole.log"

target:
  type: "no-op"
```

## Use Cases

### Performance Testing

The patterned generator is ideal for:
- **Load testing log ingestion pipelines** with realistic data patterns
- **Testing log aggregation and analysis systems** with correlated log data
- **Benchmarking search and query performance** with realistic cardinality

### Development and Testing

Use patterned logs to:
- **Develop log parsers and processors** with production-like data
- **Test monitoring and alerting rules** with realistic error rates
- **Validate distributed tracing implementations** with correlated trace IDs

### Capacity Planning

Generate realistic loads to:
- **Estimate storage requirements** for log retention
- **Plan network bandwidth** for log shipping
- **Size compute resources** for log processing

## Advantages Over Random Logs

1. **Realistic Cardinality**: Fixed sets of services, hosts, and users simulate production entity counts
2. **Realistic Distributions**: Status codes and log levels follow production patterns
3. **Correlation Support**: Trace IDs enable testing of distributed tracing scenarios
4. **Pattern Recognition**: Common message templates and entities enable realistic pattern matching tests
5. **Reproducibility**: Seeded generation ensures repeatable test scenarios

## Running the Examples

```bash
# Traditional file generator with JSON
lading --config-path examples/patterned-logs-traditional.yaml

# Logrotate generator
lading --config-path examples/patterned-logs-logrotate.yaml

# Logfmt format
lading --config-path examples/patterned-logs-logfmt.yaml

# Traditional syslog-style format
lading --config-path examples/patterned-logs-traditional-format.yaml
```

## Integration with Existing Generators

The patterned payload works with all existing file generators:
- `traditional`: Simple file rotation
- `logrotate`: Mimics logrotate behavior
- `logrotate_fs`: Filesystem-based logrotate simulation

Simply replace the `variant` configuration in any file_gen configuration to use patterned logs.

## Configuration Tips

### Balancing Realism and Variety

- **Low cardinality** (fewer unique entities) = more pattern repetition = more realistic for microservices
- **High cardinality** (more unique entities) = more variety = realistic for large distributed systems

### Trace ID Usage

Enable `include_trace_ids` when testing:
- Distributed tracing systems
- Log correlation features
- Request flow analysis

### Format Selection

- **JSON**: Best for structured logging systems, easy to parse
- **Logfmt**: Best for simple key-value parsers, human-readable
- **Traditional**: Best for legacy systems, minimal structure

## Performance Considerations

The patterned generator:
- Pre-generates entity lists at startup (minimal overhead)
- Generates log lines on-demand during payload creation
- Supports the same block caching as other payload types
- Has comparable performance to other structured log formats

## Future Enhancements

Potential future improvements:
- Custom message templates
- Time-based patterns (e.g., business hours vs off-hours)
- Error burst simulation
- Custom entity name patterns
- Session-based log grouping
- Geographic distribution of hosts

