# `http_gen` - An http load generating program

This program generates payloads and POSTs them to configured targets. The goal
is to provide stable throughput without coordination with the test subject.

## Configuration

This program is primarily configured through its config file. Here is an
example:

```
worker_threads = 10
prometheus_addr = "0.0.0.0:9001"

[targets.simple]
target_uri = "http://localhost:8282/v1/input"
bytes_per_second = "500 Mb"
parallel_connections = 100
method.type = "Post"
method.variant = "DatadogLog"
method.maximum_prebuild_cache_size_bytes = "500 Mb"

[targets.simple.headers]
dd-api-key = "deadbeef"
```

This creates a single target 'simple' that emits datadog log agent payloads to
localhost:8282/v1/input at 500Mb per second with 100 parallel connections.

## Telemetry

This program self-instruments through prometheus metrics. This is subject to
change and we do not document exactly what metrics are available, though you may
find out for yourself by examining [`src/lib.rs`](./src/lib.rs) if you need.

## Weird Quirks

This program's configuration does not understand byte sizes greater than `u32`
bytes. Ultimately this is a limitation inherited from the
[`governor`](https://github.com/antifuchs/governor) dependency. If this
limitation needs to be lifted we'll have to contribute a fix upstream, or adjust
our rate limiting approach.
