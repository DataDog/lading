# `lading` - A tool for measuring the performance of daemons.

The `lading` binary in this project is a tool for measuring the performance
behavior of long-running programs -- daemons -- using synthetic load. Consider
the following `lading.yaml`:

```yaml
worker_threads: 2
experiment_duration: 30

telemetry:
  path: "/tmp/foobar.captures"
  global_labels:
    test_global: "blt"

generator:
  tcp:
    seed: [2, 3, 5, 7, 11, 13, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131, 137]
    addr: "0.0.0.0:8282"
    variant: "syslog5424"
    bytes_per_second: "500 Mb"
    block_sizes: ["1Mb", "0.5Mb", "0.25Mb", "0.125Mb", "128Kb"]
    maximum_prebuild_cache_size_bytes: "256 Mb"

target:
  command:
    path: "/usr/bin/foobar"
  arguments: []
  environment_variables: { "LOG_LEVEL": "trace"}
  output:
    stderr: "/tmp/foobar.stderr.log"

blackhole:
  tcp:
    binding_addr: "0.0.0.0:8080"
```

The `generator` defines where load comes from, the `target` where it goes and
`blackhole` where the `target` program's output goes to, if necessary. The
`telemetry` section defines where `lading`'s self-instrumentation and
instrumentation of the `target` is meant to go to. By default `lading` will
expose a prometheus endpoint but in the above lading is in "capture file"
mode. The `generator` in the above emits syslog style lines into the target
`/usr/bin/foobar`. The program `foobar` is run by `lading` as a sub-process with
no arguments but with `LOG_LEVEL=trace` set as an environment variable. It's
stdout is not captured but its stderr is written to
`/tmp/foobar.stderr.log`. The `blackhole` is a simple TCP server on `:8080`.
