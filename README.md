# `lading` - A tool for measuring the performance of long-running programs.

The `lading` project is a tool for measuring the performance behavior of
long-running programs -- daemons, often -- using synthetic, repeatable load
generation across a variety of protocols. The ambition is to be a worry-free
component of a larger performance testing strategy for complex programs. The
[Vector][vector] project uses lading in their 'soak' tests.

## Development Setup

`lading` requires the protobuf compiler to build. See
[installation instructions](https://grpc.io/docs/protoc-installation/) from the
protobuf docs.

For criterion benchmarks, you can run them via `cargo bench`.
[`cargo-criterion`](https://github.com/bheisler/cargo-criterion)
 is a more advanced cargo extension that provides
historical (ie baseline) tracking functionality.

## Operating Model

`lading` operates on three conceptual components:

* generators,
* target and
* blackholes

A "generator" in `lading` is responsible for creating load and "pushing" it into
the target. The "target" in is the program that `lading` runs as a sub-process,
collecting resource consumption data about the target from the operating system
but also self-telemetry regarding generation. The "blackhole" exists for targets
to push load into, when necessary, allowing for a target to be run and examined
by `lading` without including external programs or APIs into the setup.

As much as possible `lading` pre-computes any generator and blackhole payloads,
intending to minimize runtime overhead compared to the target program. Users
must provide a seed to `lading` -- see below -- ensuring that payloads are
generated in a repeatable manner across `lading` runs. While pre-computation
does mean that `lading` is capable of outpacing many targets with minimal
runtime interference it does also mean higher memory use compared to other load
generation tools.

## Configuration

The configuration of `lading` is split between a configuration file in YAML
format and command-line options. Generators and blackholes are configured in the
config file, targets on the command line. This is, at first glance, awkward but
does allow for `lading` to be used in dynamic environments like CI without
foreknowledge of the target.

That "push" can be as direct as network IO into the target or as indirect as
doing file operations for a target who's tracing with BPF.

Consider the following `lading.yaml`:

```yaml
generator:
  - http:
      seed: [2, 3, 5, 7, 11, 13, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131, 137]
      target_uri: "http://localhost:8282/"
      bytes_per_second: "100 Mb"
      parallel_connections: 10
      method:
        post:
          variant: "fluent"
          maximum_prebuild_cache_size_bytes: "256 Mb"
      headers: {}

blackhole:
  - http:
      binding_addr: "0.0.0.0:8080"
```

In this setup `lading` is configured to run one generator and one blackhole. In
general, at least one generator is required and zero or more blackholes are. The
generator here, named "http", uses a fixed seed to repeatably produce [fluent's
forward protocol][fluent] instances at 100 Mb per second into the target, with a
pre-built size of 256 Mb. That is, `lading` will _attempt_ to push at a fixed
throughput, although the target might not be able to cope with that load and
`lading` will consume 256 Mb of RAM to accommodate pre-build payloads. The
blackhole in this configuration responds with an empty body 200 OK.

`lading` supports two types of targets, binary launch mode and PID watch mode.
In binary launch mode, `lading` acts like a wrapper around the target. To use
this mode, one specifies where on disk the configuration is, the path to the
target and its arguments. `--target-stderr-path` and `--target-stdout-path`
allow the target's stdout, stderr to be forwarded into files. At runtime
`lading` will output diagnostic information on its stdout about the current
state of the target.

In PID watch mode, `lading` monitors a process that is already running on the
system. `lading` does not handle any target orchestration in this mode. The
target should be launched before running `lading` and terminated once `lading`
exits.

The data `lading` captures at runtime can be pulled by polling `lading`'s
prometheus endpoint -- configurable with `--prometheus-addr` -- or can be
written to disk by lading by specifying `--capture-path`. The captured data,
when written to disk, is newline delimited json payloads.

Logs about lading's operation can be controlled using the standard `RUST_LOG`
environment variable. eg `RUST_LOG=debug ./lading` will emit logs at `debug`
and above (ie, `info`, `warn`, `error`).

## Contributing

See [Contributing][contributing].

## License

Licensed under the [MIT license][mit-license].

[contributing]: CONTRIBUTING.md
[mit-license]: LICENSE
[vector]: github.com/vectordotdev/vector
[fluent]: https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1
