# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased
### Fixed
- The target metrics prometheus parser now handles labels that have spaces in
  them rather than incorrectly identifying the metric value.
- Prometheus target metrics scraper will no longer panic if a metric has an
  invalid value (instead it will be logged)
- DogStatsD payloads try harder to satisfy the specified `num_contexts`. Before
  this change, dogstatsd payloads may be significantly under-representing the
  desired number of contexts.

## [0.23.3]
### Changed
- Linux observer is more resilient to scenarios where lading lacks ptrace permission.
### Removed
- lading_capture no longer exports a protobuf version of the capture.
### Added
- HTTP blackhole now has a `response_delay_millis` setting, allowing for
  simulation of latent network connections. The default value is 0.
### Fixed
- Logrotate file generator will no longer panic when rotating files beyond one depth.

## [0.23.2]
### Changed
- Now built using rust 1.81.0.

### Fixed
- Warmup period is now respected when container targeting is in use.
- Capture manager waits for the target to start running before recording data.

## [0.23.1]
### Fixed
- Fixes a panic in the signal mechanism that appeared when using the file
  generator most prominately.

## [0.23.0]
### Added
- Added ability to create tags for both expvar and prometheus target metrics specific to a single target_metrics configuration (example below shows prometheus metrics collected from the core agent and two additional tags created)
  ```yaml
  target_metrics:
    - prometheus: #core agent telemetry
        uri: "http://127.0.0.1:5000/telemetry"
        tags:
          sub_agent: "core"
          any_label: "any-string-value"
  ```

## [0.22.0]
### Fixed
- Fixes bugs in `smaps` parsing code that can result in under-counting RSS in
  the smaps view of the data.
- Target observer was not exposed through CLI.
### Changed
- Now built using rust 1.79.0
### Added
- Target observer now allows a docker target, identified by name.
- Lading experiment duration may be set to (effectively) infinite via `--experiment-duration-infinite`.
- Allow lading to export prometheus over UDS socket.

## [0.21.1]
### Changed
- Added lading version to initial welcome print msg.
### Fixed
- Prometheus metric exporter did not include some internal metrics (generator
  metrics)
- Range values in the dogstatsd payload will now generate the full inclusive
  range. Previously, values were generated up to but not including the `max`
  value.
- HTTP Blackhole now supports ZSTD encoded requests

## [0.21.0]
### Changed
- Improved the block cache construction to better fill the pre-defined space.
- Removed streaming cache method. Fixed is now the only option.
- Users now configure a maximum block size in generators, not individual blocks.
- Maximum datagram size in bytes for unix datagram generator is 8,192.
- Altered default cache construction from 'fixed' to 'streaming'
- Increased maximum DogStatsD context limit from 100k to 1M
- Procfs generator now generates process names as long as 254 characters, up from 253.
### Fixed
- The capture manager will no longer panic if recording a capture and checking for a shutdown combined takes longer than one second.
- A shutdown race was partially fixed in the capture manager which could result in truncated (invalid) json capture files.
- Unix datagram generator will not longer 'shear' blocks across datagrams.
- Procfs generator now generates `pid/stat` files with `comm` in parens, matching Linux behavior.

## [0.20.10]
### Added
- Optional new feature for DogStatsD generation -- metric name prefixing.
- Musl builds are now available for aarch64 and x86-64
### Changed
### Fixed

## [0.20.9]
### Added
- Enforces changelog entries via CI check
- `payloadtool` binary useful for developers writing new experiments.
- Respects the env var `RUST_LOG` to control logging.
### Fixed
- During payload generation, blocks will reliably be filled to the requested amount.
- During payload generation, block chunks now give more feedback when the
  requested amount cannot be hit.
- Does not crash on empty line in prometheus exports
### Changed
- Increases the amount of blocks available during payload generation.
- DogStatsD tag generation updated to better reflect real DSD traffic.
- Users should avoid setting custom `block_sizes` and rely on the default
  blocks. `payloadtool` is available to help evaluate block size options.

## [0.20.8]
### Added
- Parse working set memory from cgroups on Linux, opens the door to future
  cgroups inspection.

## [0.20.7]
### Fixed
- Fix panic in memory map reading code

## [0.20.6]
### Added
- Adds `/proc/<pid>/smap` parsing to add very detailed memory usage.

### Fixed
- Fixed a bug in CLI key/value parsing where values might be incorrectly parsed
  as key/value pairs if they held lading's delimiter character.

## [0.20.5]
### Added
- Adds a new config option to `lading_payload::dogstatsd::Config`,
  `length_prefix_framed`. If this option is on, each "block" emitted by the
  dogstatsd generator will have a little-endian u32 prefix (4 bytes) containing
  the length of the following block.

### Fixed
- Total CPU calculation routine no longer panics in some scenarios
- Fixes bug with retry logic in both `unix-stream` and `tcp` generators that could
  result in unexpected, extra connections.

## [0.20.4]
### Added
- `lading.running` gauge metric that submits a value of 1 as long as the main
  `select!` of lading is executing.
- `target.running` gauge metric that submits a value of 1 as long as the target
  is running.

### Changed
- Dogstatsd payload structs now have public fields.
- Capture file descriptor is flushed as soon as current lines are written.
- Renamed `Shutdown` data type to `Phase` so that this data type can be used as
  a signal to control `lading` behavior over distinct stages of load generation
  (e.g., warmup, shutdown).
- Prometheus telemetry and Go expvar target metrics are no longer emitted during
  `lading`'s warmup ("experimental") phase.

### Fixed
- Refactors the main `select!` in `lading/src/bin/lading.rs` to loop over the
  select to address a potential early-termination bug when the set of generators
  are empty.

## [0.20.3]
### Added
- ProcFs generator is now suitable for use.

## [0.20.2]
### Fixed
- Process gauges will now be zeroed before iteration. This prevents values from
  exited processes from being reported.

### Changed
- Lading now aborts on panic.
- The shutdown mechanism has been adjusted to improve its reliability.
- The capture manager now runs on a background OS thread.

### Added
- Added metrics that report CPU and memory usage of the whole target process tree.

## [0.20.1]
### Changed
- Internal shutdown sequence is now changed to an advisory signal. There is no
  user-facing change.
- Disabled ANSI codes in logging.
### Fixed
- Removed generational storage in capture management, resolving a runtime hang.

## [0.20.0]
### Added
- Sampling and sampling probability configuration parameters added for
  dogstatsd payloads.
- A new 'logrotate' file generator is introduced.
### Fixed
- Memory consumption stability issues in Dogstatsd payload generator
### Changed
- Existing file generator is renamed 'traditional', requiring a configuration
  change.
- Unknown/old configuration keys are now rejected

## [0.19.1]
### Fixed
- Report scraped Prometheus counters correctly
- Correct metrics emitted by http blackhole

## [0.19.0]
### Changed
- HTTP blackhole can respond with arbitrary data.
- Dogstatsd payload generation now takes range configuration, allows constant settings.

## [0.18.1]
### Added
- `lading-payload` crate is now split out from the `lading` crate
- It is now possible for users to configure the range of DogStatsD payloads
  values. Previously the range was 64-bits wide. The range is inclusive or
  constant. Additionally, users may configure a probability for values being a
  floating point or not.
### Changed
- The block mechanism is reworked to provide a 'fixed' and 'streaming' model,
  running in a separate OS thread from the tokio runtime.

## [0.18.0]
### Changed
- The predictive throttle no longer exists. The only options are stable and
  all-out.

## [0.17.4]
### Added
- `lading` and `lading-capture` releases are now published on crates.io.

## [0.17.3]
### Fixed
- We no longer incorrectly send multiple values on a SET metric in DogStatsD.
### Changed
- Rust compiler updated to 1.71.0
- We now allow users to configure the number of multi-values in a DogStatsD metric.

## [0.17.2]
### Removed
- Observer no longer emits tick data for kernel and user-space time.
### Changed
- We now avoid scaling utilization and clamp it directly to the appropriate
  range.
- We now elide sampling threads that are not also processes in the observer.
- Observer now distinguishes between parent and children processes.
- Config can now be specified using an env var `LADING_CONFIG`. If set, the env var takes precedence over the on-disk config file.
- We now expose two crates from this project, one for capture payloads and the
  other the main lading project.

## [0.17.1]
### Fixed
- Removed a leftover debug print in the Json payload implementation.

## [0.17.0]
### Changed
- Adjusted the default throttle to stable from predictive
- On Linux calculate CPU utilization in terms of logical, not physical, cores when possible.
- CPU percentage calculated in the same manner as Agent's
- Throttle metrics are now labeled with the respective generator's labels.
- Observer now calculates CPU utilization with respect to target cgroup hard, soft limits.

## [0.16.1]
### Changed
- Unix datagram connect errors are now logged at the `error` level.

### Fixed
- No-target mode no longer hangs at startup
- The `unix_datagram` generator doesn't start generating data until the target is running.
- Panic fixed in `unix_datagram` generator

## [0.16.0]
### Added
- `Generators` and `Blackholes` now support an `id` configuration field. This
  will be added to all metrics produced by a component instance.

## [0.15.3]
### Changed
- Reduced the CPU time that `file_gen` consumes at the expense of slightly
  longer flush-to-disk times.
- `file_gen` now shares its pre-computed block between write children, reducing
  memory consumption.

## [0.15.2]
### Fixed
- Disallow the creation of DogStatsD metrics with no values

## [0.15.1]
### Added
- Add 'stable' throttle
- Add 'all-out' throttle
- Allow users to configure generators to use the various throttles
- Added the ability for users to configure DogStatsD payload kind ratios
- Added the ability for users to configure DogStatsd metric kind ratios

## [0.15.0]
### Added
- Added target metrics support for Go expvars
- Added target metrics support for Prometheus

### Fixed
- Fixed throttle behavior for generators that run very quickly

## [0.14.0]
### Added
- Added the ability to configure details about DogStatsD payload
### Changed
- Datadog logs generation now much faster, relying on an experimentally
  determined 'encoded size' rather than searching at runtime for the same.
  (PR #564).
- Adjust generators to consume less CPU by registering metrics where
  possible. (PR #544)
- Adjusted trace-agent msgpack payload generation to be much faster at the cost
  of some memory during the generation process. (PR #547)

## [0.13.1]
### Added
- Introduced Proportional Set Size (PSS) memory measurement under the
  `pss_bytes` metric.
- Convert `Block` to use `Bytes` type instead of `Vec<u8>`.
- Introduce Datadog trace-agent payload support in JSON and MsgPack serialization.

## [0.13.0]
### Added
- Introduced automatic throttling into generators to search for stable target load.
### Fixed
- Use saturating addition in observer stats gathering routine

## [0.12.0]
### Added
- Added the ability for lading to back-off load generation based on RSS limits.
- Process tree generator contributed by @safchain
- Fixed: OpenTelemetry message length calculation corrected for some messages.
- **Breaking change:** Split UDS support between explict datagram and stream modules.
- Fixed: Corrected mistakes in the DogStatsD payload implementation.
### Changed
- Adjusted the cardinality of DogStatsD keys, values and names downward.
- **Breaking change:** Added support for DogStatsD payload.
- **Breaking change:** Support for Kafka generator is removed.
### Fixed
- Lading's UDS will now re-attempt to connect to a UDS socket, rather than
  erroring.

## [0.11.3] - 2022-12-12
### Fixed
- gRPC calls that respond with data are now handled correctly. This previously
emitted an error and dropped the response.

## [0.11.2] - 2022-12-01
### Added
- Targets can inherit lading's environment variables using the
`--target-inherit-environment` flag

### Changed
- glibc based releases are now built on ubuntu-20.04 rather than ubuntu-latest

## [0.11.1] - 2022-11-22
### Added
- Releases now include x86-64 musl binaries
- The musl build does not support kafka

### Changed
- The Protobuf compiler is no longer required to build lading

## [0.11.0] - 2022-11-16
### Added
- Observer now measures child processes of the target

## [0.10.4] - 2022-11-16
### Added
- HTTP blackhole can be configured with arbitrary response body, headers and
status code
- gRPC HTTP2 client concurrency can now be configured

## [0.10.3] - 2022-11-02
### Fixed
- Works around an issue where the UDP generator would cause lading to never
exit.

## [0.10.2] - 2022-10-21
### Fixed
- No notable code changes. Addresses issues with the release workflow.

## [0.10.1] - 2022-10-20
### Fixed
- No code changes. Addresses issues with the release workflow.

## [0.10.0] - 2022-10-20
### Added
- New payload: Apache Common
- New payload: OpenTelemetry protobufs for metrics, traces, and logs
- New generator: gRPC over HTTP/2
- Collect CPU, memory, and scheduling information about the target process
- Target processes may now be launched externally and watched by PID

### Changed
- **Breaking change:** Support multiple generators and blackholes. These config
sections are now lists.
- **Breaking change:** Share the same payload configuration across all
generators. In alignment with other generators, the file generator now requires
snake-cased payload names.
- HTTP and Splunk HEC blackholes support gzip decoding based on incoming headers
- TCP blackhole counts bytes recieved and publishes a `bytes_received` metric
- Ignore empty strings in CLI key-value arguments. This was previously an error.
- The static payload can accept a directory of files
- The file generator's file rotation behavior can be configured

### Fixed
- Improve startup sequencing
- Fix TCP generator shutdown for long lived connections
