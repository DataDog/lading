# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
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
