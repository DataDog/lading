# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
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
