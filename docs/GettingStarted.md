# Getting Started

## Installation

`lading` builds are currently only published as containers. Ping @GeorgeHahn on slack about getting binaries into github releases.

## Configuration File

A YAML file is used to configure lading's inputs and outputs. In lading terminology, these are known as generators and blackholes. Most generators can be configured to send different data formats, these are known as payloads. At least one generator is required for all configurations. Beyond that, any number of generators and blackholes may be used.

See [generators.md] and [blackholes.md] for a listing of the available sources and sinks. See [payloads.md] for a listing of available data formats.

## Target

Lading can target a binary on disk or a running process. In binary-launch mode, lading will directly launch and manage the binary. In process-watch mode, lading requires external process orchestration. It will not perform any management of the target process in this mode.

## Running lading

Example config file:
```yaml
```

Example binary-launch invocation:
``

Example process-watch invocation:
``

## Outputs

Todo: talk about captures
