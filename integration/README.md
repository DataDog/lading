# Lading Integration Tests

This directory contains an integration test harness (`sheepdog`) and target
(`ducks`). The two work together to integration test `lading`.

## Setup

### Macos:

Install `protoc` with brew: `brew install protobuf`

Set `PROTOC=/opt/homebrew/bin/protoc` in your shell

### Linux

Install `protoc` with apt: `apt install protobuf-compiler`

## Test Flow

Each test's entry point is a normal async rust test in the `sheepdog` crate. The
test implementation defines `lading` and `ducks` configurations and performs
assertions on the test results. Tests start by launching a `ducks` process and
establishing an RPC link between `sheepdog` and `ducks`. This link is used to
configure, control, and receive measurement information from `ducks`. Next, a
`lading` process is started using a configuration that instructs it to send load
to `ducks`. This load is characterized by `ducks` and measurements are sent to
`sheepdog` for validation.
