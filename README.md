# `file_gen` - A repeatable file generating program.

This program generates line oriented file content in a repeatable way as rapidly
as possible. The goal is to provide stable input to programs that track line
content and to do so at scale. Line content is not guaranteed to be sourced from
quality randomness.

## Basic Operation

The `file_gen` program generates line oriented files with size, bytes per second
and variant goals. Today there are three supported line variants: json, constant
and ascii. The `json` variant produces a known payload, the `constant` fills
lines with a constant char and the `ascii` variant fills lines with printable
ascii characters. There are performance implications for each of these variants,
see [Performance](#performance) below.

## Configuration

This program is primarily configured through its config file. Here is an
example:

```
random_seed = 201010
worker_threads = 3

[targets.foo]
path_template = "/tmp/playground/%NNN%-foo.json"
duplicates = 3
variant = "Json"
maximum_bytes_per = "1Gb"
bytes_per_second = "10Mb"
maximum_line_size_bytes = "1024 Kb"

[targets.bar]
path_template = "/tmp/playground/%NNN%-bar.txt"
duplicates = 10
variant = "Ascii"
maximum_bytes_per = "1Gb"
bytes_per_second = "100Mb"
maximum_line_size_bytes = "1024 Kb"

[targets.bing]
path_template = "/tmp/playground/%NNN%-bing.const"
duplicates = 2
variant = "Constant"
maximum_bytes_per = "4Gb"
bytes_per_second = "500Mb"
maximum_line_size_bytes = "1Mb"
```

This configuration will create three total targets 'foo', 'bar' and 'bing'. Each
target will produce a different line variant at a distinct tempo. Please see
[`example.toml`](./example.toml) for more details about the various options.

## Telemetry

This program self-instruments through prometheus metrics. This is subject to
change and we do not document exactly what metrics are available, though you may
find out for yourself by examining [`src/lib.rs`](./src/lib.rs) if you need.

## Performance

Every attempt has been made to make `file_gen` generate bytes with as much
throughput as possible. Our measurements are made with the programs in
[`benches/`](./benches). If you are particularly sensitive about performance
numbers please run these benchmarks yourself on your representative system. You
may do so by running [`cargo
criterion`](https://crates.io/crates/cargo-criterion) from the root of this
project. From our system we find that the variants produce bytes with the
following profile:

* json: 450 Mb/s, +/- 3%
* constant: 125 Gb/s, +/- 5%
* ascii: 1.2 Gb/s, +/- 1%
