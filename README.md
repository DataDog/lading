# `file_gen` - A repeatable file generating program.

This program generates line oriented file content in a repeatable way as rapidly
as possible. The goal is to provide stable input to programs that track line
content and to do so at scale. Line content is not guaranteed to be sourced from
quality randomness.

## Basic Operation

The `file_gen` program generates line oriented files with size, bytes per second
and variant goals. Today there are three supported line variants: json, constant
and ascii. The `json` variant produces a known payload and the `ascii` variant
fills lines with printable ascii characters. More formats are planned.

## Configuration

This program is primarily configured through its config file. Here is an
example:

```
worker_threads = 3

[targets.bar]
path_template = "/tmp/data/file_gen/%NNN%-bar.log"
duplicates = 1
variant = "Ascii"
maximum_bytes_per_file = "4 Gb"
bytes_per_second = "50 Mb"
maximum_prebuild_cache_size_bytes = "3 Gb"

[targets.foo]
path_template = "/tmp/data/file_gen/%NNN%-foo.log"
duplicates = 2
variant = "Json"
maximum_bytes_per_file = "4 Gb"
bytes_per_second = "1 Gb"
maximum_prebuild_cache_size_bytes = "2 Gb"
```

This configuration will create two total targets 'foo' and 'bar'. Each
target will produce a different line variant at a distinct tempo. Please see
[`example.toml`](./example.toml) for more details about the various options.

## Telemetry

This program self-instruments through prometheus metrics. This is subject to
change and we do not document exactly what metrics are available, though you may
find out for yourself by examining [`src/lib.rs`](./src/lib.rs) if you need.

## Performance

Every attempt has been made to make `file_gen` generate bytes with as much
throughput as possible. In the 0.3 series we have pivoted to a slower line
construction mechanism but relied on prebuilding blocks to write, dramatically
improving total throughput up to the limit of 4Gb/s per target duplicate. This
is currently a hard limit based on a u32 embedded in the program. See "Weird
Quirks".

## Weird Quirks

This program's configuration does not understand byte sizes greater than `u32`
bytes. Ultimately this is a limitation inherited from the
[`governor`](https://github.com/antifuchs/governor) dependency. If this
limitation needs to be lifted we'll have to contribute a fix upstream, or adjust
our rate limiting approach.

Json generation is painfully slow. I'm very open to alternative approaches.
