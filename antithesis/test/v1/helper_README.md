# Test templates

This directory is baked into the `workload-client` image at
`/opt/antithesis/test/v1/` via `COPY` in `antithesis/Dockerfile`, so adding
or changing commands requires rebuilding that image. Each subdirectory here is
a *test template* named after its directory. The executable files inside it are
*test commands*.

`antithesis-setup` only wires the path so later workload code can run here. Real
test commands and assertions belong to the `antithesis-workload` skill.

Each test command file must use a recognized prefix:
`parallel_driver_`, `singleton_driver_`, `serial_driver_`, `first_`,
`eventually_`, `finally_`, or `anytime_`. Files or directories prefixed with
`helper_`, like this file, are ignored by Antithesis.
