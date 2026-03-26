# HTTP ASCII 100 MiB/s

Resource usage of lading under a steady 100 MiB/s HTTP load with ASCII payloads.

## What

Runs lading as a target with an HTTP generator sending ASCII payloads at 100 MiB/s to its own HTTP blackhole. This exercises the HTTP generator, ASCII payload construction, and HTTP blackhole within a single lading instance.

## Why

Establishes a baseline for lading's resource consumption under a high HTTP workload. Regressions here indicate overhead in the HTTP generator or payload path independent of payload complexity.

## Paired Benchmark

This experiment is paired with the local benchmark in `lading_payload/benches/ascii.rs`.
If throughput sizes change in either place, update the other to match.

## Enforcements

Memory usage is enforced by bounding `total_pss_bytes`.
CPU usage is enforced by bounding `avg(total_cpu_usage_millicores)`.
