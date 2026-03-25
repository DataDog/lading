# Idle

Baseline resource usage of lading with no active workload.

## What

Runs lading as a target with an empty generator and a single HTTP blackhole; this represents the most basic way to run lading.

## Why

Establishes a floor for lading's memory footprint. Regressions here indicate overhead introduced independent of any workload.

## Enforcements

Memory usage is enforced by bounding `total_pss_bytes`.
CPU usage is enforced by bounding `avg(total_cpu_usage_millicores)`.
