# lading-observer

Linux metrics observer library extracted from lading for use in external projects.

## Overview

This crate provides low-level polling functions for collecting container and process metrics on Linux:

- **cgroup v2 metrics**: CPU, memory, IO, and PSI (Pressure Stall Information)
- **procfs metrics**: Memory statistics from `/proc/<pid>/smaps_rollup` and `/proc/<pid>/stat`

## Usage

```rust
use lading_observer::cgroup::v2;
use lading_observer::procfs::memory::smaps_rollup;

// Poll cgroup v2 metrics
let cgroup_path = std::path::Path::new("/sys/fs/cgroup/my-container");
let labels = vec![("container".to_string(), "my-container".to_string())];
v2::poll(&cgroup_path, &labels).await?;

// Poll process memory metrics
let pid = 1234;
let labels = [("pid", pid.to_string())];
let mut aggregator = smaps_rollup::Aggregator::default();
smaps_rollup::poll(pid, &labels, &mut aggregator).await?;
```

## Requirements

- Linux kernel with cgroup v2 support
- `/proc` filesystem mounted
- Rust 1.90+

## License

MIT
