//! The lading daemon load generation and introspection tool.
//!
//! This library support the lading binary found elsewhere in this project. The
//! bits and pieces here are not intended to be used outside of supporting
//! lading, although if they are helpful in other domains that's a nice
//! surprise.

#![deny(clippy::cargo)]
#![expect(clippy::cast_precision_loss)]
#![expect(clippy::multiple_crate_versions)]

use http_body_util::BodyExt;

pub mod blackhole;
pub(crate) mod codec;
mod common;
pub mod config;
pub mod generator;
pub mod inspector;
pub mod observer;
pub(crate) mod proto;
pub mod target;
pub mod target_metrics;

use std::fs::read_to_string;
use std::io::Error;
use std::path::PathBuf;

use byte_unit::Byte;
use sysinfo::System;

#[inline]
pub(crate) fn full<T: Into<bytes::Bytes>>(
    chunk: T,
) -> http_body_util::combinators::BoxBody<bytes::Bytes, hyper::Error> {
    http_body_util::Full::new(chunk.into())
        .map_err(|never| match never {})
        .boxed()
}

/// Determine the available memory for this process by inspecting cgroup
/// limits, falling back to total system memory.
///
/// Resolution order:
///   1. cgroup v2 -- walk from process cgroup up to root, return minimum
///      `memory.max` across the ancestor chain
///   2. cgroup v2 root (`/sys/fs/cgroup/memory.max`)
///   3. Total system memory via `sysinfo`
///
/// Step 1 uses `/proc/self/cgroup` to resolve the process path.
/// Step 2 is the fallback when `/proc/self/cgroup` is unavailable or shows root (`/`).
///
/// References:
///   - cgroup v2: <https://docs.kernel.org/admin-guide/cgroup-v2.html>
///   - `/proc/$PID/cgroup`: <https://docs.kernel.org/admin-guide/cgroup-v1/cgroups.html>
#[must_use]
pub fn get_available_memory() -> Byte {
    get_available_memory_with(read_to_string)
}

fn parse_cgroup_v2_limit(content: &str) -> Option<Byte> {
    let content = content.trim();
    if content == "max" {
        return None;
    }
    let bytes: u64 = content.parse().ok()?;
    Some(Byte::from_u64(bytes))
}

/// Extract the cgroup v2 path from `/proc/self/cgroup` content (`0::/path`).
fn resolve_cgroup_v2_path(proc_cgroup_content: &str) -> Option<&str> {
    for line in proc_cgroup_content.lines() {
        // cgroup v2 unified entries start with "0::"
        if let Some(path) = line.strip_prefix("0::") {
            let path = path.trim();
            if !path.is_empty() && path != "/" {
                return Some(path);
            }
        }
    }
    None
}

/// Inner implementation of [`get_available_memory`] with an injectable file
/// reader for testing.
fn get_available_memory_with<F>(read_file: F) -> Byte
where
    F: Fn(PathBuf) -> Result<String, Error>,
{
    let proc_cgroup = read_file(PathBuf::from("/proc/self/cgroup")).ok();

    // Try cgroup v2: walk from the process-specific path up to the root,
    // taking the minimum memory.max across the ancestor chain.
    if let Some(ref proc_cgroup) = proc_cgroup
        && let Some(cgroup_path) = resolve_cgroup_v2_path(proc_cgroup)
    {
        let cgroup_root = PathBuf::from("/sys/fs/cgroup");
        let mut path = cgroup_root.join(&cgroup_path[1..]); // strip leading '/'
        let mut min_limit: Option<Byte> = None;
        loop {
            let memory_max = path.join("memory.max");
            if let Ok(content) = read_file(memory_max)
                && let Some(limit) = parse_cgroup_v2_limit(&content)
            {
                min_limit = Some(match min_limit {
                    Some(prev) if prev.as_u64() < limit.as_u64() => prev,
                    _ => limit,
                });
            }
            if path == cgroup_root {
                break;
            }
            if !path.pop() {
                break;
            }
        }
        if let Some(limit) = min_limit {
            return limit;
        }
    }

    // Try cgroup v2 root (works when container has its own cgroup namespace)
    if let Ok(content) = read_file(PathBuf::from("/sys/fs/cgroup/memory.max"))
        && let Some(limit) = parse_cgroup_v2_limit(&content)
    {
        return limit;
    }

    let sys = System::new_all();
    Byte::from_u64(sys.total_memory())
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;

    use super::*;

    fn mock_reader(
        files: &'static [(&'static str, &'static str)],
    ) -> impl Fn(PathBuf) -> Result<String, Error> {
        move |path| {
            for (p, content) in files {
                if PathBuf::from(p) == path {
                    return Ok((*content).to_string());
                }
            }
            Err(Error::new(ErrorKind::NotFound, "not found"))
        }
    }

    #[test]
    fn cgroup_v2_max_falls_back_to_system_memory() {
        let read = mock_reader(&[("/sys/fs/cgroup/memory.max", "max\n")]);
        let mem = get_available_memory_with(read);
        let sys = System::new_all();
        assert_eq!(mem, Byte::from_u64(sys.total_memory()));
    }

    #[test]
    fn cgroup_v2_numeric_limit() {
        let read = mock_reader(&[("/sys/fs/cgroup/memory.max", "12582912\n")]);
        let mem = get_available_memory_with(read);
        assert_eq!(mem, Byte::from_u64(12_582_912));
    }

    #[test]
    fn cgroup_v2_garbage_falls_back_to_system_memory() {
        let read = mock_reader(&[("/sys/fs/cgroup/memory.max", "not-a-number\n")]);
        let mem = get_available_memory_with(read);
        let sys = System::new_all();
        assert_eq!(mem, Byte::from_u64(sys.total_memory()));
    }

    #[test]
    fn no_cgroup_file_falls_back_to_system_memory() {
        let read = mock_reader(&[]);
        let mem = get_available_memory_with(read);
        let sys = System::new_all();
        assert_eq!(mem, Byte::from_u64(sys.total_memory()));
    }

    #[test]
    fn process_specific_cgroup_v2_path() {
        // Simulates host cgroup namespace: /proc/self/cgroup points to a
        // nested cgroup, and the memory limit is at that nested path.
        let read = mock_reader(&[
            (
                "/proc/self/cgroup",
                "0::/system.slice/docker-abc123.scope\n",
            ),
            (
                "/sys/fs/cgroup/system.slice/docker-abc123.scope/memory.max",
                "12582912\n",
            ),
            // Root cgroup has no limit
            ("/sys/fs/cgroup/memory.max", "max\n"),
        ]);
        let mem = get_available_memory_with(read);
        assert_eq!(mem, Byte::from_u64(12_582_912));
    }

    #[test]
    fn process_specific_path_takes_precedence_over_root() {
        let read = mock_reader(&[
            (
                "/proc/self/cgroup",
                "0::/system.slice/docker-abc123.scope\n",
            ),
            (
                "/sys/fs/cgroup/system.slice/docker-abc123.scope/memory.max",
                "8388608\n",
            ),
            ("/sys/fs/cgroup/memory.max", "16777216\n"),
        ]);
        let mem = get_available_memory_with(read);
        assert_eq!(mem, Byte::from_u64(8_388_608));
    }

    #[test]
    fn process_specific_path_max_falls_through_to_root() {
        let read = mock_reader(&[
            (
                "/proc/self/cgroup",
                "0::/system.slice/docker-abc123.scope\n",
            ),
            (
                "/sys/fs/cgroup/system.slice/docker-abc123.scope/memory.max",
                "max\n",
            ),
            ("/sys/fs/cgroup/memory.max", "16777216\n"),
        ]);
        let mem = get_available_memory_with(read);
        assert_eq!(mem, Byte::from_u64(16_777_216));
    }

    #[test]
    fn proc_cgroup_root_path_skipped() {
        // When /proc/self/cgroup shows root path "/", skip it and fall
        // through to the root cgroup check.
        let read = mock_reader(&[
            ("/proc/self/cgroup", "0::/\n"),
            ("/sys/fs/cgroup/memory.max", "12582912\n"),
        ]);
        let mem = get_available_memory_with(read);
        assert_eq!(mem, Byte::from_u64(12_582_912));
    }

    #[test]
    fn resolve_cgroup_v2_path_parses_correctly() {
        assert_eq!(
            resolve_cgroup_v2_path("0::/system.slice/docker-abc.scope\n"),
            Some("/system.slice/docker-abc.scope")
        );
        assert_eq!(resolve_cgroup_v2_path("0::/\n"), None);
        assert_eq!(resolve_cgroup_v2_path(""), None);
        // Non-v2 lines are ignored
        assert_eq!(
            resolve_cgroup_v2_path("1:memory:/docker/abc\n0::/system.slice\n"),
            Some("/system.slice")
        );
    }

    #[test]
    fn cgroup_v2_ancestor_limit_is_tighter() {
        // Nested v2 cgroup where the parent has a tighter limit than the
        // child. The effective limit is the minimum up the ancestor chain.
        let read = mock_reader(&[
            ("/proc/self/cgroup", "0::/parent/child\n"),
            // child: 16 MB
            ("/sys/fs/cgroup/parent/child/memory.max", "16777216\n"),
            // parent: 8 MB (tighter)
            ("/sys/fs/cgroup/parent/memory.max", "8388608\n"),
            // root: no limit
            ("/sys/fs/cgroup/memory.max", "max\n"),
        ]);
        let mem = get_available_memory_with(read);
        assert_eq!(mem, Byte::from_u64(8_388_608));
    }

    #[test]
    fn cgroup_v2_ancestor_limit_three_levels() {
        // Three-level hierarchy: grandparent is tightest.
        let read = mock_reader(&[
            ("/proc/self/cgroup", "0::/a/b/c\n"),
            ("/sys/fs/cgroup/a/b/c/memory.max", "33554432\n"), // 32 MB
            ("/sys/fs/cgroup/a/b/memory.max", "16777216\n"),   // 16 MB
            ("/sys/fs/cgroup/a/memory.max", "8388608\n"),      // 8 MB (tightest)
            ("/sys/fs/cgroup/memory.max", "max\n"),
        ]);
        let mem = get_available_memory_with(read);
        assert_eq!(mem, Byte::from_u64(8_388_608));
    }

    #[test]
    fn cgroup_v2_ancestor_middle_is_tightest() {
        // Middle ancestor is tightest.
        let read = mock_reader(&[
            ("/proc/self/cgroup", "0::/a/b/c\n"),
            ("/sys/fs/cgroup/a/b/c/memory.max", "33554432\n"), // 32 MB
            ("/sys/fs/cgroup/a/b/memory.max", "4194304\n"),    // 4 MB (tightest)
            ("/sys/fs/cgroup/a/memory.max", "16777216\n"),     // 16 MB
            ("/sys/fs/cgroup/memory.max", "max\n"),
        ]);
        let mem = get_available_memory_with(read);
        assert_eq!(mem, Byte::from_u64(4_194_304));
    }

    #[test]
    fn get_available_memory_returns_nonzero() {
        let mem = get_available_memory();
        assert!(mem.as_u64() > 0);
    }
}
