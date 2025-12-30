//! The lading daemon load generation and introspection tool.
//!
//! This library support the lading binary found elsewhere in this project. The
//! bits and pieces here are not intended to be used outside of supporting
//! lading, although if they are helpful in other domains that's a nice
//! surprise.

#![deny(clippy::cargo)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::multiple_crate_versions)]

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

/// Get available memory for the process, checking cgroup v2 limits first,
/// then falling back to system memory.
#[must_use]
pub fn get_available_memory() -> Byte {
    if let Ok(content) = std::fs::read_to_string("/sys/fs/cgroup/memory.max") {
        let content = content.trim();
        if content == "max" {
            return Byte::from_u64(u64::MAX);
        }
        let ignore_case = true;
        if let Ok(limit) = Byte::parse_str(content.trim(), ignore_case) {
            return limit;
        }
    }

    let sys = System::new_all();
    Byte::from_u64(sys.total_memory())
}

#[cfg(all(feature = "jemalloc_profiling", unix))]
/// Log jemalloc memory statistics for debugging.
pub fn log_jemalloc_stats(label: &str) {
    use std::os::raw::{c_char, c_void};
    use tikv_jemalloc_ctl::epoch;
    use tikv_jemalloc_sys as jemalloc_sys;
    use tracing::info;

    // Refresh jemalloc stats
    let _ = epoch::advance();

    // Read stats via mallctl
    fn read_stat(name: &[u8]) -> usize {
        let mut value: usize = 0;
        let mut len = std::mem::size_of::<usize>();
        unsafe {
            jemalloc_sys::mallctl(
                name.as_ptr() as *const c_char,
                &mut value as *mut _ as *mut c_void,
                &mut len,
                std::ptr::null_mut(),
                0,
            );
        }
        value
    }

    let allocated = read_stat(b"stats.allocated\0");
    let active = read_stat(b"stats.active\0");
    let resident = read_stat(b"stats.resident\0");
    let retained = read_stat(b"stats.retained\0");
    let mapped = read_stat(b"stats.mapped\0");

    info!(
        "[JEMALLOC] {}: allocated={} MB, active={} MB, resident={} MB, retained={} MB, mapped={} MB",
        label,
        allocated / (1024 * 1024),
        active / (1024 * 1024),
        resident / (1024 * 1024),
        retained / (1024 * 1024),
        mapped / (1024 * 1024)
    );
}

#[cfg(not(all(feature = "jemalloc_profiling", unix)))]
/// Stub for non-jemalloc builds
pub fn log_jemalloc_stats(_label: &str) {}

#[cfg(all(feature = "jemalloc_profiling", unix))]
/// Install a SIGUSR1 handler that writes jemalloc heap profiles to `/tmp`.
///
/// This requires running lading with the `jemalloc_profiling` feature enabled
/// and with jemalloc profiling turned on at runtime, for example via
/// `MALLOC_CONF=prof:true,prof_active:true`. When the process receives SIGUSR1
/// a heap profile will be written to `/tmp/lading-heap-<unix_ts>.heap`.
pub fn install_jemalloc_profiling_handler() {
    use signal_hook::consts::signal::SIGUSR1;
    use signal_hook::iterator::Signals;
    use std::ffi::CString;
    use std::os::raw::{c_char, c_void};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tikv_jemalloc_ctl::epoch;
    use tikv_jemalloc_sys as jemalloc_sys;
    use tokio::task;
    use tracing::{error, info};

    task::spawn_blocking(move || {
        let mut signals = match Signals::new([SIGUSR1]) {
            Ok(sig) => sig,
            Err(err) => {
                error!(?err, "failed to install jemalloc profiling signal handler");
                return;
            }
        };

        for signal in &mut signals {
            let _ = epoch::advance();
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let path = format!("/tmp/lading-heap-{timestamp}.heap");
            let c_path = match CString::new(path.clone()) {
                Ok(c) => c,
                Err(err) => {
                    error!(?err, %path, "failed to build CString for heap profile path");
                    continue;
                }
            };
            let mut filename_ptr = c_path.as_ptr();
            // Safety: mallctl expects a pointer to a C string pointer for prof.dump.
            let res = unsafe {
                jemalloc_sys::mallctl(
                    b"prof.dump\0".as_ptr() as *const c_char,
                    std::ptr::null_mut::<c_void>(),
                    std::ptr::null_mut(),
                    &mut filename_ptr as *mut _ as *mut c_void,
                    std::mem::size_of::<*const c_char>(),
                )
            };
            if res == 0 {
                info!(signal, %path, "wrote jemalloc heap profile");
            } else {
                error!(signal, %path, res, "failed to write jemalloc heap profile");
            }
        }
    });
}
