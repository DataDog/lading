//! The lading daemon load generation and introspection tool.
//!
//! This library support the lading binary found elsewhere in this project. The
//! bits and pieces here are not intended to be used outside of supporting
//! lading, although if they are helpful in other domains that's a nice
//! surprise.

#![deny(clippy::all)]
#![deny(clippy::cargo)]
#![deny(clippy::pedantic)]
#![deny(clippy::perf)]
#![deny(clippy::suspicious)]
#![deny(clippy::complexity)]
#![deny(clippy::unnecessary_to_owned)]
#![deny(clippy::manual_memcpy)]
#![deny(clippy::float_cmp)]
#![deny(clippy::large_stack_arrays)]
#![deny(clippy::large_futures)]
#![deny(clippy::rc_buffer)]
#![deny(clippy::redundant_allocation)]
#![deny(clippy::print_stdout)]
#![deny(clippy::print_stderr)]
#![deny(clippy::dbg_macro)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::mod_module_files)]
#![deny(unused_extern_crates)]
#![deny(unused_allocation)]
#![deny(unused_assignments)]
#![deny(unused_comparisons)]
#![deny(unreachable_pub)]
#![deny(missing_docs)]
#![warn(missing_copy_implementations)]
#![deny(missing_debug_implementations)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::multiple_crate_versions)]

use http_body_util::BodyExt;

pub mod blackhole;
pub mod captures;
pub(crate) mod codec;
mod common;
pub mod config;
pub mod generator;
pub mod inspector;
pub mod observer;
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
