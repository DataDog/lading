//! Shared networking infrastructure for neper-style workloads.
//!
//! This module provides reusable building blocks — flow management, per-thread
//! metrics, and OS thread lifecycle helpers — that are composed by mode-specific
//! generators and blackholes (e.g. `tcp_rr`, `tcp_crr`, `tcp_stream`).

#[cfg(target_os = "linux")]
pub(crate) mod bpf;
#[cfg(not(target_os = "linux"))]
#[path = "neper/bpf_stub.rs"]
pub(crate) mod bpf;
pub(crate) mod flow;
pub(crate) mod metrics;
pub(crate) mod thread;
