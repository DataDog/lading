//! Main entry point for lading_payload benchmarks.
//!
//! Not all payload types have benchmarks:
//! - `Static` and `StaticChunks` require external file paths and cannot be
//!   deterministically generated for benchmarking. They depend on user-supplied
//!   content rather than synthesized data.

use criterion::criterion_main;

mod apache_common;
mod ascii;
mod datadog_logs;
mod dogstatsd;
mod fluent;
mod json;
mod opentelemetry_log;
mod opentelemetry_metric;
mod opentelemetry_traces;
mod splunk_hec;
mod syslog;
mod trace_agent;

criterion_main!(
    apache_common::benches,
    ascii::benches,
    datadog_logs::benches,
    dogstatsd::benches,
    fluent::benches,
    json::benches,
    opentelemetry_log::benches,
    opentelemetry_metric::benches,
    opentelemetry_traces::benches,
    splunk_hec::benches,
    syslog::benches,
    trace_agent::benches,
);
