//! OpenTelemetry payload generation
//!
//! This module contains payload generators for OpenTelemetry formats.

pub mod common;
pub mod log;
pub mod metric;
pub mod trace;
pub mod trace_graph;

pub use log::OpentelemetryLogs;
pub use metric::OpentelemetryMetrics;
pub use trace::OpentelemetryTraces;
pub use trace_graph::OpentelemetryTracesGraph;
