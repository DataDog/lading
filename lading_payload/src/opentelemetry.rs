//! OpenTelemetry payload generation
//!
//! This module contains payload generators for OpenTelemetry formats.

pub mod log;
pub mod metric;
pub mod trace;

pub use log::OpentelemetryLogs;
pub use metric::OpentelemetryMetrics;
pub use trace::OpentelemetryTraces;
