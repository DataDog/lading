//! The lading payloads
//!
//! This library supports payload generation for the lading project.

#![deny(clippy::cargo)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::multiple_crate_versions)]

use std::{
    io::{self, Write},
    path::PathBuf,
};

use rand::{Rng, distr::weighted};
use serde::Deserialize;

pub mod block;

pub use apache_common::ApacheCommon;
pub use ascii::Ascii;
pub use datadog_logs::DatadogLog;
pub use dogstatsd::DogStatsD;
pub use fluent::Fluent;
pub use json::Json;
pub use opentelemetry::log::OpentelemetryLogs;
pub use opentelemetry::metric::OpentelemetryMetrics;
pub use opentelemetry::trace::OpentelemetryTraces;
pub use splunk_hec::SplunkHec;
pub use statik::Static;
pub use statik_second::StaticSecond;
pub use statik_line_rate::StaticLinesPerSecond;
pub use syslog::Syslog5424;

pub mod apache_common;
pub mod ascii;
pub mod common;
pub mod datadog_logs;
pub mod dogstatsd;
pub mod fluent;
pub mod json;
pub mod opentelemetry;
pub mod procfs;
pub mod splunk_hec;
pub mod statik;
pub mod statik_second;
pub mod statik_line_rate;
pub mod syslog;
pub mod trace_agent;

/// Errors related to serialization
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// `MsgPack` payload could not be encoded
    #[error("MsgPack payload could not be encoded: {0}")]
    MsgPack(#[from] rmp_serde::encode::Error),
    /// Json payload could not be encoded
    #[error("Json payload could not be encoded: {0}")]
    Json(#[from] serde_json::Error),
    /// IO operation failed
    #[error("IO operation failed: {0}")]
    Io(#[from] io::Error),
    /// failed to generate string
    #[error("Failed to generate string")]
    StringGenerate,
    /// Serialization failed
    #[error("Serialization failed")]
    Serialize,
    /// See [`weighted::Error`]
    #[error(transparent)]
    Weights(#[from] weighted::Error),
    /// See [`unit::Error`]
    #[error(transparent)]
    Unit(#[from] opentelemetry::metric::unit::Error),
    /// See [`prost::EncodeError`]
    #[error(transparent)]
    ProstEncode(#[from] prost::EncodeError),
    /// See [`opentelemetry::common::PoolError`]
    #[error("Unable to choose from pool: {0}")]
    Pool(
        #[from] opentelemetry::common::templates::PoolError<opentelemetry::common::GeneratorError>,
    ),
    /// Validation error
    #[error("Validation error: {0}")]
    Validation(String),
}

/// To serialize into bytes
pub trait Serialize {
    /// Write bytes into writer, subject to `max_bytes` limitations.
    ///
    /// # Errors
    ///
    /// Most implementations are serializing data in some way. The errors that
    /// result come from serialization crackups.
    fn to_bytes<W, R>(&mut self, rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write;

    /// Reports data points count for the most recently generated content.
    ///
    /// IMPORTANT: This method should be called immediately after `to_bytes` to
    /// get accurate counts for the most recently generated block. The
    /// information WILL be overwritten by subsequent calls to `to_bytes`.
    ///
    /// If this function returns None the serialize does not support tracking
    /// data points.
    fn data_points_generated(&self) -> Option<u64> {
        None
    }
}

/// Configuration for `Payload`
#[derive(Debug, Deserialize, serde::Serialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields)]
pub enum Config {
    /// Generates Fluent messages
    Fluent,
    /// Generates syslog5424 messages
    Syslog5424,
    /// Generates Splunk HEC messages
    SplunkHec {
        /// Defines the encoding to use for the Splunk HEC messages.
        encoding: splunk_hec::Encoding,
    },
    /// Generates Datadog Logs JSON messages
    DatadogLog,
    /// Generates a static, user supplied data
    Static {
        /// Defines the file path to read static variant data from. Content is
        /// assumed to be line-oriented but no other claim is made on the file.
        static_path: PathBuf,
    },
    /// Generates static data but limits the number of lines emitted per block
    StaticLinesPerSecond {
        /// Defines the file path to read static variant data from. Content is
        /// assumed to be line-oriented but no other claim is made on the file.
        static_path: PathBuf,
        /// Number of lines to emit in each generated block
        lines_per_second: u32,
    },
    /// Generates static data grouped by second; each block contains one
    /// second's worth of logs as determined by a parsed timestamp prefix.
    StaticSecond {
        /// Defines the file path to read static variant data from. Content is
        /// assumed to be line-oriented.
        static_path: PathBuf,
        /// Chrono-compatible timestamp format string used to parse the leading
        /// timestamp in each line.
        timestamp_format: String,
        /// Emit a minimal placeholder block (single newline) for seconds with
        /// no lines. When false, empty seconds are skipped.
        #[serde(default)]
        emit_placeholder: bool,
    },
    /// Generates a line of printable ascii characters
    Ascii,
    /// Generates a json encoded line
    Json,
    /// Generates a Apache Common log lines
    ApacheCommon,
    /// Generates OpenTelemetry traces
    OpentelemetryTraces,
    /// Generates OpenTelemetry logs
    OpentelemetryLogs(crate::opentelemetry::log::Config),
    /// Generates OpenTelemetry metrics
    OpentelemetryMetrics(crate::opentelemetry::metric::Config),
    /// Generates `DogStatsD`
    #[serde(rename = "dogstatsd")]
    DogStatsD(crate::dogstatsd::Config),
    /// Generates `TraceAgent` payloads in `MsgPack` format
    #[serde(rename = "trace_agent")]
    TraceAgent(crate::trace_agent::Config),
}

/// Unified payload type for all serializers
#[derive(Debug)]
#[allow(dead_code, clippy::large_enum_variant)]
pub enum Payload {
    /// Apache Common Log format
    ApacheCommon(ApacheCommon),
    /// ASCII text
    Ascii(Ascii),
    /// Datadog Log format
    DatadogLog(DatadogLog),
    /// Fluent message format
    Fluent(Fluent),
    /// JSON format
    Json(Json),
    /// Splunk HEC format
    SplunkHec(splunk_hec::SplunkHec),
    /// Static file content
    Static(Static),
    /// Static file content with a fixed number of lines emitted per block
    StaticLinesPerSecond(StaticLinesPerSecond),
    /// Static file content grouped into one-second blocks based on timestamps
    StaticSecond(StaticSecond),
    /// Syslog RFC 5424 format
    Syslog(Syslog5424),
    /// OpenTelemetry traces
    OtelTraces(OpentelemetryTraces),
    /// OpenTelemetry logs
    OtelLogs(OpentelemetryLogs),
    /// OpenTelemetry metrics
    OtelMetrics(OpentelemetryMetrics),
    /// `DogStatsD` metrics
    DogStatsdD(DogStatsD),
    /// Datadog Trace Agent format
    TraceAgent(crate::trace_agent::v04::V04),
}

impl Serialize for Payload {
    fn to_bytes<W, R>(&mut self, rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        W: Write,
        R: Rng + Sized,
    {
        match self {
            Payload::ApacheCommon(ser) => ser.to_bytes(rng, max_bytes, writer),
            Payload::Ascii(ser) => ser.to_bytes(rng, max_bytes, writer),
            Payload::DatadogLog(ser) => ser.to_bytes(rng, max_bytes, writer),
            Payload::Fluent(ser) => ser.to_bytes(rng, max_bytes, writer),
            Payload::Json(ser) => ser.to_bytes(rng, max_bytes, writer),
            Payload::SplunkHec(ser) => ser.to_bytes(rng, max_bytes, writer),
            Payload::Static(ser) => ser.to_bytes(rng, max_bytes, writer),
            Payload::StaticLinesPerSecond(ser) => ser.to_bytes(rng, max_bytes, writer),
            Payload::StaticSecond(ser) => ser.to_bytes(rng, max_bytes, writer),
            Payload::Syslog(ser) => ser.to_bytes(rng, max_bytes, writer),
            Payload::OtelTraces(ser) => ser.to_bytes(rng, max_bytes, writer),
            Payload::OtelLogs(ser) => ser.to_bytes(rng, max_bytes, writer),
            Payload::OtelMetrics(ser) => ser.to_bytes(rng, max_bytes, writer),
            Payload::DogStatsdD(ser) => ser.to_bytes(rng, max_bytes, writer),
            Payload::TraceAgent(ser) => ser.to_bytes(rng, max_bytes, writer),
        }
    }

    fn data_points_generated(&self) -> Option<u64> {
        match self {
            Payload::OtelMetrics(ser) => ser.data_points_generated(),
            Payload::StaticLinesPerSecond(ser) => ser.data_points_generated(),
            Payload::StaticSecond(ser) => ser.data_points_generated(),
            // Other implementations use the default None
            _ => None,
        }
    }
}

/// Generate instances of `Self::Output` from source of randomness.
///
/// NOTE this generator is suitable for use only when the size of a
/// serialization is not perfectly predictable. If it is use `SizedGenerator`
/// instead.
pub(crate) trait Generator<'a> {
    type Output: 'a;
    type Error: 'a;

    fn generate<R>(&'a self, rng: &mut R) -> Result<Self::Output, Self::Error>
    where
        R: rand::Rng + ?Sized;
}

/// Generate instances of `Self::Output` from source of randomness, constrained
/// to byte budgets.
///
/// NOTE this generator is suitable for use only when the size of a
/// serialization is perfectly predictable. If it is not use `Generator`
/// instead.
pub(crate) trait SizedGenerator<'a> {
    type Output: 'a;
    type Error: 'a;

    /// Generate a new instance of `Self::Output`. Implementations MUST uphold
    /// the following properties:
    ///
    /// * `budget` is decremented if and only if return is Ok
    /// * `budget` must be decremented only by the amount required to store
    ///   returned instance of `Self::Output`.
    fn generate<R>(
        &'a mut self,
        rng: &mut R,
        budget: &mut usize,
    ) -> Result<Self::Output, Self::Error>
    where
        R: rand::Rng + ?Sized;
}
