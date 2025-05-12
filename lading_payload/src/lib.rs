//! The lading payloads
//!
//! This library supports payload generation for the lading project.

#![deny(clippy::all)]
#![deny(clippy::cargo)]
#![deny(clippy::pedantic)]
#![deny(clippy::print_stdout)]
#![deny(clippy::print_stderr)]
#![deny(clippy::dbg_macro)]
#![deny(clippy::unwrap_used)]
#![deny(unused_extern_crates)]
#![deny(unused_allocation)]
#![deny(unused_assignments)]
#![deny(unused_comparisons)]
#![deny(unreachable_pub)]
#![deny(missing_docs)]
#![deny(missing_copy_implementations)]
#![deny(missing_debug_implementations)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::multiple_crate_versions)]

use std::{
    io::{self, Write},
    path::PathBuf,
};

use rand::{Rng, distr::weighted};
use serde::{Deserialize, Serialize as SerdeSerialize};

pub mod block;

pub use apache_common::ApacheCommon;
pub use ascii::Ascii;
pub use datadog_logs::DatadogLog;
pub use dogstatsd::DogStatsD;
pub use fluent::Fluent;
pub use json::Json;
pub use opentelemetry_log::OpentelemetryLogs;
pub use opentelemetry_metric::OpentelemetryMetrics;
pub use opentelemetry_trace::OpentelemetryTraces;
pub use splunk_hec::SplunkHec;
pub use statik::Static;
pub use syslog::Syslog5424;
pub use trace_agent::TraceAgent;

pub mod apache_common;
pub mod ascii;
pub(crate) mod common;
pub mod datadog_logs;
pub mod dogstatsd;
pub mod fluent;
pub mod json;
pub mod opentelemetry_log;
pub mod opentelemetry_metric;
pub mod opentelemetry_trace;
pub mod procfs;
pub mod splunk_hec;
pub mod statik;
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
}

/// Sub-configuration for `TraceAgent` format
#[derive(Debug, Deserialize, SerdeSerialize, Clone, Copy, PartialEq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Encoding {
    /// Use JSON format
    Json,
    /// Use `MsgPack` binary format
    #[serde(alias = "msgpack")]
    MsgPack,
}

/// Configuration for `Payload`
#[derive(Debug, Deserialize, SerdeSerialize, Clone, PartialEq)]
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
    /// Generates a line of printable ascii characters
    Ascii,
    /// Generates a json encoded line
    Json,
    /// Generates a Apache Common log lines
    ApacheCommon,
    /// Generates OpenTelemetry traces
    OpentelemetryTraces,
    /// Generates OpenTelemetry logs
    OpentelemetryLogs,
    /// Generates OpenTelemetry metrics
    OpentelemetryMetrics,
    /// Generates `DogStatsD`
    #[serde(rename = "dogstatsd")]
    DogStatsD(crate::dogstatsd::Config),
    /// Generates `TraceAgent` payloads in JSON format
    TraceAgent(Encoding),
}

#[derive(Debug)]
#[allow(dead_code, clippy::large_enum_variant)]
pub(crate) enum Payload {
    ApacheCommon(ApacheCommon),
    Ascii(Ascii),
    DatadogLog(DatadogLog),
    Fluent(Fluent),
    Json(Json),
    SplunkHec(splunk_hec::SplunkHec),
    Static(Static),
    Syslog(Syslog5424),
    OtelTraces(OpentelemetryTraces),
    OtelLogs(OpentelemetryLogs),
    OtelMetrics(OpentelemetryMetrics),
    DogStatsdD(DogStatsD),
    TraceAgent(TraceAgent),
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
            Payload::Syslog(ser) => ser.to_bytes(rng, max_bytes, writer),
            Payload::OtelTraces(ser) => ser.to_bytes(rng, max_bytes, writer),
            Payload::OtelLogs(ser) => ser.to_bytes(rng, max_bytes, writer),
            Payload::OtelMetrics(ser) => ser.to_bytes(rng, max_bytes, writer),
            Payload::DogStatsdD(ser) => ser.to_bytes(rng, max_bytes, writer),
            Payload::TraceAgent(ser) => ser.to_bytes(rng, max_bytes, writer),
        }
    }
}

/// Generate instance of `I` from source of randomness `S`.
pub(crate) trait Generator<'a> {
    type Output: 'a;
    type Error: 'a;

    fn generate<R>(&'a self, rng: &mut R) -> Result<Self::Output, Self::Error>
    where
        R: rand::Rng + ?Sized;
}
