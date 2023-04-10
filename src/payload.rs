use std::{
    io::{self, Write},
    path::PathBuf,
};

use rand::Rng;
use serde::Deserialize;

pub(crate) use apache_common::ApacheCommon;
pub(crate) use ascii::Ascii;
pub(crate) use datadog_logs::DatadogLog;
pub(crate) use dogstatsd::DogStatsD;
pub(crate) use fluent::Fluent;
pub(crate) use json::Json;
pub(crate) use opentelemetry_log::OpentelemetryLogs;
pub(crate) use opentelemetry_metric::OpentelemetryMetrics;
pub(crate) use opentelemetry_trace::OpentelemetryTraces;
pub(crate) use splunk_hec::{Encoding as SplunkHecEncoding, SplunkHec};
pub(crate) use statik::Static;
pub(crate) use syslog::Syslog5424;
pub(crate) use trace_agent::TraceAgent;

mod apache_common;
mod ascii;
mod common;
mod datadog_logs;
mod dogstatsd;
mod fluent;
mod json;
mod opentelemetry_log;
mod opentelemetry_metric;
mod opentelemetry_trace;
mod splunk_hec;
mod statik;
mod syslog;
mod trace_agent;

/// Errors related to serialization
#[derive(thiserror::Error, Debug)]
pub(crate) enum Error {
    /// MsgPack payload could not be encoded
    #[error("MsgPack payload could not be encoded: {0}")]
    MsgPack(#[from] rmp_serde::encode::Error),
    /// Json payload could not be encoded
    #[error("Json payload could not be encoded: {0}")]
    Json(#[from] serde_json::Error),
    /// IO operation failed
    #[error("IO operation failed: {0}")]
    Io(#[from] io::Error),
}

pub(crate) trait Serialize {
    /// Write bytes into writer, subject to `max_bytes` limitations.
    ///
    /// # Errors
    ///
    /// Most implementations are serializing data in some way. The errors that
    /// result come from serialization crackups.
    fn to_bytes<W, R>(&self, rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write;
}

/// Sub-configuration for `TraceAgent` format
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Encoding {
    /// Use JSON format
    Json,
    /// Use MsgPack binary format
    #[serde(alias = "msgpack")]
    MsgPack,
}

/// Configuration for `Payload`
#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Config {
    /// Generates Fluent messages
    Fluent,
    /// Generates syslog5424 messages
    Syslog5424,
    /// Generates Splunk HEC messages
    SplunkHec { encoding: SplunkHecEncoding },
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
    /// Generates DogStatsD
    #[serde(rename = "dogstatsd")]
    DogStatsD,
    /// Generates TraceAgent payloads in JSON format
    TraceAgent(Encoding),
}

#[derive(Debug)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[allow(dead_code)]
pub(crate) enum Payload {
    ApacheCommon(ApacheCommon),
    Ascii(Ascii),
    DatadogLog(DatadogLog),
    Fluent(Fluent),
    Json(Json),
    SplunkHec(SplunkHec),
    #[cfg_attr(test, proptest(skip))]
    // No way to generate the files necessary to back Static, so avoid
    // generating this variant entirely.
    Static(Static),
    Syslog(Syslog5424),
    OtelTraces(OpentelemetryTraces),
    OtelLogs(OpentelemetryLogs),
    OtelMetrics(OpentelemetryMetrics),
    DogStatsdD(DogStatsD),
    TraceAgent(TraceAgent),
}

impl Serialize for Payload {
    fn to_bytes<W, R>(&self, rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
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

/// Calculates the quotient of `lhs` and `rhs`, rounding the result towards
/// positive infinity.
///
/// Adapted from rustc `int_roundings` implementation. Replace upon
/// stabilization: <https://github.com/rust-lang/rust/issues/88581>
///
/// # Panics
///
/// This function will panic if `rhs` is 0 or the division results in overflow.
const fn div_ceil(lhs: usize, rhs: usize) -> usize {
    let d = lhs / rhs;
    let r = lhs % rhs;
    if r > 0 && rhs > 0 {
        d + 1
    } else {
        d
    }
}

/// Generate instance of `I` from source of randomness `S`.
pub(crate) trait Generator<I> {
    fn generate<R>(&self, rng: &mut R) -> I
    where
        R: rand::Rng + ?Sized;
}
