use std::{
    io::{self, Write},
    path::PathBuf,
};

pub(crate) use apache_common::ApacheCommon;
pub(crate) use ascii::Ascii;
pub(crate) use datadog_logs::DatadogLog;
pub(crate) use fluent::Fluent;
pub(crate) use foundationdb::FoundationDb;
pub(crate) use json::Json;
use rand::Rng;
use serde::Deserialize;
pub(crate) use splunk_hec::{Encoding as SplunkHecEncoding, SplunkHec};
pub(crate) use statik::Static;
pub(crate) use syslog::Syslog5424;

mod apache_common;
mod ascii;
mod common;
mod datadog_logs;
mod fluent;
mod foundationdb;
mod json;
mod splunk_hec;
mod statik;
mod syslog;

/// Errors related to serialization
#[derive(Debug)]
pub(crate) enum Error {
    /// MsgPack payload could not be encoded
    MsgPack(rmp_serde::encode::Error),
    /// Json payload could not be encoded
    Json(serde_json::Error),
    /// IO operation failed
    Io(io::Error),
    /// Arbitrary instance could not be created
    Arbitrary(arbitrary::Error),
}

impl From<rmp_serde::encode::Error> for Error {
    fn from(error: rmp_serde::encode::Error) -> Self {
        Error::MsgPack(error)
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Error::Json(error)
    }
}

impl From<arbitrary::Error> for Error {
    fn from(error: arbitrary::Error) -> Self {
        Error::Arbitrary(error)
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Error::Io(error)
    }
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

/// Configuration for `Payload`
#[derive(Debug, Deserialize, Clone)]
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
    /// Generates a limited subset of FoundationDB logs
    FoundationDb,
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
}

#[derive(Debug)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[allow(dead_code)]
pub(crate) enum Payload {
    ApacheCommon(ApacheCommon),
    Ascii(Ascii),
    DatadogLog(DatadogLog),
    Fluent(Fluent),
    FoundationDb(FoundationDb),
    Json(Json),
    SplunkHec(SplunkHec),
    #[cfg_attr(test, proptest(skip))]
    // No way to generate the files necessary to back Static, so avoid
    // generating this variant entirely.
    Static(Static),
    Syslog(Syslog5424),
}

impl Payload {}

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
            Payload::FoundationDb(ser) => ser.to_bytes(rng, max_bytes, writer),
            Payload::Json(ser) => ser.to_bytes(rng, max_bytes, writer),
            Payload::SplunkHec(ser) => ser.to_bytes(rng, max_bytes, writer),
            Payload::Static(ser) => ser.to_bytes(rng, max_bytes, writer),
            Payload::Syslog(ser) => ser.to_bytes(rng, max_bytes, writer),
        }
    }
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;
    use rand::{rngs::SmallRng, SeedableRng};

    use crate::payload::{Payload, Serialize};

    // We want to be sure that the serialized size of the payload does not
    // exceed `max_bytes`.
    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes: u16, payload: Payload) {
            let max_bytes = max_bytes as usize;
            let rng = SmallRng::seed_from_u64(seed);

            let mut bytes = Vec::with_capacity(max_bytes);
            payload.to_bytes(rng, max_bytes, &mut bytes).unwrap();
            debug_assert!(
                bytes.len() <= max_bytes,
                "{:?}",
                std::str::from_utf8(&bytes).unwrap()
            );
        }
    }
}
