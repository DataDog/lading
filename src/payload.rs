use std::io::{self, Write};

pub(crate) use ascii::Ascii;
pub(crate) use datadog_logs::DatadogLog;
pub(crate) use fluent::Fluent;
pub(crate) use foundationdb::FoundationDb;
pub(crate) use json::Json;
use rand::Rng;
pub(crate) use splunk_hec::{Encoding as SplunkHecEncoding, SplunkHec};
pub(crate) use statik::Static;
pub(crate) use syslog::Syslog5424;

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
