use std::io::{self, Write};

mod ascii;
mod common;
mod datadog_logs;
mod foundationdb;
mod json;
mod statik;

pub use ascii::Ascii;
pub use datadog_logs::DatadogLog;
pub use foundationdb::FoundationDb;
pub use json::Json;
use rand::Rng;
pub use statik::Static;

/// Errors related to serialization
#[derive(Debug)]
pub enum Error {
    /// Json payload could not be encoded
    Json(serde_json::Error),
    /// IO operation failed
    Io(io::Error),
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Error::Json(error)
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Error::Io(error)
    }
}

pub trait Serialize {
    fn to_bytes<W, R>(&self, rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write;
}
