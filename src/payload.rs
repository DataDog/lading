use std::io::{self, Write};

mod ascii;
mod json;
mod statik;

pub use ascii::Ascii;
pub use json::Json;
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
    fn to_bytes<W>(&self, writer: &mut W) -> Result<(), Error>
    where
        W: Write;
}
