//! The OTLP protocol speaking generator.

use serde::{Deserialize, Serialize};
use super::General;

#[derive(Copy, Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
/// Configuration of this generator.
pub struct Config {
}

#[derive(Copy, Clone, thiserror::Error, Debug)]
/// Errors produced by [`Otlp`].
pub enum Error {
}

/// The OTLP generator.
///
/// This generator is reposnsible for connecting to the target via OTLP.
#[derive(Copy, Clone, Debug)]
pub struct Otlp {
}

impl Otlp {
    /// Create a new [`Otlp`] instance
    ///
    /// # Errors
    ///
    /// Creation will fail if the underlying governor capacity exceeds u32.
    ///
    /// # Panics
    ///
    /// Function will panic if user has passed non-zero values for any byte
    /// values. Sharp corners.
    #[allow(clippy::cast_possible_truncation)]
    pub fn new(
        general: General,
        config: Config,
        shutdown: lading_signal::Watcher,
    ) -> Result<Self, Error> {
        Ok(Self {})
    }

    /// Run [`Otlp`] to completion or until a shutdown signal is received.
    ///
    /// # Errors
    ///
    /// TODO
    ///
    pub async fn spin(self) -> Result<(), Error> {
        return Ok(());
    }
}
