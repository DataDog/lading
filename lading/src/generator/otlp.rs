//! The OTLP protocol speaking generator.

use super::General;
use hyper::Uri;
use serde::{Deserialize, Serialize};

/// Config for [`OTLP`]
#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
/// Configuration of this generator.
pub struct Config {
    /// The seed for random operations against this target
    pub seed: [u8; 32],
    /// The URI for the target, must be a valid URI
    #[serde(with = "http_serde::uri")]
    pub target_uri: Uri,
    /// The bytes per second to send or receive from the target
    pub bytes_per_second: byte_unit::Byte,
    /// The maximum size in bytes of the largest block in the prebuild cache.
    #[serde(default = "lading_payload::block::default_maximum_block_size")]
    pub maximum_block_size: byte_unit::Byte,
    /// The total number of parallel connections to maintain
    pub parallel_connections: u16,
    /// The load throttle configuration
    #[serde(default)]
    pub throttle: lading_throttle::Config,
}

#[derive(Copy, Clone, thiserror::Error, Debug)]
/// Errors produced by [`Otlp`].
pub enum Error {}

/// The OTLP generator.
///
/// This generator is reposnsible for connecting to the target via OTLP.
#[derive(Copy, Clone, Debug)]
pub struct Otlp {}

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
        _general: General,
        _config: Config,
        _shutdown: lading_signal::Watcher,
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
