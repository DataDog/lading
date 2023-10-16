//! The UDP protocol speaking generator.
//!
//! ## Metrics
//!
//! `bytes_written`: Bytes written successfully
//! `packets_sent`: Packets written successfully
//! `request_failure`: Number of failed writes; each occurrence causes a socket re-bind
//! `connection_failure`: Number of socket bind failures
//! `bytes_per_second`: Configured rate to send data
//!
//! Additional metrics may be emitted by this generator's [throttle].
//!

use serde::Deserialize;
use tracing::info;

use crate::signals::Shutdown;

#[derive(Debug, Deserialize, PartialEq, Clone, Copy)]
/// Configuration of this generator.
pub struct Config {}

/// Errors produced by [`Idle`].
#[derive(thiserror::Error, Debug, Clone, Copy)]
pub enum Error {}

#[derive(Debug)]
/// The Idle generator.
///
/// This generator is responsible for doing nothing, except wait to shut down.
pub struct Idle {
    shutdown: Shutdown,
}

impl Idle {
    /// Create a new [`Idle`] instance
    ///
    /// # Errors
    ///
    /// Creation will not fail.
    ///
    /// # Panics
    ///
    /// Function will not panic.
    #[allow(clippy::cast_possible_truncation)]
    pub fn new(_config: &Config, shutdown: Shutdown) -> Result<Self, Error> {
        Ok(Self { shutdown })
    }

    /// Run [`Idle`] to completion or until a shutdown signal is received.
    ///
    /// # Errors
    ///
    /// Function will return an error when the UDP socket cannot be written to.
    ///
    /// # Panics
    ///
    /// Function will panic if underlying byte capacity is not available.
    pub async fn spin(mut self) -> Result<(), Error> {
        self.shutdown.recv().await;
        info!("shutdown signal received");
        Ok(())
    }
}
