//! The file generator.
//!
//! Unlike the other generators the file generator does not "connect" however
//! losely to the target but instead, without coordination, merely writes files
//! on disk.
//!
//! ## Metrics
//!
//! `bytes_written`: Total bytes written
//! `bytes_per_second`: Configured rate to send data
//!
//! Additional metrics may be emitted by this generator's [throttle].
//!

pub mod logrotate;
pub mod traditional;

use std::str;

use serde::{Deserialize, Serialize};

use lading_signal::Phase;

use super::General;

#[derive(thiserror::Error, Debug)]
/// Errors produced by [`FileGen`].
pub enum Error {
    /// Wrapper around [`traditional::Error`].
    #[error(transparent)]
    Traditional(#[from] traditional::Error),
    /// Wrapper around [`logrotate::Error`].
    #[error(transparent)]
    Logrotate(#[from] logrotate::Error),
}

/// Configuration of [`FileGen`]
#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum Config {
    /// See [`traditional::Config`].
    Traditional(traditional::Config),
    /// See [`logrotate::Config`].
    Logrotate(logrotate::Config),
}

#[derive(Debug)]
/// The file generator.
///
/// This generator writes files to disk, rotating them as appropriate. It does
/// this without coordination to the target.
pub enum FileGen {
    /// See [`traditional::Server`] for details.
    Traditional(traditional::Server),
    /// See [`logrotate::Server`] for details.
    Logrotate(logrotate::Server),
}

impl FileGen {
    /// Create a new [`FileGen`]
    ///
    /// # Errors
    ///
    /// Creation will fail if the target file cannot be opened for writing.
    ///
    /// # Panics
    ///
    /// Function will panic if variant is Static and the `static_path` is not
    /// set.
    #[allow(clippy::cast_possible_truncation)]
    pub fn new(general: General, config: Config, shutdown: Phase) -> Result<Self, Error> {
        let srv = match config {
            Config::Traditional(c) => {
                Self::Traditional(traditional::Server::new(general, c, shutdown)?)
            }
            Config::Logrotate(c) => Self::Logrotate(logrotate::Server::new(general, c, shutdown)?),
        };
        Ok(srv)
    }

    /// Run [`FileGen`] to completion or until a shutdown signal is received.
    ///
    /// In this loop the target file will be populated with lines of the variant
    /// dictated by the end user.
    ///
    /// # Errors
    ///
    /// This function will terminate with an error if file permissions are not
    /// correct, if the file cannot be written to etc. Any error from
    /// `std::io::Error` is possible.
    #[allow(clippy::cast_precision_loss)]
    #[allow(clippy::cast_possible_truncation)]
    pub async fn spin(self) -> Result<(), Error> {
        match self {
            Self::Traditional(inner) => inner.spin().await?,
            Self::Logrotate(inner) => inner.spin().await?,
        };

        Ok(())
    }
}
