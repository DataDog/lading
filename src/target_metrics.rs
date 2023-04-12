//! Fetch metrics from the target process
//!
//! This module allows lading to fetch metrics from the target process and
//! include them in the captures file.
//!

use serde::Deserialize;

use crate::signals::Shutdown;

pub mod expvar;

#[derive(Debug, Clone, Copy)]
/// Errors produced by [`Server`]
pub enum Error {
    /// See [`crate::target_metrics::expvar::Expvar`] for details.
    Expvar(expvar::Error),
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
/// Configuration for [`Server`]
pub enum Config {
    /// See [`crate::target_metrics::expvar::Config`] for details.
    Expvar(expvar::Config),
}

/// The target_metrics server.
#[derive(Debug)]
pub enum Server {
    /// See [`crate::target_metrics::expvar::Expvar`] for details.
    Expvar(expvar::Expvar),
}

impl Server {
    /// Create a new [`Server`] instance
    ///
    /// The target_metrics `Server` is responsible for scraping metrics from
    /// the target process.
    ///
    pub fn new(config: Config, shutdown: Shutdown) -> Self {
        match config {
            Config::Expvar(conf) => Self::Expvar(expvar::Expvar::new(conf, shutdown)),
        }
    }

    /// Run this [`Server`] to completion
    ///
    /// todo[gh]
    ///
    /// # Errors
    ///
    /// Function will return an error if the underlying metrics collector
    /// returns an error.
    ///
    /// # Panics
    ///
    /// None are known.
    pub async fn run(self) -> Result<(), Error> {
        match self {
            Server::Expvar(inner) => inner.run().await.map_err(Error::Expvar),
        }
    }
}
